from __future__ import annotations

import asyncio
import json
import logging
import time

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.config import get_stream_writer

from app.agent.schemas import StepResult
from app.agent.state import ETLArtifacts, ETLState, ETLStep
from app.agent.system_prompt import ANALYZER_PROMPT, EXECUTOR_PROMPT
from app.config import settings
from app.tools import ALL_TOOLS

logger = logging.getLogger(__name__)


def _get_llm(role: str = "") -> ChatOpenAI:
    """根据角色名获取对应模型，未单独配置时回退到默认模型。"""
    model = getattr(settings, f"{role}_model", "") if role else ""
    return ChatOpenAI(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        model=model or settings.llm_model,
        temperature=0,
        extra_body={"enable_thinking": False},
    )


def _artifacts_json(artifacts: ETLArtifacts) -> str:
    return artifacts.model_dump_json(indent=2, exclude_none=True)


# ── parallel tool node ───────────────────────────────────────────────

# 写操作工具名集合，这些工具保持串行执行
WRITE_TOOLS = {"execute_sql"}


async def parallel_tool_node(state: ETLState) -> dict:
    """并行执行只读工具，串行执行写操作工具"""
    last_msg = state["messages"][-1]
    tool_calls = getattr(last_msg, "tool_calls", [])
    if not tool_calls:
        return {"messages": []}

    tool_map = {t.name: t for t in ALL_TOOLS}

    async def _run_one(call: dict) -> ToolMessage:
        tool = tool_map[call["name"]]
        try:
            result = await asyncio.to_thread(tool.invoke, call["args"])
        except Exception as e:
            logger.error("[parallel_tool_node] 工具 %s 执行失败: %s", call["name"], e)
            result = f"工具执行失败: {e}"
        return ToolMessage(content=str(result), tool_call_id=call["id"], name=call["name"])

    start = time.time()

    # 按原始顺序记录索引，分组执行后再合并
    results: list[ToolMessage | None] = [None] * len(tool_calls)

    read_indices = []
    write_indices = []
    for i, call in enumerate(tool_calls):
        if call["name"] in WRITE_TOOLS:
            write_indices.append(i)
        else:
            read_indices.append(i)

    # 只读工具并行执行
    if read_indices:
        read_tasks = [_run_one(tool_calls[i]) for i in read_indices]
        read_results = await asyncio.gather(*read_tasks)
        for idx, res in zip(read_indices, read_results):
            results[idx] = res

    # 写操作串行执行
    for i in write_indices:
        results[i] = await _run_one(tool_calls[i])

    elapsed = time.time() - start
    logger.info(
        "[parallel_tool_node] 执行 %d 个工具（%d 并行, %d 串行）耗时 %.2fs",
        len(tool_calls), len(read_indices), len(write_indices), elapsed,
    )

    return {"messages": list(results)}


# ── executor ─────────────────────────────────────────────────────────


async def executor(state: ETLState) -> dict:
    """执行当前步骤，调用 tools"""
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)
    artifacts = state.get("artifacts", ETLArtifacts())

    if current_step < len(plan):
        step = plan[current_step]
        step_desc = f"步骤 {step.index}: {step.title}\n{step.description}"
        step.status = "in_progress"
        logger.info("[executor] 执行步骤 %d/%d: %s", step.index, len(plan), step.title)
    else:
        step_desc = "所有步骤已完成"
        logger.info("[executor] 所有步骤已完成")

    llm = _get_llm("executor")
    llm_with_tools = llm.bind_tools(ALL_TOOLS)

    prompt = EXECUTOR_PROMPT.format(
        current_step_description=step_desc,
        artifacts_json=_artifacts_json(artifacts),
    )
    messages = [SystemMessage(content=prompt)] + state["messages"]

    response: AIMessage = await llm_with_tools.ainvoke(messages)

    if getattr(response, "tool_calls", None):
        for tc in response.tool_calls:
            logger.info("[executor] 调用工具: %s(%s)", tc["name"], json.dumps(tc["args"], ensure_ascii=False)[:200])
    else:
        logger.info("[executor] LLM 无工具调用，直接输出文本")

    return {"messages": [response]}


# ── analyzer（合并 observer + replanner）────────────────────────────


async def analyzer(state: ETLState) -> dict:
    """分析工具执行结果 + 决策下一步行动（合并原 observer 和 replanner）"""
    logger.info("[analyzer] 开始分析结果并决策")
    llm = _get_llm("observer").with_structured_output(StepResult, method="function_calling")
    artifacts = state.get("artifacts", ETLArtifacts())
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)
    past_steps = state.get("past_steps", [])

    # 收集最近的 tool 结果
    tool_results = []
    for msg in reversed(state["messages"]):
        if msg.type == "tool":
            tool_results.insert(0, f"[{msg.name}]: {msg.content}")
        elif msg.type == "ai" and not getattr(msg, "tool_calls", None):
            break

    prompt = ANALYZER_PROMPT.format(
        plan_json=json.dumps([s.model_dump() for s in plan], ensure_ascii=False, indent=2),
        past_steps_json=json.dumps(past_steps, ensure_ascii=False, indent=2),
        artifacts_json=_artifacts_json(artifacts),
        tool_results="\n\n".join(tool_results) if tool_results else "无工具调用结果",
    )
    messages = [SystemMessage(content=prompt)] + state["messages"]

    result: StepResult = await llm.ainvoke(messages)

    logger.info("[analyzer] 摘要: %s | action=%s", result.summary, result.action)

    # 更新 artifacts
    if result.artifacts_update:
        updated_artifacts = artifacts.model_copy(update=result.artifacts_update)
    else:
        updated_artifacts = artifacts

    # 记录已完成步骤
    step_title = plan[current_step].title if current_step < len(plan) else "unknown"

    writer = get_stream_writer()
    display = result.display_text or ""

    # ── 根据 action 处理 ──

    if result.action == "respond":
        logger.info("[analyzer] 流程结束，生成最终响应")
        final_text = result.response or "ETL 流程已完成。"
        content = f"{display}\n\n{final_text}".strip() if display else final_text
        writer({"type": "response", "content": content})
        return {
            "artifacts": updated_artifacts,
            "past_steps": [(step_title, result.summary)],
            "response": final_text,
        }

    if result.action == "ask_user":
        question = result.question or "请提供更多信息。"
        logger.info("[analyzer] 向用户提问: %s (step_complete=%s)", question[:100], result.step_complete)
        content = f"{display}\n\n{question}".strip() if display else question
        writer({"type": "response", "content": content})
        state_update: dict = {
            "artifacts": updated_artifacts,
            "past_steps": [(step_title, result.summary)],
            "response": question,
        }
        if result.step_complete:
            if current_step < len(plan):
                plan[current_step].status = "completed"
            state_update["current_step"] = current_step + 1
            logger.info("[analyzer] 步骤已完成，推进到步骤 %d", current_step + 1)
        else:
            logger.info("[analyzer] 步骤未完成，停留在当前步骤")
        return state_update

    if result.action == "replan" and result.updated_plan:
        new_steps = [ETLStep(**s) for s in result.updated_plan] if isinstance(result.updated_plan[0], dict) else result.updated_plan
        logger.info("[analyzer] 重新规划，新计划包含 %d 个步骤", len(new_steps))
        if display:
            writer({"type": "response", "content": display})
        return {
            "artifacts": updated_artifacts,
            "past_steps": [(step_title, result.summary)],
            "plan": new_steps,
            "current_step": 0,
            "response": None,
        }

    # fallback：当作 ask_user + step_complete=true 处理
    logger.warning("[analyzer] 未预期的 action=%s，fallback 为 ask_user", result.action)
    question = result.question or "请告诉我下一步需要做什么。"
    content = f"{display}\n\n{question}".strip() if display else question
    writer({"type": "response", "content": content})
    if current_step < len(plan):
        plan[current_step].status = "completed"
    return {
        "artifacts": updated_artifacts,
        "past_steps": [(step_title, result.summary)],
        "response": question,
        "current_step": current_step + 1,
    }
