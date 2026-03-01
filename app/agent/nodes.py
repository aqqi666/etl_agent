from __future__ import annotations

import asyncio
import json
import logging
import time

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.config import get_stream_writer

from app.agent.schemas import ETLPlan, ReplanDecision, StepObservation
from app.agent.state import ETLArtifacts, ETLState, ETLStep
from app.agent.system_prompt import (
    EXECUTOR_PROMPT,
    OBSERVER_PROMPT,
    PLANNER_PROMPT,
    REPLANNER_PROMPT,
)
from app.config import settings
from app.tools import ALL_TOOLS

logger = logging.getLogger(__name__)


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
        temperature=0,
    )


def _artifacts_json(artifacts: ETLArtifacts) -> str:
    return artifacts.model_dump_json(indent=2, exclude_none=True)


# ── planner ──────────────────────────────────────────────────────────


async def planner(state: ETLState) -> dict:
    """生成或更新 ETL 计划"""
    logger.info("[planner] 开始生成 ETL 计划")
    llm = _get_llm().with_structured_output(ETLPlan, method="function_calling")
    artifacts = state.get("artifacts", ETLArtifacts())
    past_steps = state.get("past_steps", [])

    prompt = PLANNER_PROMPT.format(
        artifacts_json=_artifacts_json(artifacts),
        past_steps_json=json.dumps(past_steps, ensure_ascii=False, indent=2) if past_steps else "无",
    )
    messages = [SystemMessage(content=prompt)] + state["messages"]

    plan: ETLPlan = await llm.ainvoke(messages)

    logger.info("[planner] 生成计划包含 %d 个步骤", len(plan.steps))
    for step in plan.steps:
        logger.info("[planner]   步骤 %d: %s", step.index, step.title)

    writer = get_stream_writer()
    # 推送计划给前端
    plan_text = "## ETL 计划\n\n"
    for step in plan.steps:
        plan_text += f"{step.index}. **{step.title}** — {step.description}\n"
    writer({"type": "plan", "content": plan_text})

    return {
        "plan": plan.steps,
        "current_step": 0,
        "response": None,
    }


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

    llm = _get_llm()
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


# ── observer ─────────────────────────────────────────────────────────


async def observer(state: ETLState) -> dict:
    """分析 tool 执行结果，生成结构化输出，更新 artifacts"""
    logger.info("[observer] 开始分析工具执行结果")
    llm = _get_llm().with_structured_output(StepObservation, method="function_calling")
    artifacts = state.get("artifacts", ETLArtifacts())

    # 收集最近的 tool 结果
    tool_results = []
    for msg in reversed(state["messages"]):
        if msg.type == "tool":
            tool_results.insert(0, f"[{msg.name}]: {msg.content}")
        elif msg.type == "ai" and not getattr(msg, "tool_calls", None):
            break

    prompt = OBSERVER_PROMPT.format(
        artifacts_json=_artifacts_json(artifacts),
        tool_results="\n\n".join(tool_results) if tool_results else "无工具调用结果",
    )
    messages = [SystemMessage(content=prompt)] + state["messages"]

    observation: StepObservation = await llm.ainvoke(messages)

    logger.info("[observer] 观察摘要: %s", observation.summary)
    if observation.sql_status:
        logger.info("[observer] SQL 状态: %s", observation.sql_status)

    # 更新 artifacts
    if observation.artifacts_update:
        updated = artifacts.model_copy(update=observation.artifacts_update)
    else:
        updated = artifacts

    # 不在 observer 中标记步骤完成，由 replanner 的 continue 决定
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)

    return {
        "artifacts": updated,
        "observation_text": observation.display_text,
        "past_steps": [(
            plan[current_step].title if current_step < len(plan) else "unknown",
            observation.summary,
        )],
    }


# ── replanner ────────────────────────────────────────────────────────


async def replanner(state: ETLState) -> dict:
    """决定下一步：继续 / 调整计划 / 结束 / 提问"""
    logger.info("[replanner] 开始决策下一步行动")
    llm = _get_llm().with_structured_output(ReplanDecision, method="function_calling")
    artifacts = state.get("artifacts", ETLArtifacts())
    plan = state.get("plan", [])
    past_steps = state.get("past_steps", [])

    prompt = REPLANNER_PROMPT.format(
        plan_json=json.dumps([s.model_dump() for s in plan], ensure_ascii=False, indent=2),
        past_steps_json=json.dumps(past_steps, ensure_ascii=False, indent=2),
        artifacts_json=_artifacts_json(artifacts),
    )
    messages = [SystemMessage(content=prompt)] + state["messages"]

    decision: ReplanDecision = await llm.ainvoke(messages)

    logger.info("[replanner] 决策结果: action=%s", decision.action)

    writer = get_stream_writer()
    obs_text = state.get("observation_text") or ""

    if decision.action == "respond":
        logger.info("[replanner] 流程结束，生成最终响应")
        final_text = decision.response or "ETL 流程已完成。"
        # 合并 observation + 总结
        content = f"{obs_text}\n\n{final_text}".strip() if obs_text else final_text
        writer({"type": "response", "content": content})
        return {"response": final_text, "observation_text": None}

    if decision.action == "ask_user":
        question = decision.question or "请提供更多信息。"
        logger.info("[replanner] 向用户提问: %s (step_complete=%s)", question[:100], decision.step_complete)
        # 合并 observation + 引导问题为一条消息
        content = f"{obs_text}\n\n{question}".strip() if obs_text else question
        writer({"type": "response", "content": content})
        result: dict = {"response": question, "observation_text": None}
        if decision.step_complete:
            # 步骤已完成（如连接成功后问选哪个表）→ 标记完成并推进
            plan = state.get("plan", [])
            current_step = state.get("current_step", 0)
            if current_step < len(plan):
                plan[current_step].status = "completed"
            result["current_step"] = current_step + 1
            logger.info("[replanner] 步骤已完成，推进到步骤 %d", current_step + 1)
        else:
            # 步骤未完成（如等待用户确认 SQL）→ 停留在当前步骤
            logger.info("[replanner] 步骤未完成，停留在当前步骤")
        return result

    if decision.action == "replan" and decision.updated_plan:
        new_steps = [ETLStep(**s) for s in decision.updated_plan] if isinstance(decision.updated_plan[0], dict) else decision.updated_plan
        logger.info("[replanner] 重新规划，新计划包含 %d 个步骤", len(new_steps))
        if obs_text:
            writer({"type": "response", "content": obs_text})
        return {
            "plan": new_steps,
            "current_step": 0,
            "response": None,
            "observation_text": None,
        }

    # fallback（含 continue）：当作 ask_user + step_complete=true 处理
    # 对话式流程不应跳过用户，始终回到用户
    logger.warning("[replanner] 未预期的 action=%s，fallback 为 ask_user", decision.action)
    question = decision.question or "请告诉我下一步需要做什么。"
    content = f"{obs_text}\n\n{question}".strip() if obs_text else question
    writer({"type": "response", "content": content})
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)
    if current_step < len(plan):
        plan[current_step].status = "completed"
    return {
        "response": question,
        "current_step": current_step + 1,
        "observation_text": None,
    }
