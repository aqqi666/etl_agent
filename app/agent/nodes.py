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


# ── render 格式化 ────────────────────────────────────────────────────


def _format_payload_to_markdown(payload: dict) -> str:
    """将单个结构化 payload 格式化为 Markdown 片段。"""
    sections: list[str] = []
    title = payload.get("title", "")
    if title:
        sections.append(f"**{title}**\n")

    # SQL 代码块
    sql = payload.get("sql")
    if sql:
        sections.append(f"```sql\n{sql}\n```\n")

    # 表格
    columns = payload.get("columns")
    rows = payload.get("rows")
    if columns and rows:
        header = "| " + " | ".join(str(c) for c in columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        data_lines = []
        for row in rows:
            data_lines.append("| " + " | ".join(str(row.get(c, "")) for c in columns) + " |")
        sections.append(header + "\n" + separator + "\n" + "\n".join(data_lines) + "\n")
        total = payload.get("total_rows")
        if total and total > len(rows):
            sections.append(f"*... 共 {total} 行，仅显示前 {len(rows)} 行*\n")

    # 纯文本
    text = payload.get("text")
    if text:
        sections.append(text + "\n")

    # quality_report 的数值极值附加表格
    metadata = payload.get("metadata", {})
    if payload.get("result_type") == "quality_report":
        total_rows = metadata.get("total_rows")
        if total_rows is not None:
            sections.insert(0, f"总行数: {total_rows}\n")
        numeric_stats = metadata.get("numeric_stats")
        if numeric_stats:
            ns_columns = numeric_stats["columns"]
            ns_rows = numeric_stats["rows"]
            sections.append("\n**数值字段极值**\n")
            header = "| " + " | ".join(str(c) for c in ns_columns) + " |"
            separator = "| " + " | ".join("---" for _ in ns_columns) + " |"
            data_lines = []
            for row in ns_rows:
                data_lines.append("| " + " | ".join(str(row.get(c, "")) for c in ns_columns) + " |")
            sections.append(header + "\n" + separator + "\n" + "\n".join(data_lines) + "\n")

    # lineage 血缘图谱（Mermaid）
    if payload.get("result_type") == "lineage":
        mermaid_code = metadata.get("mermaid", "")
        if mermaid_code:
            sections.append(f"```mermaid\n{mermaid_code}\n```\n")

    return "\n".join(sections)


def _handle_render(call: dict, render_cache: dict, rendered_parts: list[str]) -> ToolMessage:
    """处理 render 工具调用：从缓存格式化 Markdown，存入 rendered_parts 供 analyzer 合并。"""
    text = call["args"].get("text", "")
    raw_ids = call["args"].get("tool_call_ids")

    # 兼容模型传入 JSON 字符串（如 '["describe_table","preview_data"]'）
    if isinstance(raw_ids, str):
        try:
            raw_ids = json.loads(raw_ids)
        except (json.JSONDecodeError, TypeError):
            raw_ids = None

    # 选择要渲染的工具结果
    if isinstance(raw_ids, list):
        # 模型显式指定了要展示的工具（空列表 = 不展示任何缓存结果，只展示 text）
        name_index: dict[str, list[str]] = {}
        for tid, payload in render_cache.items():
            tname = payload.get("tool_name", "")
            name_index.setdefault(tname, []).append(tid)

        selected_ids: list[str] = []
        for ref in raw_ids:
            if ref in render_cache:
                selected_ids.append(ref)          # 精确匹配 tool_call_id
            elif ref in name_index:
                selected_ids.extend(name_index[ref])  # 按 tool_name 匹配

        selected = {tid: render_cache[tid] for tid in selected_ids}
    else:
        # 未传 tool_call_ids（None）则展示所有缓存结果
        selected = dict(render_cache)

    for _tool_call_id, payload in selected.items():
        md = _format_payload_to_markdown(payload)
        if md.strip():
            rendered_parts.append(md)

    # 追加模型传入的附加文字
    if text:
        rendered_parts.append(text)

    logger.info("[render] 已格式化 %d/%d 个工具结果，等待 analyzer 合并发送",
                len(selected), len(render_cache))

    # 清除已渲染的缓存项
    for tid in selected:
        render_cache.pop(tid, None)

    return ToolMessage(content="已渲染展示给用户", tool_call_id=call["id"], name="render")


# ── parallel tool node ───────────────────────────────────────────────

# 写操作 / 后处理工具名集合，这些工具在只读工具之后串行执行
WRITE_TOOLS = {"execute_sql", "render"}


async def parallel_tool_node(state: ETLState) -> dict:
    """并行执行只读工具，串行执行写操作工具。
    结构化结果缓存供 render 工具使用，render 格式化后存入 rendered_content 供 analyzer 合并。"""
    last_msg = state["messages"][-1]
    tool_calls = getattr(last_msg, "tool_calls", [])
    if not tool_calls:
        return {"messages": []}

    tool_map = {t.name: t for t in ALL_TOOLS}

    # 加载上轮缓存（跨 ReAct 周期）+ 本轮新增
    render_cache: dict[str, dict] = dict(state.get("render_cache") or {})
    # 收集 render 工具格式化的内容片段
    rendered_parts: list[str] = []

    async def _run_one(call: dict) -> ToolMessage:
        name = call["name"]

        # render 工具拦截：不调用实际函数，由 _handle_render 处理
        if name == "render":
            return _handle_render(call, render_cache, rendered_parts)

        tool = tool_map[name]
        try:
            result = await asyncio.to_thread(tool.invoke, call["args"])
        except Exception as e:
            logger.error("[parallel_tool_node] 工具 %s 执行失败: %s", name, e)
            result = f"工具执行失败: {e}"

        raw = str(result)
        llm_content = raw  # 默认原样传给 LLM

        # 尝试解析结构化结果
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and parsed.get("__structured__"):
                # 缓存 payload 供 render 使用
                render_cache[call["id"]] = parsed["payload"]
                # LLM 只拿到摘要
                llm_content = parsed.get("summary", "工具执行完成")
        except (json.JSONDecodeError, KeyError, TypeError):
            pass  # 非结构化结果，原样传递

        return ToolMessage(content=llm_content, tool_call_id=call["id"], name=name)

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

    # 写操作 / render 串行执行（确保在只读工具之后）
    for i in write_indices:
        results[i] = await _run_one(tool_calls[i])

    elapsed = time.time() - start
    logger.info(
        "[parallel_tool_node] 执行 %d 个工具（%d 并行, %d 串行）耗时 %.2fs",
        len(tool_calls), len(read_indices), len(write_indices), elapsed,
    )

    # rendered_content: render 工具格式化的 Markdown，由 analyzer 合并到最终 response
    rendered_content = "\n".join(rendered_parts).strip() if rendered_parts else None

    return {
        "messages": list(results),
        "render_cache": render_cache,
        "rendered_content": rendered_content,
    }


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

    # render 工具格式化的内容（由 parallel_tool_node 存入 state）
    rendered = state.get("rendered_content") or ""

    def _build_content(*parts: str) -> str:
        """将 rendered_content、display_text、question/response 等拼接为一条完整内容。"""
        return "\n\n".join(p for p in parts if p.strip()).strip()

    # ── 根据 action 处理 ──

    if result.action == "respond":
        logger.info("[analyzer] 流程结束，生成最终响应")
        final_text = result.response or "ETL 流程已完成。"
        content = _build_content(rendered, display, final_text)
        writer({"type": "response", "content": content})
        return {
            "artifacts": updated_artifacts,
            "past_steps": [(step_title, result.summary)],
            "response": final_text,
            "rendered_content": None,
        }

    if result.action == "ask_user":
        question = result.question or "请提供更多信息。"
        logger.info("[analyzer] 向用户提问: %s (step_complete=%s)", question[:100], result.step_complete)
        content = _build_content(rendered, display, question)
        writer({"type": "response", "content": content})
        state_update: dict = {
            "artifacts": updated_artifacts,
            "past_steps": [(step_title, result.summary)],
            "response": question,
            "rendered_content": None,
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
        content = _build_content(rendered, display)
        if content:
            writer({"type": "response", "content": content})
        return {
            "artifacts": updated_artifacts,
            "past_steps": [(step_title, result.summary)],
            "plan": new_steps,
            "current_step": 0,
            "response": None,
            "rendered_content": None,
        }

    # fallback：当作 ask_user + step_complete=true 处理
    logger.warning("[analyzer] 未预期的 action=%s，fallback 为 ask_user", result.action)
    question = result.question or "请告诉我下一步需要做什么。"
    content = _build_content(rendered, display, question)
    writer({"type": "response", "content": content})
    if current_step < len(plan):
        plan[current_step].status = "completed"
    return {
        "artifacts": updated_artifacts,
        "past_steps": [(step_title, result.summary)],
        "response": question,
        "current_step": current_step + 1,
        "rendered_content": None,
    }
