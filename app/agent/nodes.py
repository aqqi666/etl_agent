from __future__ import annotations

import json

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
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
    llm = _get_llm().with_structured_output(ETLPlan)
    artifacts = state.get("artifacts", ETLArtifacts())

    prompt = PLANNER_PROMPT.format(artifacts_json=_artifacts_json(artifacts))
    messages = [SystemMessage(content=prompt)] + state["messages"]

    plan: ETLPlan = await llm.ainvoke(messages)

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


# ── executor ─────────────────────────────────────────────────────────


async def executor(state: ETLState) -> dict:
    """执行当前步骤，调用 tools"""
    llm = _get_llm()
    llm_with_tools = llm.bind_tools(ALL_TOOLS)

    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)
    artifacts = state.get("artifacts", ETLArtifacts())

    if current_step < len(plan):
        step = plan[current_step]
        step_desc = f"步骤 {step.index}: {step.title}\n{step.description}"
        # 标记步骤为进行中
        step.status = "in_progress"
    else:
        step_desc = "所有步骤已完成"

    prompt = EXECUTOR_PROMPT.format(current_step_description=step_desc)
    messages = [SystemMessage(content=prompt)] + state["messages"]

    response: AIMessage = await llm_with_tools.ainvoke(messages)
    return {"messages": [response]}


# ── observer ─────────────────────────────────────────────────────────


async def observer(state: ETLState) -> dict:
    """分析 tool 执行结果，生成结构化输出，更新 artifacts"""
    llm = _get_llm().with_structured_output(StepObservation)
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

    # 更新 artifacts
    if observation.artifacts_update:
        updated = artifacts.model_copy(update=observation.artifacts_update)
    else:
        updated = artifacts

    # 标记当前步骤完成
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)
    if current_step < len(plan):
        plan[current_step].status = "completed"

    # 通过 stream writer 推送结构化结果
    writer = get_stream_writer()
    observation_md = _render_observation(observation)
    writer({"type": "observation", "content": observation_md})

    return {
        "artifacts": updated,
        "past_steps": [(
            plan[current_step].title if current_step < len(plan) else "unknown",
            observation.summary,
        )],
    }


def _render_observation(obs: StepObservation) -> str:
    parts = [f"### {obs.summary}\n"]
    if obs.sql_executed:
        parts.append(f"**执行的 SQL:**\n```sql\n{obs.sql_executed}\n```\n")
    if obs.result_display:
        parts.append(f"**结果:**\n{obs.result_display}\n")
    if obs.sql_status:
        parts.append(f"**状态:** {obs.sql_status}\n")
    if obs.analysis:
        parts.append(f"**分析:** {obs.analysis}\n")
    if obs.sql_explanation:
        parts.append(f"**SQL 解释:**\n{obs.sql_explanation}\n")
    if obs.next_step_hint:
        parts.append(f"**下一步:** {obs.next_step_hint}\n")
    if obs.missing_info:
        parts.append("**需要补充的信息:**\n" + "\n".join(f"- {i}" for i in obs.missing_info) + "\n")
    return "\n".join(parts)


# ── replanner ────────────────────────────────────────────────────────


async def replanner(state: ETLState) -> dict:
    """决定下一步：继续 / 调整计划 / 结束 / 提问"""
    llm = _get_llm().with_structured_output(ReplanDecision)
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

    writer = get_stream_writer()

    if decision.action == "respond":
        writer({"type": "text_delta", "content": decision.response or "ETL 流程已完成。"})
        return {"response": decision.response or "ETL 流程已完成。"}

    if decision.action == "ask_user":
        question = decision.question or "请提供更多信息。"
        writer({"type": "text_delta", "content": question})
        return {"response": question}

    if decision.action == "replan" and decision.updated_plan:
        new_steps = [ETLStep(**s) for s in decision.updated_plan] if isinstance(decision.updated_plan[0], dict) else decision.updated_plan
        return {
            "plan": new_steps,
            "current_step": 0,
            "response": None,
        }

    # continue: 前进到下一步
    return {
        "current_step": state.get("current_step", 0) + 1,
        "response": None,
    }
