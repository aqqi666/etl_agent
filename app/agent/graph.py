import logging

from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from app.agent.nodes import executor, observer, planner, replanner
from app.agent.state import ETLState
from app.tools import ALL_TOOLS

logger = logging.getLogger(__name__)


def route_entry(state: ETLState) -> str:
    """入口路由：有计划且在等待用户回复时走 executor，否则走 planner"""
    plan = state.get("plan", [])
    if plan and state.get("current_step", 0) < len(plan):
        logger.debug("[graph] 入口路由 -> executor（已有计划，继续执行）")
        return "executor"
    logger.debug("[graph] 入口路由 -> planner（需要生成计划）")
    return "planner"


def route_after_replan(state: ETLState) -> str:
    """replanner 之后的路由：有 response 则结束，否则继续执行"""
    target = "end" if state.get("response") else "executor"
    logger.debug("[graph] replanner 路由 -> %s", target)
    return target


def build_graph():
    logger.info("[graph] 开始构建 ETL Agent 流程图")
    tool_node = ToolNode(ALL_TOOLS)

    graph = StateGraph(ETLState)

    # 添加节点
    graph.add_node("planner", planner)
    graph.add_node("executor", executor)
    graph.add_node("tools", tool_node)
    graph.add_node("observer", observer)
    graph.add_node("replanner", replanner)

    # 入口路由：区分首次规划和用户回复后继续
    graph.add_conditional_edges(
        START, route_entry, {"planner": "planner", "executor": "executor"}
    )
    graph.add_edge("planner", "executor")

    # executor → tools_condition（有 tool_call 则走 tools，否则走 observer）
    graph.add_conditional_edges(
        "executor",
        tools_condition,
        {"tools": "tools", "__end__": "observer"},
    )

    # tools → executor（ReAct 循环）
    graph.add_edge("tools", "executor")

    # observer → replanner
    graph.add_edge("observer", "replanner")

    # replanner → executor 或 END
    graph.add_conditional_edges(
        "replanner",
        route_after_replan,
        {"executor": "executor", "end": END},
    )

    serde = JsonPlusSerializer(
        allowed_msgpack_modules=[
            ("app.agent.state", "ETLStep"),
            ("app.agent.state", "ETLArtifacts"),
        ]
    )
    memory = MemorySaver(serde=serde)
    compiled = graph.compile(checkpointer=memory)
    logger.info("[graph] ETL Agent 流程图构建完成（含 checkpointer）")
    return compiled


# 编译好的 graph 实例
etl_graph = build_graph()
