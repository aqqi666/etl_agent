from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode, tools_condition

from app.agent.nodes import executor, observer, planner, replanner
from app.agent.state import ETLArtifacts, ETLState
from app.tools import ALL_TOOLS


def route_after_replan(state: ETLState) -> str:
    """replanner 之后的路由：有 response 则结束，否则继续执行"""
    if state.get("response"):
        return "end"
    return "executor"


def build_graph():
    tool_node = ToolNode(ALL_TOOLS)

    graph = StateGraph(ETLState)

    # 添加节点
    graph.add_node("planner", planner)
    graph.add_node("executor", executor)
    graph.add_node("tools", tool_node)
    graph.add_node("observer", observer)
    graph.add_node("replanner", replanner)

    # 边：START → planner → executor
    graph.add_edge(START, "planner")
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

    return graph.compile()


# 编译好的 graph 实例
etl_graph = build_graph()
