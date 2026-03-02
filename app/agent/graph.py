import logging

from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import tools_condition

from app.agent.nodes import analyzer, executor, parallel_tool_node
from app.agent.state import ETLState, ETLStep

logger = logging.getLogger(__name__)

# ── 默认 ETL 计划 ────────────────────────────────────────────────────
# 标准 ETL 流程几乎不变（连接→选表→建表→映射→检查→溯源），
# 无需浪费一次 LLM 调用（~8-15s）让 Planner "想出"这个固定流程。
# 首条消息直接使用硬编码计划，跳过 Planner 节点。
#
# 如果用户中途改变需求（如修改目标表结构），由 analyzer 节点的
# action="replan" + updated_plan 处理，同样不需要 Planner 节点。
#
# Planner 节点已从图中移除。
DEFAULT_PLAN = [
    ETLStep(index=1, title="连接数据库", description="验证用户提供的数据库连接串，建立连接"),
    ETLStep(index=2, title="选择基表", description="查看用户指定的源表结构和样例数据"),
    ETLStep(index=3, title="定义目标表结构", description="根据用户需求生成 CREATE TABLE SQL，等待用户确认后执行"),
    ETLStep(index=4, title="建立字段映射", description="根据用户描述的映射规则生成 INSERT INTO ... SELECT SQL，等待用户确认后执行"),
    ETLStep(index=5, title="数据质量检查", description="检查目标表的数据质量（行数、空值率等）"),
    ETLStep(index=6, title="异常溯源", description="如发现数据异常，追溯到源表分析原因"),
    ETLStep(index=7, title="生成血缘图谱", description="根据字段映射 SQL 分析字段级数据血缘关系，调用 generate_lineage 工具生成 Mermaid 图谱，通过 render 展示"),
]


def init_plan(state: ETLState) -> dict:
    """首条消息入口：注入默认计划，直接进入 executor，省掉 Planner 的 LLM 调用。"""
    logger.info("[init_plan] 注入默认 ETL 计划（%d 步），跳过 Planner", len(DEFAULT_PLAN))
    return {
        "plan": [step.model_copy() for step in DEFAULT_PLAN],
        "current_step": 0,
        "response": None,
    }


def route_entry(state: ETLState) -> str:
    """入口路由：无计划时注入默认计划，有计划则直接执行。"""
    plan = state.get("plan", [])
    if plan and state.get("current_step", 0) < len(plan):
        logger.debug("[graph] 入口路由 -> executor（已有计划，继续执行）")
        return "executor"
    # 无计划（首条消息）或所有步骤已完成（用户发起新任务）→ 注入默认计划
    logger.debug("[graph] 入口路由 -> init_plan（注入默认计划）")
    return "init_plan"


def route_after_analyzer(state: ETLState) -> str:
    """analyzer 之后的路由：有 response 则结束，步骤越界则结束，否则继续执行"""
    if state.get("response"):
        logger.debug("[graph] analyzer 路由 -> end（有 response）")
        return "end"
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)
    if current_step >= len(plan):
        logger.warning("[graph] analyzer 路由 -> end（步骤越界: %d >= %d）", current_step, len(plan))
        return "end"
    logger.debug("[graph] analyzer 路由 -> executor")
    return "executor"


def build_graph():
    logger.info("[graph] 开始构建 ETL Agent 流程图")

    graph = StateGraph(ETLState)

    # 添加节点
    # 注意：没有 planner 节点。默认计划由 init_plan 硬编码注入，
    # replan 由 analyzer 的 action="replan" 直接返回新计划。
    graph.add_node("init_plan", init_plan)
    graph.add_node("executor", executor)
    graph.add_node("tools", parallel_tool_node)
    graph.add_node("analyzer", analyzer)

    # 入口路由：首次 → init_plan → executor，后续 → executor
    graph.add_conditional_edges(
        START, route_entry, {"init_plan": "init_plan", "executor": "executor"}
    )
    graph.add_edge("init_plan", "executor")

    # executor → tools_condition（有 tool_call 则走 tools，否则走 analyzer）
    graph.add_conditional_edges(
        "executor",
        tools_condition,
        {"tools": "tools", "__end__": "analyzer"},
    )

    # tools → executor（ReAct 循环）
    graph.add_edge("tools", "executor")

    # analyzer → executor 或 END
    graph.add_conditional_edges(
        "analyzer",
        route_after_analyzer,
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
