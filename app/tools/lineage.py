import logging
from collections import defaultdict

from langchain_core.tools import tool

from app.tools.rich_result import make_rich_result

logger = logging.getLogger(__name__)


@tool
def generate_lineage(nodes: list[dict], edges: list[dict], title: str = "") -> str:
    """根据字段级血缘的节点和边生成 Mermaid 图谱。

    Args:
        nodes: 节点列表，每个节点包含:
            - id: 唯一标识，如 "s1"
            - table: 所属表全名，如 "source_db.orders"
            - column: 字段名，如 "quantity"
        edges: 边列表，每条边包含:
            - from: 源节点 id
            - to: 目标节点 id
            - label: 可选，转换说明，如 "× unit_price" 或 "JOIN region_code"
        title: 图谱标题（可选）
    """
    logger.info("[tool:generate_lineage] 生成血缘图谱，%d 节点, %d 边", len(nodes), len(edges))

    # 按 table 分组
    groups: dict[str, list[dict]] = defaultdict(list)
    for node in nodes:
        groups[node["table"]].append(node)

    # 生成 Mermaid
    lines = ["graph LR"]
    for table_name, table_nodes in groups.items():
        lines.append(f"  subgraph {table_name}")
        for n in table_nodes:
            lines.append(f"    {n['id']}[{n['column']}]")
        lines.append("  end")

    for edge in edges:
        src = edge["from"]
        tgt = edge["to"]
        label = edge.get("label", "")
        if label:
            lines.append(f'  {src} -->|"{label}"| {tgt}')
        else:
            lines.append(f"  {src} --> {tgt}")

    mermaid_code = "\n".join(lines)

    return make_rich_result(
        tool_name="generate_lineage",
        result_type="lineage",
        title=title or "字段级数据血缘图谱",
        text=mermaid_code,
        summary=f"已生成血缘图谱，包含 {len(nodes)} 个字段节点和 {len(edges)} 条血缘关系",
        metadata={"mermaid": mermaid_code},
    )
