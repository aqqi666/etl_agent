"""结构化工具结果辅助函数。

数据查询类工具调用 make_rich_result() 返回带 __structured__ 标记的 JSON，
parallel_tool_node 据此分离出结构化 payload（供 render 工具格式化）和文本摘要（传给 LLM）。
"""

from __future__ import annotations

import json
from typing import Any


def make_rich_result(
    tool_name: str,
    result_type: str,
    title: str,
    *,
    sql: str | None = None,
    columns: list[str] | None = None,
    rows: list[dict[str, Any]] | None = None,
    total_rows: int | None = None,
    text: str | None = None,
    summary: str = "",
    metadata: dict[str, Any] | None = None,
) -> str:
    """构造工具的富结果 JSON。

    返回的 JSON 字符串包含:
    - __structured__: True  — 标记，供 parallel_tool_node 识别
    - payload: 结构化载荷，供 render 工具格式化为 Markdown
    - summary: 简短文本摘要，作为 ToolMessage.content 传给 LLM

    Args:
        tool_name: 工具名称
        result_type: 结果类型 (table | text | quality_report)
        title: 结果标题
        sql: 执行的 SQL 语句（可选）
        columns: 表格列名列表（可选）
        rows: 表格数据行列表（可选）
        total_rows: 总行数（可选，数据可能被截断）
        text: 纯文本结果（可选）
        summary: 给 LLM 的文本摘要
        metadata: 额外元数据（可选）
    """
    return json.dumps(
        {
            "__structured__": True,
            "payload": {
                "tool_name": tool_name,
                "result_type": result_type,
                "title": title,
                "sql": sql,
                "columns": columns,
                "rows": rows,
                "total_rows": total_rows,
                "text": text,
                "metadata": metadata or {},
            },
            "summary": summary,
        },
        ensure_ascii=False,
        default=str,
    )
