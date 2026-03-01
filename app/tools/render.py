import logging

from langchain_core.tools import tool

logger = logging.getLogger(__name__)


@tool
def render(tool_call_ids: list[str] | None = None, text: str = "") -> str:
    """展示工具执行结果给用户。先调用数据查询工具获取结果，看到摘要后再调用此工具展示。

    Args:
        tool_call_ids: 要展示的工具调用 ID 列表。不传则展示所有未展示的工具结果。
        text: 附加的说明文字或 SQL 代码块（可选），会追加到工具结果之后
    """
    # 此函数不会被实际调用 —— parallel_tool_node 会拦截 render 工具调用，
    # 从缓存中取出结构化结果，格式化为 Markdown 后存入 state。
    return "已渲染"
