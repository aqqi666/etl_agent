import logging

from langchain_core.tools import tool

from app.db.executor import execute_sql_query, resolve_connection

logger = logging.getLogger(__name__)


@tool
def execute_sql(sql: str, connection_string: str = "") -> str:
    """执行 DDL/DML SQL 语句（CREATE/INSERT/UPDATE/DELETE/ALTER 等）。
    注意：这是危险操作，调用前应先向用户展示 SQL 并获得确认。
    connection_string 可不传，自动使用已建立的连接。"""
    logger.warning("[tool:execute_sql] 执行修改操作: %s", sql[:300])
    try:
        conn_str = resolve_connection(connection_string or None)
        rows = execute_sql_query(conn_str, sql)
        if rows and "affected_rows" in rows[0]:
            logger.info("[tool:execute_sql] 成功，影响 %d 行", rows[0]["affected_rows"])
            return f"执行成功，影响 {rows[0]['affected_rows']} 行"
        logger.info("[tool:execute_sql] 执行成功")
        return "执行成功"
    except Exception as e:
        logger.error("[tool:execute_sql] 失败: %s", e)
        return f"执行失败: {e}"
