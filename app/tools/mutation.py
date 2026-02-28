from langchain_core.tools import tool
from app.db.executor import execute_sql_query


@tool
def execute_sql(connection_string: str, sql: str) -> str:
    """执行 DDL/DML SQL 语句（CREATE/INSERT/UPDATE/DELETE/ALTER 等）。
    注意：这是危险操作，调用前应先向用户展示 SQL 并获得确认。"""
    try:
        rows = execute_sql_query(connection_string, sql)
        if rows and "affected_rows" in rows[0]:
            return f"执行成功，影响 {rows[0]['affected_rows']} 行"
        return "执行成功"
    except Exception as e:
        return f"执行失败: {e}"
