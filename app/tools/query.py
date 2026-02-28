from langchain_core.tools import tool
from app.db.executor import execute_sql_query


@tool
def execute_query(connection_string: str, sql: str) -> str:
    """执行只读 SQL 查询（SELECT），返回结果。用于数据预览和分析。"""
    try:
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("SHOW") and not sql_upper.startswith("DESC"):
            return "错误: 此工具仅支持 SELECT/SHOW/DESC 查询，修改操作请使用 execute_sql"
        rows = execute_sql_query(connection_string, sql)
        if not rows:
            return "查询返回 0 行数据"
        columns = list(rows[0].keys())
        lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
        for r in rows[:50]:
            lines.append("| " + " | ".join(str(r.get(c, "")) for c in columns) + " |")
        result = "\n".join(lines)
        if len(rows) > 50:
            result += f"\n\n... 共 {len(rows)} 行，仅显示前 50 行"
        return result
    except Exception as e:
        return f"查询失败: {e}"


@tool
def preview_data(connection_string: str, database: str, table: str, limit: int = 10) -> str:
    """预览表中的前 N 行数据。"""
    try:
        rows = execute_sql_query(
            connection_string,
            f"SELECT * FROM `{database}`.`{table}` LIMIT {limit}",
        )
        if not rows:
            return f"表 {database}.{table} 没有数据"
        columns = list(rows[0].keys())
        lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
        for r in rows:
            lines.append("| " + " | ".join(str(r.get(c, "")) for c in columns) + " |")
        return f"表 {database}.{table} 前 {limit} 行:\n" + "\n".join(lines)
    except Exception as e:
        return f"查询失败: {e}"
