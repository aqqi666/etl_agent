from langchain_core.tools import tool
from app.db.executor import execute_sql_query


@tool
def list_databases(connection_string: str) -> str:
    """列出数据库服务器上的所有数据库。"""
    try:
        rows = execute_sql_query(connection_string, "SHOW DATABASES")
        dbs = [list(r.values())[0] for r in rows]
        return "数据库列表:\n" + "\n".join(f"- {db}" for db in dbs)
    except Exception as e:
        return f"查询失败: {e}"


@tool
def list_tables(connection_string: str, database: str) -> str:
    """列出指定数据库中的所有表。"""
    try:
        rows = execute_sql_query(connection_string, f"SHOW TABLES FROM `{database}`")
        tables = [list(r.values())[0] for r in rows]
        return f"数据库 {database} 中的表:\n" + "\n".join(f"- {t}" for t in tables)
    except Exception as e:
        return f"查询失败: {e}"


@tool
def describe_table(connection_string: str, database: str, table: str) -> str:
    """查看表结构（字段名、类型、是否为空、默认值等）。"""
    try:
        rows = execute_sql_query(
            connection_string,
            f"SHOW FULL COLUMNS FROM `{database}`.`{table}`",
        )
        lines = ["| 字段 | 类型 | 允许空 | 键 | 默认值 | 备注 |", "| --- | --- | --- | --- | --- | --- |"]
        for r in rows:
            lines.append(
                f"| {r.get('Field', '')} | {r.get('Type', '')} | {r.get('Null', '')} "
                f"| {r.get('Key', '')} | {r.get('Default', '')} | {r.get('Comment', '')} |"
            )
        return f"表 {database}.{table} 结构:\n" + "\n".join(lines)
    except Exception as e:
        return f"查询失败: {e}"


@tool
def get_column_details(connection_string: str, database: str, table: str, column: str) -> str:
    """获取指定列的详细信息，包括唯一值数量、样本值等。"""
    try:
        count_result = execute_sql_query(
            connection_string,
            f"SELECT COUNT(DISTINCT `{column}`) AS cnt FROM `{database}`.`{table}`",
        )
        distinct_count = count_result[0]["cnt"]

        sample_result = execute_sql_query(
            connection_string,
            f"SELECT DISTINCT `{column}` AS val FROM `{database}`.`{table}` LIMIT 20",
        )
        samples = [str(r["val"]) for r in sample_result]

        return (
            f"列 {database}.{table}.{column} 详情:\n"
            f"- 唯一值数量: {distinct_count}\n"
            f"- 样本值: {', '.join(samples)}"
        )
    except Exception as e:
        return f"查询失败: {e}"
