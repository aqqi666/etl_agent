import logging

from langchain_core.tools import tool

from app.db.executor import execute_sql_query, resolve_connection

logger = logging.getLogger(__name__)


@tool
def list_databases(connection_string: str = "") -> str:
    """列出数据库服务器上的所有数据库。connection_string 可不传，自动使用已建立的连接。"""
    logger.info("[tool:list_databases] 列出所有数据库")
    try:
        conn_str = resolve_connection(connection_string or None)
        rows = execute_sql_query(conn_str, "SHOW DATABASES")
        dbs = [list(r.values())[0] for r in rows]
        logger.info("[tool:list_databases] 找到 %d 个数据库", len(dbs))
        return "数据库列表:\n" + "\n".join(f"- {db}" for db in dbs)
    except Exception as e:
        logger.error("[tool:list_databases] 失败: %s", e)
        return f"查询失败: {e}"


@tool
def list_tables(database: str, connection_string: str = "") -> str:
    """列出指定数据库中的所有表。connection_string 可不传，自动使用已建立的连接。"""
    logger.info("[tool:list_tables] 列出 %s 中的表", database)
    try:
        conn_str = resolve_connection(connection_string or None)
        rows = execute_sql_query(conn_str, f"SHOW TABLES FROM `{database}`")
        tables = [list(r.values())[0] for r in rows]
        logger.info("[tool:list_tables] 找到 %d 张表", len(tables))
        return f"数据库 {database} 中的表:\n" + "\n".join(f"- {t}" for t in tables)
    except Exception as e:
        logger.error("[tool:list_tables] 失败: %s", e)
        return f"查询失败: {e}"


@tool
def describe_table(database: str, table: str, connection_string: str = "") -> str:
    """查看表结构（字段名、类型、是否为空、默认值等）。connection_string 可不传，自动使用已建立的连接。"""
    logger.info("[tool:describe_table] 查看 %s.%s 表结构", database, table)
    try:
        conn_str = resolve_connection(connection_string or None)
        rows = execute_sql_query(
            conn_str,
            f"SHOW FULL COLUMNS FROM `{database}`.`{table}`",
        )
        lines = ["| 字段 | 类型 | 允许空 | 键 | 默认值 | 备注 |", "| --- | --- | --- | --- | --- | --- |"]
        for r in rows:
            lines.append(
                f"| {r.get('Field', '')} | {r.get('Type', '')} | {r.get('Null', '')} "
                f"| {r.get('Key', '')} | {r.get('Default', '')} | {r.get('Comment', '')} |"
            )
        logger.info("[tool:describe_table] 表 %s.%s 有 %d 个字段", database, table, len(rows))
        return f"表 {database}.{table} 结构:\n" + "\n".join(lines)
    except Exception as e:
        logger.error("[tool:describe_table] 失败: %s", e)
        return f"查询失败: {e}"


@tool
def get_column_details(database: str, table: str, column: str, connection_string: str = "") -> str:
    """获取指定列的详细信息，包括唯一值数量、样本值等。connection_string 可不传，自动使用已建立的连接。"""
    logger.info("[tool:get_column_details] 查看 %s.%s.%s 列详情", database, table, column)
    try:
        conn_str = resolve_connection(connection_string or None)
        count_result = execute_sql_query(
            conn_str,
            f"SELECT COUNT(DISTINCT `{column}`) AS cnt FROM `{database}`.`{table}`",
        )
        distinct_count = count_result[0]["cnt"]

        sample_result = execute_sql_query(
            conn_str,
            f"SELECT DISTINCT `{column}` AS val FROM `{database}`.`{table}` LIMIT 20",
        )
        samples = [str(r["val"]) for r in sample_result]

        logger.info("[tool:get_column_details] 列 %s 有 %d 个唯一值", column, distinct_count)
        return (
            f"列 {database}.{table}.{column} 详情:\n"
            f"- 唯一值数量: {distinct_count}\n"
            f"- 样本值: {', '.join(samples)}"
        )
    except Exception as e:
        logger.error("[tool:get_column_details] 失败: %s", e)
        return f"查询失败: {e}"
