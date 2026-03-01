import logging

from langchain_core.tools import tool

from app.db.executor import execute_sql_query, resolve_connection
from app.tools.rich_result import make_rich_result

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
        text = "数据库列表:\n" + "\n".join(f"- {db}" for db in dbs)
        summary = f"共 {len(dbs)} 个数据库: {', '.join(dbs)}"
        return make_rich_result(
            tool_name="list_databases",
            result_type="text",
            title="数据库列表",
            sql="SHOW DATABASES",
            text=text,
            summary=summary,
        )
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
        text = f"数据库 {database} 中的表:\n" + "\n".join(f"- {t}" for t in tables)
        summary = f"数据库 {database} 共 {len(tables)} 张表: {', '.join(tables)}"
        return make_rich_result(
            tool_name="list_tables",
            result_type="text",
            title=f"数据库 {database} 中的表",
            sql=f"SHOW TABLES FROM `{database}`",
            text=text,
            summary=summary,
        )
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
        columns = ["字段", "类型", "允许空", "键", "默认值", "备注"]
        table_rows = []
        for r in rows:
            table_rows.append({
                "字段": r.get("Field", ""),
                "类型": r.get("Type", ""),
                "允许空": r.get("Null", ""),
                "键": r.get("Key", ""),
                "默认值": str(r.get("Default", "")),
                "备注": r.get("Comment", ""),
            })
        field_names = [r.get("Field", "") for r in rows]
        summary = f"表 {database}.{table} 有 {len(rows)} 个字段: {', '.join(field_names)}"
        logger.info("[tool:describe_table] 表 %s.%s 有 %d 个字段", database, table, len(rows))
        return make_rich_result(
            tool_name="describe_table",
            result_type="table",
            title=f"表 {database}.{table} 结构",
            sql=f"SHOW FULL COLUMNS FROM `{database}`.`{table}`",
            columns=columns,
            rows=table_rows,
            total_rows=len(rows),
            summary=summary,
        )
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
        text = (
            f"列 {database}.{table}.{column} 详情:\n"
            f"- 唯一值数量: {distinct_count}\n"
            f"- 样本值: {', '.join(samples)}"
        )
        summary = f"列 {database}.{table}.{column} 有 {distinct_count} 个唯一值，样本: {', '.join(samples[:5])}"
        return make_rich_result(
            tool_name="get_column_details",
            result_type="text",
            title=f"列 {database}.{table}.{column} 详情",
            text=text,
            summary=summary,
            metadata={"distinct_count": distinct_count, "samples": samples},
        )
    except Exception as e:
        logger.error("[tool:get_column_details] 失败: %s", e)
        return f"查询失败: {e}"
