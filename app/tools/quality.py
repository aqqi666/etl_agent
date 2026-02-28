import logging

from langchain_core.tools import tool

from app.db.executor import execute_sql_query, resolve_connection

logger = logging.getLogger(__name__)


@tool
def check_data_quality(database: str, table: str, connection_string: str = "") -> str:
    """检查目标表的数据质量：总行数、空值统计、重复行等。connection_string 可不传，自动使用已建立的连接。"""
    logger.info("[tool:check_data_quality] 检查 %s.%s 数据质量", database, table)
    try:
        conn_str = resolve_connection(connection_string or None)
        # 总行数
        count_result = execute_sql_query(
            conn_str,
            f"SELECT COUNT(*) AS total FROM `{database}`.`{table}`",
        )
        total = count_result[0]["total"]

        # 获取列信息
        columns_result = execute_sql_query(
            conn_str,
            f"SHOW COLUMNS FROM `{database}`.`{table}`",
        )
        columns = [r["Field"] for r in columns_result]

        # 每列空值数
        null_checks = ", ".join(
            f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}_nulls`"
            for c in columns
        )
        null_result = execute_sql_query(
            conn_str,
            f"SELECT {null_checks} FROM `{database}`.`{table}`",
        )
        null_row = null_result[0] if null_result else {}

        lines = [
            f"## 数据质量报告: {database}.{table}",
            f"- 总行数: {total}",
            "",
            "### 空值统计",
            "| 字段 | 空值数 | 空值率 |",
            "| --- | --- | --- |",
        ]
        for c in columns:
            nulls = null_row.get(f"{c}_nulls", 0)
            rate = f"{nulls / total * 100:.1f}%" if total > 0 else "N/A"
            lines.append(f"| {c} | {nulls} | {rate} |")

        logger.info("[tool:check_data_quality] 检查完成，总行数: %d", total)
        return "\n".join(lines)
    except Exception as e:
        logger.error("[tool:check_data_quality] 失败: %s", e)
        return f"数据质量检查失败: {e}"
