import logging

from langchain_core.tools import tool

from app.db.executor import execute_sql_query, resolve_connection

logger = logging.getLogger(__name__)

# 数值类型前缀，用于判断字段是否为数值字段以计算 MIN/MAX/AVG
_NUMERIC_TYPE_PREFIXES = ("int", "tinyint", "smallint", "mediumint", "bigint",
                          "float", "double", "decimal", "numeric", "real")


@tool
def check_data_quality(database: str, table: str, connection_string: str = "") -> str:
    """检查目标表的数据质量：总行数、每列空值统计、数值字段极值（MIN/MAX/AVG）。
    connection_string 可不传，自动使用已建立的连接。"""
    logger.info("[tool:check_data_quality] 检查 %s.%s 数据质量", database, table)
    try:
        conn_str = resolve_connection(connection_string or None)

        # ── 总行数 ──
        count_result = execute_sql_query(
            conn_str,
            f"SELECT COUNT(*) AS total FROM `{database}`.`{table}`",
        )
        total = count_result[0]["total"]

        # ── 获取列信息 ──
        columns_result = execute_sql_query(
            conn_str,
            f"SHOW COLUMNS FROM `{database}`.`{table}`",
        )
        columns = [r["Field"] for r in columns_result]

        # 识别数值类型字段
        numeric_columns = [
            r["Field"] for r in columns_result
            if r.get("Type", "").lower().split("(")[0] in _NUMERIC_TYPE_PREFIXES
        ]

        # ── 每列空值数 ──
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
            rate = f"{nulls / total * 100:.2f}%" if total > 0 else "N/A"
            lines.append(f"| {c} | {nulls} | {rate} |")

        # ── 数值字段极值（MIN / MAX / AVG）──
        if numeric_columns:
            agg_parts = []
            for c in numeric_columns:
                agg_parts.append(f"MIN(`{c}`) AS `min_{c}`")
                agg_parts.append(f"MAX(`{c}`) AS `max_{c}`")
                agg_parts.append(f"AVG(`{c}`) AS `avg_{c}`")
            agg_sql = f"SELECT {', '.join(agg_parts)} FROM `{database}`.`{table}`"
            agg_result = execute_sql_query(conn_str, agg_sql)
            agg_row = agg_result[0] if agg_result else {}

            lines.append("")
            lines.append("### 数值字段极值")
            lines.append("| 字段 | 最小值 | 最大值 | 平均值 |")
            lines.append("| --- | --- | --- | --- |")
            for c in numeric_columns:
                min_v = agg_row.get(f"min_{c}", "N/A")
                max_v = agg_row.get(f"max_{c}", "N/A")
                avg_v = agg_row.get(f"avg_{c}", "N/A")
                # 格式化平均值为 2 位小数
                if isinstance(avg_v, (int, float)):
                    avg_v = f"{avg_v:.2f}"
                lines.append(f"| {c} | {min_v} | {max_v} | {avg_v} |")

        logger.info("[tool:check_data_quality] 检查完成，总行数: %d, 数值字段: %d 个", total, len(numeric_columns))
        return "\n".join(lines)
    except Exception as e:
        logger.error("[tool:check_data_quality] 失败: %s", e)
        return f"数据质量检查失败: {e}"
