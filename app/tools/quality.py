from langchain_core.tools import tool
from app.db.executor import execute_sql_query


@tool
def check_data_quality(connection_string: str, database: str, table: str) -> str:
    """检查目标表的数据质量：总行数、空值统计、重复行等。"""
    try:
        # 总行数
        count_result = execute_sql_query(
            connection_string,
            f"SELECT COUNT(*) AS total FROM `{database}`.`{table}`",
        )
        total = count_result[0]["total"]

        # 获取列信息
        columns_result = execute_sql_query(
            connection_string,
            f"SHOW COLUMNS FROM `{database}`.`{table}`",
        )
        columns = [r["Field"] for r in columns_result]

        # 每列空值数
        null_checks = ", ".join(
            f"SUM(CASE WHEN `{c}` IS NULL THEN 1 ELSE 0 END) AS `{c}_nulls`"
            for c in columns
        )
        null_result = execute_sql_query(
            connection_string,
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

        return "\n".join(lines)
    except Exception as e:
        return f"数据质量检查失败: {e}"
