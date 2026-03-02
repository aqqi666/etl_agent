import logging

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import settings

logger = logging.getLogger(__name__)

_engines: dict[str, Engine] = {}
_current_connection: str | None = None

# SQL 关键词 → MOI operation 映射
_SQL_TO_MOI_OPERATION: dict[str, str] = {
    "CREATE": "create_table",
    "INSERT": "insert",
    "REPLACE": "replace",
    "UPDATE": "update",
    "DELETE": "delete",
    "TRUNCATE": "truncate",
    "ALTER": "alter_table",
}


def get_current_connection() -> str | None:
    """获取当前活跃的连接串"""
    return _current_connection


def set_current_connection(connection_string: str) -> None:
    """设置当前活跃的连接串"""
    global _current_connection
    _current_connection = connection_string


def resolve_connection(connection_string: str | None) -> str:
    """解析连接串：有传入则用传入的，否则用当前活跃连接"""
    if connection_string:
        return connection_string
    if _current_connection:
        return _current_connection
    raise ValueError("没有可用的数据库连接，请先调用 test_connection 建立连接")


def _ensure_charset(connection_string: str) -> str:
    """确保连接串包含 charset=utf8mb4"""
    if "charset=" not in connection_string:
        sep = "&" if "?" in connection_string else "?"
        return f"{connection_string}{sep}charset=utf8mb4"
    return connection_string


def _moi_enabled() -> bool:
    """检查 MOI 配置是否完整"""
    return bool(settings.moi_key and settings.moi_base_url)


def _is_write_sql(sql: str) -> bool:
    """判断 SQL 是否为写操作"""
    first_word = sql.strip().split()[0].upper() if sql.strip() else ""
    return first_word in _SQL_TO_MOI_OPERATION


def _get_moi_operation(sql: str) -> str:
    """从 SQL 获取 MOI operation 类型"""
    first_word = sql.strip().split()[0].upper() if sql.strip() else ""
    return _SQL_TO_MOI_OPERATION.get(first_word, "run_sql")


def _execute_via_moi(sql: str) -> list[dict]:
    """通过 MOI REST API 执行写操作 SQL"""
    base_url = settings.moi_base_url.rstrip("/")
    url = f"{base_url}/catalog/nl2sql/run_sql"
    operation = _get_moi_operation(sql)

    headers = {
        "Content-Type": "application/json",
        "moi-key": settings.moi_key,
    }
    payload = {
        "operation": operation,
        "statement": sql,
    }

    logger.info("[moi] 通过 MOI API 执行 (operation=%s): %s", operation, sql[:300])

    resp = httpx.post(url, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    result = resp.json()
    logger.info("[moi] MOI API 返回: %s", str(result)[:500])

    # 检查业务错误
    code = result.get("code", "")
    if code != "OK":
        msg = result.get("msg", "未知错误")
        raise RuntimeError(f"MOI API 错误: {msg}")

    # 兼容现有返回格式
    return [{"affected_rows": 0, "moi_response": result}]


def get_engine(connection_string: str) -> Engine:
    if connection_string not in _engines:
        safe_conn = connection_string.split("@")[-1] if "@" in connection_string else connection_string
        logger.info("[db] 创建新数据库引擎: %s", safe_conn)
        _engines[connection_string] = create_engine(
            _ensure_charset(connection_string), pool_pre_ping=True, pool_size=5
        )
    return _engines[connection_string]


def execute_sql_query(connection_string: str, sql: str) -> list[dict]:
    logger.info("[db] 执行 SQL: %s", sql[:300])

    # MOI 已配置且为写操作 → 走 MOI API
    if _moi_enabled() and _is_write_sql(sql):
        return _execute_via_moi(sql)

    # 其他走 SQLAlchemy
    engine = get_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        if result.returns_rows:
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            logger.info("[db] 查询返回 %d 行", len(rows))
            return rows
        conn.commit()
        affected = result.rowcount
        logger.info("[db] 执行完成，影响 %d 行", affected)
        return [{"affected_rows": affected}]


def test_db_connection(connection_string: str) -> dict:
    safe_conn = connection_string.split("@")[-1] if "@" in connection_string else connection_string
    logger.info("[db] 测试数据库连接: %s", safe_conn)
    engine = get_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    set_current_connection(connection_string)
    logger.info("[db] 连接测试成功，已设为当前活跃连接")
    return {"status": "ok", "message": "连接成功"}
