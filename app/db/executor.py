import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engines: dict[str, Engine] = {}
_current_connection: str | None = None


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
