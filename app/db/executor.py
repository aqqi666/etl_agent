import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engines: dict[str, Engine] = {}


def get_engine(connection_string: str) -> Engine:
    if connection_string not in _engines:
        # 日志中隐藏密码
        safe_conn = connection_string.split("@")[-1] if "@" in connection_string else connection_string
        logger.info("[db] 创建新数据库引擎: %s", safe_conn)
        _engines[connection_string] = create_engine(
            connection_string, pool_pre_ping=True, pool_size=5
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
    logger.info("[db] 连接测试成功")
    return {"status": "ok", "message": "连接成功"}
