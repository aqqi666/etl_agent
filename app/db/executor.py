from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


_engines: dict[str, Engine] = {}


def get_engine(connection_string: str) -> Engine:
    if connection_string not in _engines:
        _engines[connection_string] = create_engine(
            connection_string, pool_pre_ping=True, pool_size=5
        )
    return _engines[connection_string]


def execute_sql_query(connection_string: str, sql: str) -> list[dict]:
    engine = get_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        if result.returns_rows:
            columns = list(result.keys())
            return [dict(zip(columns, row)) for row in result.fetchall()]
        conn.commit()
        return [{"affected_rows": result.rowcount}]


def test_db_connection(connection_string: str) -> dict:
    engine = get_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok", "message": "连接成功"}
