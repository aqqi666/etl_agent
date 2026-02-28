from langchain_core.tools import tool
from app.db.executor import test_db_connection


@tool
def test_connection(connection_string: str) -> str:
    """测试数据库连接是否可用。参数 connection_string 为数据库连接串，如 mysql+pymysql://user:pass@host:3306/db"""
    try:
        result = test_db_connection(connection_string)
        return f"连接成功: {result['message']}"
    except Exception as e:
        return f"连接失败: {e}"
