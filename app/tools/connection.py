import logging

from langchain_core.tools import tool

from app.db.executor import test_db_connection

logger = logging.getLogger(__name__)


@tool
def test_connection(connection_string: str) -> str:
    """测试数据库连接是否可用。参数 connection_string 为数据库连接串，如 mysql+pymysql://user:pass@host:3306/db"""
    logger.info("[tool:test_connection] 测试连接")
    try:
        result = test_db_connection(connection_string)
        logger.info("[tool:test_connection] 连接成功")
        return f"连接成功: {result['message']}"
    except Exception as e:
        logger.error("[tool:test_connection] 连接失败: %s", e)
        return f"连接失败: {e}"
