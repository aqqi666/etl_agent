import logging

from fastapi import FastAPI

from app.logging_config import setup_logging

setup_logging()

from app.api.websocket import router as ws_router

logger = logging.getLogger(__name__)

app = FastAPI(title="ETL Agent")
app.include_router(ws_router)

logger.info("ETL Agent 服务已初始化")

if __name__ == "__main__":
    import uvicorn

    logger.info("启动 Uvicorn 服务 0.0.0.0:8000")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
