from fastapi import FastAPI
from app.api.websocket import router as ws_router

app = FastAPI(title="ETL Agent")
app.include_router(ws_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
