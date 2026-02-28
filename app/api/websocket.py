from __future__ import annotations

import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from langchain_core.messages import HumanMessage

from app.agent.graph import etl_graph

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    logger.info("[session=%s] WebSocket 连接已建立", session_id)

    config = {"configurable": {"thread_id": session_id}}

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("[session=%s] 收到无效 JSON: %s", session_id, raw[:200])
                await websocket.send_json({"type": "error", "content": "无效的 JSON 格式"})
                continue

            msg_type = data.get("type", "chat")
            content = data.get("content", "")

            if msg_type != "chat" or not content.strip():
                continue

            logger.info("[session=%s] 收到用户消息: %s", session_id, content[:200])

            try:
                graph_input = {
                    "messages": [HumanMessage(content=content)],
                    "response": None,
                }

                async for stream_mode, chunk in etl_graph.astream(
                    graph_input,
                    config=config,
                    stream_mode=["updates", "custom"],
                ):
                    if stream_mode == "custom":
                        # 只转发最终结果给前端：
                        # - "response": observer 结果 + replanner 引导（合并后的完整回复）
                        # - "step_progress": 步骤进度
                        # 过滤掉内部消息（plan 等）
                        chunk_type = chunk.get("type", "")
                        if chunk_type in ("response", "step_progress"):
                            await websocket.send_json(chunk)

                logger.info("[session=%s] 本轮 agent 流程完成", session_id)
                await websocket.send_json({"type": "done"})

            except Exception as e:
                logger.exception("[session=%s] agent 处理异常", session_id)
                await websocket.send_json({
                    "type": "error",
                    "content": f"处理出错: {e}",
                })

    except WebSocketDisconnect:
        logger.info("[session=%s] WebSocket 连接已断开", session_id)
