from __future__ import annotations

import json
import logging
import traceback
import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from langchain_core.messages import AIMessageChunk, HumanMessage

from app.agent.graph import etl_graph
from app.agent.state import ETLArtifacts

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/{session_id}")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    logger.info("[session=%s] WebSocket 连接已建立", session_id)

    # 每个 session 维护独立的 graph 状态
    graph_state = {
        "messages": [],
        "plan": [],
        "current_step": 0,
        "past_steps": [],
        "artifacts": ETLArtifacts(),
        "response": None,
    }

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

            # 添加用户消息，清除上一轮 response
            graph_state["messages"].append(HumanMessage(content=content))
            graph_state["response"] = None

            try:
                async for stream_mode, chunk in etl_graph.astream(
                    graph_state,
                    stream_mode=["messages", "updates", "custom"],
                ):
                    if stream_mode == "messages":
                        msg, metadata = chunk
                        # 流式转发 LLM 文本 token
                        if isinstance(msg, AIMessageChunk) and msg.content:
                            await websocket.send_json({
                                "type": "text_delta",
                                "content": msg.content,
                            })
                        # 转发 tool_call 信息
                        if isinstance(msg, AIMessageChunk) and msg.tool_call_chunks:
                            for tc in msg.tool_call_chunks:
                                await websocket.send_json({
                                    "type": "tool_call",
                                    "content": {
                                        "name": tc.get("name", ""),
                                        "args": tc.get("args", ""),
                                    },
                                })

                    elif stream_mode == "custom":
                        # observer/planner 推送的结构化数据
                        await websocket.send_json(chunk)

                    elif stream_mode == "updates":
                        # 节点状态更新 — 同步 graph_state
                        if isinstance(chunk, dict):
                            for node_name, node_output in chunk.items():
                                if isinstance(node_output, dict):
                                    for k, v in node_output.items():
                                        if k in graph_state:
                                            graph_state[k] = v

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
