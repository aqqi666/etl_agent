from __future__ import annotations

import json
import logging

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

    # 使用 checkpointer 的 thread_id 让 graph 自动管理状态
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
                # 只传增量输入，由 checkpointer 自动管理累积 state
                graph_input = {
                    "messages": [HumanMessage(content=content)],
                    "response": None,  # 清除上一轮的 response，让 graph 能重新进入
                }

                async for stream_mode, chunk in etl_graph.astream(
                    graph_input,
                    config=config,
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
                        # 推送步骤进度信息
                        if isinstance(chunk, dict):
                            for node_name, node_output in chunk.items():
                                if isinstance(node_output, dict):
                                    # 推送步骤进度
                                    if "current_step" in node_output and "plan" not in node_output:
                                        # 获取当前 state 来读取 plan
                                        snapshot = etl_graph.get_state(config)
                                        plan = snapshot.values.get("plan", [])
                                        step_idx = node_output["current_step"]
                                        if step_idx < len(plan):
                                            await websocket.send_json({
                                                "type": "step_progress",
                                                "current_step": step_idx + 1,
                                                "total_steps": len(plan),
                                                "title": plan[step_idx].title,
                                            })

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
