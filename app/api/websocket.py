from __future__ import annotations

import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from langchain_core.messages import AIMessage, HumanMessage

from app.agent.graph import etl_graph
from app.agent.state import ETLArtifacts
from app.db.executor import execute_sql_query, resolve_connection

logger = logging.getLogger(__name__)

router = APIRouter()

# ── 确认消息快速通道 ─────────────────────────────────────────────────

CONFIRM_KEYWORDS = frozenset({
    "确认", "确认执行", "确认建表", "是", "ok", "执行", "好的", "可以", "yes",
})


def _is_confirmation(content: str) -> bool:
    """判断用户消息是否为确认关键词"""
    return content.strip().strip("。.!！").lower() in CONFIRM_KEYWORDS


async def _try_fast_confirm(
    websocket: WebSocket, config: dict, content: str, session_id: str
) -> bool:
    """尝试走快速通道处理确认消息。返回 True 表示已处理，False 表示需走正常流程。"""
    if not _is_confirmation(content):
        return False

    # 读取 checkpointer 中的当前状态
    state_snapshot = await etl_graph.aget_state(config)
    if not state_snapshot or not state_snapshot.values:
        return False

    state = state_snapshot.values
    artifacts: ETLArtifacts | None = state.get("artifacts")
    if not artifacts:
        return False

    # 兼容 dict（反序列化可能返回 dict）
    if isinstance(artifacts, dict):
        artifacts = ETLArtifacts(**artifacts)

    # 判断是否有待执行的 SQL
    sql_to_execute = None
    action_type = None  # "create_table" | "mapping"

    if artifacts.target_ddl and not artifacts.target_created:
        sql_to_execute = artifacts.target_ddl
        action_type = "create_table"
    elif artifacts.field_mapping_sql and not artifacts.mapping_executed:
        sql_to_execute = artifacts.field_mapping_sql
        action_type = "mapping"

    if not sql_to_execute:
        return False  # 没有待执行的 SQL，走正常流程

    logger.info("[fast_confirm][session=%s] 触发快速通道: %s", session_id, action_type)

    # 直接执行 SQL
    try:
        conn_str = resolve_connection(artifacts.connection_string)
        rows = execute_sql_query(conn_str, sql_to_execute)
        affected = rows[0]["affected_rows"] if rows and "affected_rows" in rows[0] else 0
    except Exception as e:
        logger.error("[fast_confirm][session=%s] SQL 执行失败: %s", session_id, e)
        response_text = f"执行失败: {e}\n\n请检查 SQL 或提供新的指令。"
        await websocket.send_json({"type": "response", "content": response_text})
        await websocket.send_json({"type": "done"})
        return True

    # 构建响应文本和 artifacts 更新
    if action_type == "create_table":
        response_text = (
            f"目标表 `{artifacts.target_db}.{artifacts.target_table}` 已创建成功。"
            f"\n\n请描述字段映射规则。"
        )
        updated_artifacts = artifacts.model_copy(update={"target_created": True})
        step_summary = "目标表创建成功"
    else:
        response_text = (
            f"映射 SQL 执行成功，影响 {affected} 行。"
            f"\n\n请确认是否开始数据质量检查。"
        )
        updated_artifacts = artifacts.model_copy(update={"mapping_executed": True})
        step_summary = f"映射执行成功，影响 {affected} 行"

    # 更新 LangGraph checkpointer 中的状态
    plan = state.get("plan", [])
    current_step = state.get("current_step", 0)

    if current_step < len(plan):
        plan[current_step].status = "completed"
        step_title = plan[current_step].title
    else:
        step_title = "快速确认"

    await etl_graph.aupdate_state(
        config,
        {
            "messages": [
                HumanMessage(content=content),
                AIMessage(content=response_text),
            ],
            "artifacts": updated_artifacts,
            "current_step": current_step + 1,
            "plan": plan,
            "past_steps": [(step_title, step_summary)],
            "response": response_text,
            "observation_text": None,
        },
        as_node="replanner",
    )

    await websocket.send_json({"type": "response", "content": response_text})
    await websocket.send_json({"type": "done"})
    logger.info("[fast_confirm][session=%s] 快速通道完成: %s", session_id, action_type)
    return True


# ── WebSocket 端点 ────────────────────────────────────────────────────


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
                # 快速通道：确认消息直接执行 SQL，跳过 LLM 调用
                if await _try_fast_confirm(websocket, config, content, session_id):
                    continue

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
