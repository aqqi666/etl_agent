"""端到端测试：模拟用户通过 WebSocket 与 ETL Agent 交互"""

import json
import sys
import websocket

WS_URL = "ws://localhost:8001/ws/test-session-1"
CONN_STR = "mysql+pymysql://root:test123@127.0.0.1:3307/source_db"

TIMEOUT = 90  # 单轮最大等待秒数


def send_and_receive(ws, content: str):
    """发送一条消息，收集所有响应直到 done/error"""
    print(f"\n{'='*60}")
    print(f"[用户]: {content}")
    print(f"{'='*60}")

    ws.send(json.dumps({"type": "chat", "content": content}))

    ws.settimeout(TIMEOUT)
    while True:
        try:
            raw = ws.recv()
            data = json.loads(raw)
            msg_type = data.get("type", "")
            msg_content = data.get("content", "")

            if msg_type == "text_delta":
                print(msg_content, end="", flush=True)
            elif msg_type == "plan":
                print(f"\n[计划]:\n{msg_content}")
            elif msg_type == "observation":
                print(f"\n[分析结果]:\n{msg_content}")
            elif msg_type == "tool_call":
                name = msg_content.get("name", "") if isinstance(msg_content, dict) else ""
                if name:
                    print(f"\n[工具调用]: {name}")
            elif msg_type == "done":
                print("\n[完成]")
                return True
            elif msg_type == "error":
                print(f"\n[错误]: {msg_content}")
                return False
        except websocket.WebSocketTimeoutException:
            print("\n[超时]")
            return False


def main():
    print("连接 WebSocket...")
    ws = websocket.create_connection(WS_URL, timeout=10)
    print("已连接")

    try:
        # 测试1: 连接数据库
        send_and_receive(ws, f"连接数据库 {CONN_STR}")

        # 测试2: 查看源表
        send_and_receive(ws, "用 source_db.orders 表做数据加工")

    finally:
        ws.close()
        print("\n连接已关闭")


if __name__ == "__main__":
    main()
