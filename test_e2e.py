"""端到端测试：模拟 PRD 完整6步 ETL 流程"""

import json

import websocket

WS_URL = "ws://localhost:8001/ws/test-full-flow-1"
CONN_STR = "mysql+pymysql://root:test123@127.0.0.1:3307/source_db"

TIMEOUT = 120  # 单轮最大等待秒数（复杂步骤可能需要更久）


def send_and_receive(ws, content: str) -> bool:
    """发送一条消息，收集所有响应直到 done/error"""
    print(f"\n{'=' * 70}")
    print(f"  [用户]: {content}")
    print(f"{'=' * 70}")

    ws.send(json.dumps({"type": "chat", "content": content}))

    ws.settimeout(TIMEOUT)
    while True:
        try:
            raw = ws.recv()
            data = json.loads(raw)
            msg_type = data.get("type", "")
            msg_content = data.get("content", "")

            if msg_type == "response":
                print(f"\n  [Agent]:\n{msg_content}")
            elif msg_type == "step_progress":
                step = data.get("current_step", "?")
                total = data.get("total_steps", "?")
                title = data.get("title", "")
                print(f"\n  [进度]: 步骤 {step}/{total} — {title}")
            elif msg_type == "done":
                print("\n  [完成]\n")
                return True
            elif msg_type == "error":
                print(f"\n  [错误]: {msg_content}\n")
                return False
        except websocket.WebSocketTimeoutException:
            print("\n  [超时]\n")
            return False


def run_step(ws, step_num, label: str, content: str) -> bool:
    print(f"\n{'#' * 70}")
    print(f"# 测试 {step_num}: {label}")
    print(f"{'#' * 70}")
    ok = send_and_receive(ws, content)
    if not ok:
        print(f"!!! 测试 {step_num} ({label}) 失败，终止 !!!")
    return ok


def main():
    print("=" * 70)
    print("  ETL Agent 端到端测试 — PRD 完整流程")
    print("=" * 70)
    print(f"  WebSocket: {WS_URL}")
    print(f"  连接串: {CONN_STR}")
    print()

    print("连接 WebSocket...")
    ws = websocket.create_connection(WS_URL, timeout=10)
    print("已连接\n")

    try:
        # ── 步骤1: 连接数据库 ──
        if not run_step(ws, 1, "连接数据库",
            f"连接数据库 {CONN_STR}"
        ):
            return

        # ── 步骤2: 选择基表 ──
        if not run_step(ws, 2, "选择基表",
            "用 source_db.orders 表做数据加工"
        ):
            return

        # ── 步骤3: 定义目标表结构 ──
        # 模仿 PRD: 在 test_db 下创建 order_summary 表
        if not run_step(ws, 3, "定义目标表结构",
            "在 test_db 库创建目标表 order_summary，包含字段：\n"
            "- order_no varchar(32) 订单编号\n"
            "- customer_name varchar(64) 客户姓名\n"
            "- product varchar(64) 产品名称\n"
            "- quantity int 数量\n"
            "- unit_price decimal(10,2) 单价\n"
            "- total_amount decimal(12,2) 总金额（新增计算字段）\n"
            "- order_date date 订单日期\n"
            "- status varchar(16) 订单状态\n"
            "- region varchar(32) 地区编码\n"
            "- region_name varchar(64) 地区名称（关联维表）\n"
            "- area varchar(32) 大区（关联维表）"
        ):
            return

        # ── 步骤3b: 确认建表 SQL ──
        if not run_step(ws, "3b", "确认建表",
            "确认建表"
        ):
            return

        # ── 步骤4: 建立字段映射 ──
        # 模仿 PRD 的维表关联场景
        if not run_step(ws, 4, "建立字段映射",
            "字段映射规则如下：\n"
            "- order_no, customer_name, product, quantity, unit_price, order_date, status, region "
            "直接取基表 source_db.orders 对应字段\n"
            "- total_amount = quantity * unit_price\n"
            "- region_name 和 area 通过关联维表 source_db.region_mapping 获取，"
            "关联条件: orders.region = region_mapping.region_code"
        ):
            return

        # ── 步骤4b: 确认执行映射 SQL ──
        if not run_step(ws, "4b", "确认执行映射SQL",
            "确认执行"
        ):
            return

        # ── 步骤5: 数据检查 ──
        if not run_step(ws, 5, "数据检查",
            "开始验证"
        ):
            return

        # ── 步骤6: 异常溯源 ──
        if not run_step(ws, 6, "异常溯源",
            "数据质量报告显示有空值和异常值，请溯源分析：\n"
            "1. 哪些源表记录导致了目标表的 NULL 值？\n"
            "2. quantity 出现负数是什么原因？\n"
            "3. region_name 和 area 为 NULL 是因为维表缺失映射吗？"
        ):
            return

    finally:
        ws.close()
        print("\n" + "=" * 70)
        print("  连接已关闭，测试结束")
        print("=" * 70)


if __name__ == "__main__":
    main()
