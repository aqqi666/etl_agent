# 智能 ETL Agent

基于 LangGraph + FastAPI + WebSocket 的智能 ETL 数据加工助手，通过自然语言对话完成数据库连接、选表、建表、字段映射、数据检查全流程。

## 环境要求

- Python >= 3.11
- MySQL 数据库（本地 Docker 或远程）

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

### 2. 配置环境变量

复制 `.env` 文件并填写 LLM API 信息：

```bash
cp .env.example .env
```

`.env` 内容：

```
LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1/
LLM_API_KEY=你的API密钥
LLM_MODEL=qwen3-max
```

### 3. 准备测试数据库

需要一个 MySQL 实例（默认 `127.0.0.1:3307`），运行初始化脚本创建测试数据：

```bash
uv run python init_test_db.py
```

该脚本会创建 `source_db`（含 `orders` 和 `region_mapping` 表）和空的 `test_db`。

### 4. 启动服务

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8001
```

服务启动后 WebSocket 端点为 `ws://localhost:8001/ws/{session_id}`。

### 5. 运行端到端测试

另开终端：

```bash
uv run python test_e2e.py
```

测试模拟 PRD 完整6步流程：连接 → 选表 → 建表 → 确认 → 映射 → 确认 → 数据检查。

## 项目结构

```
├── main.py                 # FastAPI 入口
├── test_e2e.py             # 端到端测试
├── init_test_db.py         # 测试数据库初始化
├── app/
│   ├── config.py           # 配置管理
│   ├── logging_config.py   # 日志配置
│   ├── agent/
│   │   ├── graph.py        # LangGraph 流程图（含 checkpointer）
│   │   ├── nodes.py        # 4个节点：planner / executor / observer / replanner
│   │   ├── state.py        # ETLState 状态定义
│   │   ├── schemas.py      # Pydantic 结构化输出模型
│   │   └── system_prompt.py
│   ├── api/
│   │   └── websocket.py    # WebSocket 端点
│   ├── tools/              # 9个数据库操作工具
│   │   ├── connection.py   # test_connection
│   │   ├── inspection.py   # list_databases / list_tables / describe_table / get_column_details
│   │   ├── query.py        # execute_query / preview_data
│   │   ├── mutation.py     # execute_sql
│   │   └── quality.py      # check_data_quality
│   └── db/
│       └── executor.py     # SQLAlchemy 连接池 + SQL 执行
└── logs/                   # 运行日志（按天轮转）
```

## WebSocket 协议

**请求：**

```json
{"type": "chat", "content": "用户消息"}
```

**响应：**

| type | 说明 |
|------|------|
| `response` | Agent 回复（观察结果 + 引导问题，合为一条消息） |
| `step_progress` | 步骤进度 `{current_step, total_steps, title}` |
| `done` | 本轮对话完成 |
| `error` | 错误信息 |

## ETL 流程（6步）

```
1. 连接数据库  →  用户提供连接串，Agent 测试连接
2. 选择基表    →  用户指定表名，Agent 展示结构和样例数据
3. 定义目标表  →  用户描述需求，Agent 生成 CREATE TABLE SQL → 用户确认后执行
4. 字段映射    →  用户描述映射逻辑，Agent 生成 INSERT INTO ... SELECT SQL → 用户确认后执行
5. 数据检查    →  自动检查目标表空值率、行数等
6. 异常溯源    →  发现异常时追溯到源表数据
```

所有修改数据库的操作（DDL/DML）需用户确认后才执行。
