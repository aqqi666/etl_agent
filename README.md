# 智能 ETL Agent

基于 LangGraph + FastAPI + WebSocket 的智能 ETL 数据加工助手，通过自然语言对话完成数据库连接、选表、建表、字段映射、数据质量检查、异常溯源、血缘图谱生成全流程。

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

需要一个 MySQL 实例（默认 `127.0.0.1:3307`），可用 Docker 快速启动：

```bash
docker run -d --name etl-mysql -p 3307:3306 -e MYSQL_ROOT_PASSWORD=test123 mysql:8
```

运行初始化脚本创建测试数据：

```bash
uv run python init_test_db.py
```

该脚本会创建 `source_db`（含 `orders` 和 `region_mapping` 表，包含异常测试数据）和空的 `test_db`。

### 4. 启动服务

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8001
```

### 5. 打开前端

浏览器访问 http://localhost:8001/ ，即可通过聊天界面与 ETL Agent 交互。

### 6. 运行端到端测试

另开终端：

```bash
uv run python test_e2e.py
```

## 功能特性

- 对话式 ETL：通过自然语言描述需求，Agent 自动生成并执行 SQL
- 7 步标准流程：连接 → 选表 → 建表 → 映射 → 质量检查 → 异常溯源 → 血缘图谱
- 人工确认机制：所有 DDL/DML 操作需用户确认后才执行
- 数据质量检查：自动检测空值率、数值字段极值（MIN/MAX/AVG）
- 异常溯源分析：发现数据异常时追溯到源表分析根因
- 字段级血缘图谱：自动生成 Mermaid 可视化图谱，展示字段间的映射和转换关系
- 前端聊天界面：支持 Markdown 渲染、SQL 语法高亮、Mermaid 图谱展示
- 快速确认通道：识别确认关键词（"确认"、"执行"等）跳过 LLM 调用直接执行

## ETL 流程（7 步）

```
1. 连接数据库  →  用户提供连接串，Agent 测试连接
2. 选择基表    →  用户指定表名，Agent 展示结构和样例数据
3. 定义目标表  →  用户描述需求，Agent 生成 CREATE TABLE SQL → 用户确认后执行
4. 字段映射    →  用户描述映射逻辑，Agent 生成 INSERT INTO ... SELECT SQL → 用户确认后执行
5. 数据检查    →  自动检查目标表空值率、行数、数值极值
6. 异常溯源    →  发现异常时追溯到源表分析原因
7. 血缘图谱    →  生成字段级 Mermaid 血缘图谱
```

## 项目结构

```
├── main.py                 # FastAPI 入口
├── test_e2e.py             # 端到端测试
├── init_test_db.py         # 测试数据库初始化
├── static/
│   └── index.html          # 前端聊天界面（单文件）
├── app/
│   ├── config.py           # 配置管理
│   ├── logging_config.py   # 日志配置
│   ├── agent/
│   │   ├── graph.py        # LangGraph 流程图（7步默认计划）
│   │   ├── nodes.py        # 节点：init_plan / executor / parallel_tool_node / analyzer
│   │   ├── state.py        # ETLState 状态定义
│   │   ├── schemas.py      # Pydantic 结构化输出模型
│   │   └── system_prompt.py# executor 和 analyzer 提示词
│   ├── api/
│   │   └── websocket.py    # WebSocket 端点 + 快速确认通道
│   ├── tools/              # 11 个工具
│   │   ├── connection.py   # test_connection
│   │   ├── inspection.py   # list_databases / list_tables / describe_table / get_column_details
│   │   ├── query.py        # execute_query / preview_data
│   │   ├── mutation.py     # execute_sql
│   │   ├── quality.py      # check_data_quality
│   │   ├── lineage.py      # generate_lineage（Mermaid 血缘图谱）
│   │   ├── render.py       # render（结构化结果展示）
│   │   └── rich_result.py  # 结构化结果辅助函数
│   └── db/
│       └── executor.py     # SQLAlchemy 连接池 + SQL 执行
├── docs/plans/             # 设计文档
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
| `response` | Agent 回复（Markdown 格式，含表格、SQL、Mermaid 图谱等） |
| `step_progress` | 步骤进度 `{current_step, total_steps, title}` |
| `done` | 本轮对话完成 |
| `error` | 错误信息 |

## 技术栈

- **后端**：FastAPI + LangGraph + LangChain + SQLAlchemy
- **LLM**：兼容 OpenAI API 的模型（通义千问、GPT-4o 等）
- **数据库**：MySQL / MatrixOne
- **前端**：单 HTML 文件 + marked.js + mermaid.js + highlight.js
