# 智能 ETL Agent 后端实现计划（v2 — 工程化改进版）

## Context

基于 PRD `~/downloads/etl_prd.pdf`，构建通过自然语言对话完成数据库 ETL 任务的智能 Agent 后端。
v2 参考 LangGraph Plan-and-Execute 模板，做三项工程化改进：
- 自定义 StateGraph 实现 Plan-Execute-Replan
- Pydantic 结构化输出
- ETLArtifacts 状态追踪

**技术选型不变**：Python + FastAPI + WebSocket + LangGraph + OpenAI 兼容协议

---

## 项目结构（相对 v1 变更）

```
hackathon/
├── main.py                       # FastAPI 入口（不变）
├── requirements.txt              # （不变）
├── .env.example                  # （不变）
├── app/
│   ├── config.py                 # 配置（不变）
│   ├── api/
│   │   └── websocket.py          # WebSocket 端点（改：适配新 graph）
│   ├── agent/
│   │   ├── graph.py              # ★ 重写：自定义 StateGraph
│   │   ├── state.py              # ★ 新增：ETLState + ETLArtifacts 定义
│   │   ├── nodes.py              # ★ 新增：planner/executor/observer/replanner/respond 节点
│   │   ├── schemas.py            # ★ 新增：Pydantic 结构化输出模型
│   │   └── system_prompt.py      # ★ 重写：引导策略 + 输出规范
│   ├── tools/                    # （全部保留，不变）
│   │   ├── __init__.py
│   │   ├── connection.py
│   │   ├── inspection.py
│   │   ├── query.py
│   │   ├── mutation.py
│   │   └── quality.py
│   └── db/
│       └── executor.py           # （不变）
```

---

## Step 1: 定义 State 和 Artifacts — `agent/state.py`

```python
from typing import Annotated, TypedDict
from pydantic import BaseModel, Field
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
import operator

class ETLStep(BaseModel):
    """ETL 计划中的一个步骤"""
    index: int
    title: str                    # 如 "连接数据库"
    description: str              # 如 "验证用户提供的 MySQL 连接串"
    status: str = "pending"       # pending / in_progress / completed / skipped

class ETLArtifacts(BaseModel):
    """累积的 ETL 工作产物，每步 tool 执行后更新"""
    connection_string: str | None = None
    source_db: str | None = None
    source_table: str | None = None
    source_schema: str | None = None       # Markdown 表格形式
    source_sample: str | None = None       # 前 10 行样例
    target_db: str | None = None
    target_table: str | None = None
    target_ddl: str | None = None
    target_created: bool = False
    field_mapping_sql: str | None = None
    mapping_executed: bool = False
    quality_report: str | None = None
    decisions: list[str] = Field(default_factory=list)
    context_summary: str | None = None         # 涉及的维表、JOIN 关系等，由 observer 自由总结

class ETLState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    plan: list[ETLStep]                       # 直接存 Pydantic 对象，避免反复序列化
    current_step: int                         # 当前步骤索引
    past_steps: Annotated[list[tuple[str, str]], operator.add]  # (步骤描述, 执行结果)
    artifacts: ETLArtifacts                   # 直接存 Pydantic 对象
    response: str | None                      # 最终回复（非 None 时结束流程）
```

---

## Step 2: 定义结构化输出模型 — `agent/schemas.py`

```python
from pydantic import BaseModel, Field

class ETLPlan(BaseModel):
    """Planner 输出：ETL 计划"""
    steps: list[ETLStep]
    reasoning: str               # 为什么这样规划

class ToolCall(BaseModel):
    """单次工具调用"""
    name: str
    args: dict

class StepAction(BaseModel):
    """Executor 决策：当前步骤要调用哪些 tools"""
    tool_calls: list[ToolCall]   # [ToolCall(name="describe_table", args={...})]
    reasoning: str

class StepObservation(BaseModel):
    """Observer 输出：对 tool 结果的分析"""
    summary: str                 # "已查询表 xxx 的结构"
    sql_executed: str            # 执行的 SQL
    result_display: str          # Markdown 表格
    sql_status: str              # "执行成功，返回 12 行"
    analysis: str | None = None  # 异常分析
    sql_explanation: str | None = None  # 映射 SQL 的分段解释（公司代码清洗、利润中心清洗、其他字段等）
    next_step_hint: str          # "接下来请描述目标表结构"
    missing_info: list[str] | None = None  # 需要用户补充的信息
    artifacts_update: dict = Field(default_factory=dict)  # 要更新到 artifacts 的字段

class ReplanDecision(BaseModel):
    """Replanner 输出：继续/调整/完成"""
    action: str                  # "continue" | "replan" | "respond" | "ask_user"
    updated_plan: list[dict] | None = None   # action=replan 时
    response: str | None = None              # action=respond 时
    question: str | None = None              # action=ask_user 时
    reasoning: str
```

---

## Step 3: 构建 StateGraph — `agent/graph.py`

```
Graph 结构:

            ┌──────────┐
            │  START    │
            └────┬─────┘
                 │
            ┌────▼─────┐
            │ planner   │  ← 生成/更新 ETL 计划 (structured output)
            └────┬─────┘
                 │
            ┌────▼─────┐
            │ executor  │  ← 执行当前步骤的 tools (ReAct 子循环)
            └────┬─────┘
                 │
            ┌────▼──────┐
            │ observer   │  ← 分析结果，生成结构化输出，更新 artifacts
            └────┬──────┘
                 │
            ┌────▼──────┐
         ┌──│ replanner  │──┐
         │  └────────────┘  │
         │                  │
    (continue)          (respond/ask_user)
         │                  │
         ▼                  ▼
    [executor]           [END]
```

**关键节点实现逻辑** (`agent/nodes.py`):

### planner 节点
- 输入：用户消息 + 当前 artifacts
- 用 `llm.with_structured_output(ETLPlan)` 生成计划
- 首次调用：生成完整 6 步计划
- 非首次（replanning）：基于 past_steps 和 artifacts 调整剩余计划
- 输出：`{"plan": steps, "current_step": 0}`

### executor 节点
- 读取 `plan[current_step]`
- 使用 LangGraph 内置的 ReAct 机制实现 tool 循环：executor 节点绑定所有 tools，通过 `tools_condition` 条件边自动循环（LLM 返回 tool_call → 执行 tool → 结果回传 LLM → 直到 LLM 不再调用 tool），无需手写循环逻辑
- 特殊处理：execute_sql 等危险操作前，LLM 先向用户展示待执行 SQL 并询问确认，用户通过普通文本回复（如"确认"、"好的"、"执行吧"）后再实际执行
- 输出：`{"past_steps": [(step_title, tool_results)], "messages": [...]}`

### observer 节点
- 输入：executor 的 tool 结果 + 当前 artifacts
- 用 `llm.with_structured_output(StepObservation)` 分析结果
- 将 StepObservation 渲染为 Markdown 流式推送给前端
- 更新 artifacts（如设置 source_table、target_ddl 等）
- 输出：`{"artifacts": updated_artifacts}`

### replanner 节点
- 输入：plan + past_steps + artifacts + 用户最新消息
- 用 `llm.with_structured_output(ReplanDecision)` 做决策
- 4 种决策：
  - `continue`：前进到下一步 → 回到 executor
  - `replan`：修改剩余计划 → 回到 executor
  - `respond`：任务完成或需要总结 → 生成最终回复 → END
  - `ask_user`：信息不足 → 生成问题 → END（等待用户回复后重新进入 planner）
- 输出：`{"plan": updated, "current_step": next, "response": ...}`

### 条件边
```python
def route_after_replan(state: ETLState) -> str:
    if state.get("response"):
        return "end"
    return "executor"
```

---

## Step 4: 重写 System Prompt — `agent/system_prompt.py`

分为 3 个 prompt，给不同节点使用：

### PLANNER_PROMPT
```
你是 ETL 任务规划器。根据用户需求和当前已完成的工作，生成或调整 ETL 计划。

## 标准 ETL 流程（参考，不强制）
1. 连接数据库 → 2. 选择基表 → 3. 定义目标表结构 → 4. 建立字段映射 → 5. 数据检查 → 6. 异常溯源

## 规划原则
- 用户可能从任意步骤开始，根据实际情况灵活调整
- 如果用户要求修改之前的决策（如改目标表结构），在计划中插入回退步骤
- 每个步骤的 description 要具体到"做什么"，不要模糊

## 当前工作产物
{artifacts_json}
```

### EXECUTOR_PROMPT
```
你是 ETL 执行器。执行当前步骤，使用工具完成具体操作。

## 当前步骤
{current_step_description}

## 行为准则
1. 先查看相关表结构和数据，再做修改操作
2. 所有 DDL/DML 必须通过 execute_sql，执行前先展示 SQL 并用文本询问用户确认
3. SQL 兼容 MySQL / MatrixOne
4. 执行失败时分析错误、修正后重试
5. 当用户描述字段映射涉及关联表（维表）时，必须先用 describe_table 查询该维表结构并展示给用户，确认关联字段正确后再生成映射 SQL
```

### OBSERVER_PROMPT
```
你是 ETL 结果分析器。分析 tool 执行结果，生成结构化的分析报告。

## 输出要求
- summary：一句话总结做了什么
- sql_executed：本次执行的核心 SQL
- result_display：结果的 Markdown 表格
- sql_status：执行状态 + 数据摘要
- analysis：异常发现或重要信息（可选）
- next_step_hint：引导用户下一步操作
- missing_info：如果用户输入不完整，列出需要补充的信息

## 引导策略（关键）
- 当用户描述模糊时，在 missing_info 中列出需要补充的具体信息项
- 对于字段映射，如果用户只描述了部分，在 next_step_hint 中询问"还有其他字段的映射逻辑吗？"
- 不要在用户没说完时就生成最终 SQL
- 目标表创建成功后，主动对比源表和目标表字段，分析哪些字段可直接映射、哪些字段是新增需要关联维表，在 next_step_hint 中给出映射猜测建议
- 生成映射 SQL 后，在 sql_explanation 中按逻辑分段解释（如：公司代码清洗、利润中心清洗、直接映射字段、容错处理等）
```

---

## Step 5: 适配 WebSocket — `api/websocket.py`

主要变更点：
1. `agent.astream()` 改为对自定义 graph 的流式调用
2. observer 节点输出的 StepObservation 渲染为 Markdown 后通过 `text_delta` 推送
3. executor 节点的 tool_call 通过 `tool_call` 消息推送
4. replanner 的 `ask_user` 决策通过 `text_delta` 推送问题

**WebSocket 协议**：text_delta / tool_call / done / error（无 confirm_request，确认通过普通文本对话完成）

**流式处理关键**：使用 `stream_mode=["messages", "updates", "custom"]`
- `messages` 模式：转发 LLM 生成的文本 token
- `updates` 模式：检测节点状态变更
- `custom` 模式：observer 节点用 `get_stream_writer()` 推送渲染后的结构化结果

---

## Step 6: 其余文件（不变）

以下文件从 v1 计划完整保留，不做修改：
- `main.py` — FastAPI 入口
- `app/config.py` — 配置
- `app/db/executor.py` — SQL 执行器
- `app/tools/connection.py` — test_connection
- `app/tools/inspection.py` — list_databases, list_tables, describe_table, get_column_details
- `app/tools/query.py` — execute_query, preview_data
- `app/tools/mutation.py` — execute_sql（危险操作前通过对话确认）
- `app/tools/quality.py` — check_data_quality

---

## 实现优先级

| 步骤 | 内容 | 文件 |
|------|------|------|
| 1 | 项目骨架 + 配置 + 依赖 | `main.py`, `config.py`, `requirements.txt` |
| 2 | SQL 执行器 | `db/executor.py` |
| 3 | 全部 9 个 Tools | `tools/*.py` |
| 4 | State + Artifacts + Schemas 定义 | `agent/state.py`, `agent/schemas.py` |
| 5 | System Prompts（3 个） | `agent/system_prompt.py` |
| 6 | Graph 节点实现 | `agent/nodes.py` |
| 7 | StateGraph 构建 + 编译 | `agent/graph.py` |
| 8 | WebSocket 端点适配 | `api/websocket.py` |
| 9 | 端到端联调 | — |

步骤 1-3 与 v1 完全一致，步骤 4-8 是 v2 新增。

---

## 验证方案

### 基础验证（websocat）
```bash
pip install -r requirements.txt && python main.py
# 连接
websocat ws://localhost:8000/ws/test
```

### 场景 1：完整流程
```json
{"type":"chat","content":"连接数据库 mysql://..."}
// → 验证：planner 生成 6 步计划，executor 调用 test_connection，observer 返回结构化结果
{"type":"chat","content":"用 dwd_dcp.DWD_BW_ZTBPC011_02 表做加工"}
// → 验证：executor 调 describe_table + preview_data，observer 输出 SQL+表格+分析
{"type":"chat","content":"在 test_db 库创建 revenue_cost 表...（字段描述）"}
// → 验证：executor 生成 CREATE TABLE SQL，通过文本向用户展示并询问确认
{"type":"chat","content":"确认执行"}
// → 验证：建表成功，observer 引导用户描述映射逻辑
```

### 场景 2：引导补充信息
```json
{"type":"chat","content":"帮我建个目标表"}
// → 验证：observer 的 missing_info 不为空，回复列出需要补充的信息
```

### 场景 3：修改之前的决策
```json
{"type":"chat","content":"刚才的映射 SQL 不要容错处理，维表没有的就是没有"}
// → 验证：replanner 生成 replan，重新执行映射步骤，移除 COALESCE
```

### 场景 4：分批输入
```json
{"type":"chat","content":"公司代码 rbukrs 通过关联 dwd_bw_ztbpc002_com 清洗..."}
// → 验证：observer 确认已理解，next_step_hint 询问"还有其他字段的映射逻辑吗？"
{"type":"chat","content":"利润中心 prctr 通过关联 dwd_bw_ztbpc002_prc 清洗..."}
{"type":"chat","content":"就这些了"}
// → 验证：现在才生成完整的 INSERT INTO ... SELECT SQL
```
