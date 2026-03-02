# 字段级血缘图谱 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在 ETL 流程末尾新增 Step 7，自动生成字段级数据血缘的 Mermaid 图谱并展示给用户。

**Architecture:** 新增 `generate_lineage` 工具，接收 executor 传入的结构化节点和边数据，生成 Mermaid `graph LR` 语法，通过现有的 `make_rich_result` + render 流程展示。executor 负责分析 `artifacts.field_mapping_sql` 构造血缘数据。

**Tech Stack:** Python、LangChain tools、Mermaid 语法、现有 rich_result 框架

---

### Task 1: 创建 `generate_lineage` 工具

**Files:**
- Create: `app/tools/lineage.py`

**Step 1: 创建工具文件**

创建 `app/tools/lineage.py`，内容如下：

```python
import logging
from collections import defaultdict

from langchain_core.tools import tool

from app.tools.rich_result import make_rich_result

logger = logging.getLogger(__name__)


@tool
def generate_lineage(nodes: list[dict], edges: list[dict], title: str = "") -> str:
    """根据字段级血缘的节点和边生成 Mermaid 图谱。

    Args:
        nodes: 节点列表，每个节点包含:
            - id: 唯一标识，如 "s1"
            - table: 所属表全名，如 "source_db.orders"
            - column: 字段名，如 "quantity"
        edges: 边列表，每条边包含:
            - from: 源节点 id
            - to: 目标节点 id
            - label: 可选，转换说明，如 "× unit_price" 或 "JOIN region_code"
        title: 图谱标题（可选）
    """
    logger.info("[tool:generate_lineage] 生成血缘图谱，%d 节点, %d 边", len(nodes), len(edges))

    # 按 table 分组
    groups: dict[str, list[dict]] = defaultdict(list)
    for node in nodes:
        groups[node["table"]].append(node)

    # 生成 Mermaid
    lines = ["graph LR"]
    for table_name, table_nodes in groups.items():
        lines.append(f"  subgraph {table_name}")
        for n in table_nodes:
            lines.append(f"    {n['id']}[{n['column']}]")
        lines.append("  end")

    for edge in edges:
        src = edge["from"]
        tgt = edge["to"]
        label = edge.get("label", "")
        if label:
            lines.append(f'  {src} -->|"{label}"| {tgt}')
        else:
            lines.append(f"  {src} --> {tgt}")

    mermaid_code = "\n".join(lines)

    return make_rich_result(
        tool_name="generate_lineage",
        result_type="lineage",
        title=title or "字段级数据血缘图谱",
        text=mermaid_code,
        summary=f"已生成血缘图谱，包含 {len(nodes)} 个字段节点和 {len(edges)} 条血缘关系",
        metadata={"mermaid": mermaid_code},
    )
```

**Step 2: 验证模块可导入**

Run: `cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "from app.tools.lineage import generate_lineage; print('OK:', generate_lineage.name)"`
Expected: `OK: generate_lineage`

**Step 3: Commit**

```bash
git add app/tools/lineage.py
git commit -m "feat: add generate_lineage tool for Mermaid lineage graph"
```

---

### Task 2: 注册工具到 ALL_TOOLS

**Files:**
- Modify: `app/tools/__init__.py`

**Step 1: 添加 import 和注册**

在 `app/tools/__init__.py` 中：

1. 在第 5 行 `from app.tools.quality import check_data_quality` 之后添加：
```python
from app.tools.lineage import generate_lineage
```

2. 在 `ALL_TOOLS` 列表中 `check_data_quality` 和 `render` 之间添加 `generate_lineage`：
```python
ALL_TOOLS = [
    test_connection,
    list_databases,
    list_tables,
    describe_table,
    get_column_details,
    execute_query,
    preview_data,
    execute_sql,
    check_data_quality,
    generate_lineage,
    render,
]
```

**Step 2: 验证工具列表**

Run: `cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "from app.tools import ALL_TOOLS; print([t.name for t in ALL_TOOLS])"`
Expected: 列表中包含 `'generate_lineage'`，共 11 个工具

**Step 3: Commit**

```bash
git add app/tools/__init__.py
git commit -m "feat: register generate_lineage in ALL_TOOLS"
```

---

### Task 3: 添加 lineage 类型的 Markdown 渲染

**Files:**
- Modify: `app/agent/nodes.py:71-89`

**Step 1: 在 `_format_payload_to_markdown` 中增加 lineage 渲染**

在 `app/agent/nodes.py` 的 `_format_payload_to_markdown` 函数中，找到第 88 行（`quality_report` 处理块的最后一行，`sections.append(header + ...)`）后面，在 `return "\n".join(sections)` 之前，添加 lineage 渲染逻辑：

```python
    # lineage 血缘图谱（Mermaid）
    if payload.get("result_type") == "lineage":
        mermaid_code = metadata.get("mermaid", "")
        if mermaid_code:
            sections.append(f"```mermaid\n{mermaid_code}\n```\n")
```

即最终 `_format_payload_to_markdown` 函数末尾为：

```python
            sections.append(header + "\n" + separator + "\n" + "\n".join(data_lines) + "\n")

    # lineage 血缘图谱（Mermaid）
    if payload.get("result_type") == "lineage":
        mermaid_code = metadata.get("mermaid", "")
        if mermaid_code:
            sections.append(f"```mermaid\n{mermaid_code}\n```\n")

    return "\n".join(sections)
```

**Step 2: 验证渲染逻辑**

Run:
```bash
cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "
from app.agent.nodes import _format_payload_to_markdown
result = _format_payload_to_markdown({
    'title': '测试血缘',
    'result_type': 'lineage',
    'metadata': {'mermaid': 'graph LR\n  A --> B'}
})
assert '\`\`\`mermaid' in result
assert 'A --> B' in result
print('OK')
print(result)
"
```
Expected: 输出 `OK` 和包含 mermaid 代码块的 Markdown

**Step 3: Commit**

```bash
git add app/agent/nodes.py
git commit -m "feat: add Mermaid rendering for lineage result type"
```

---

### Task 4: 在 DEFAULT_PLAN 中增加 Step 7

**Files:**
- Modify: `app/agent/graph.py:22-29`

**Step 1: 添加 Step 7**

在 `app/agent/graph.py` 中，将 `DEFAULT_PLAN` 从 6 步扩展为 7 步。在第 28 行 `ETLStep(index=6, ...)` 之后添加：

```python
    ETLStep(index=7, title="生成血缘图谱", description="根据字段映射 SQL 分析字段级数据血缘关系，调用 generate_lineage 工具生成 Mermaid 图谱，通过 render 展示"),
```

最终 `DEFAULT_PLAN`：

```python
DEFAULT_PLAN = [
    ETLStep(index=1, title="连接数据库", description="验证用户提供的数据库连接串，建立连接"),
    ETLStep(index=2, title="选择基表", description="查看用户指定的源表结构和样例数据"),
    ETLStep(index=3, title="定义目标表结构", description="根据用户需求生成 CREATE TABLE SQL，等待用户确认后执行"),
    ETLStep(index=4, title="建立字段映射", description="根据用户描述的映射规则生成 INSERT INTO ... SELECT SQL，等待用户确认后执行"),
    ETLStep(index=5, title="数据质量检查", description="检查目标表的数据质量（行数、空值率等）"),
    ETLStep(index=6, title="异常溯源", description="如发现数据异常，追溯到源表分析原因"),
    ETLStep(index=7, title="生成血缘图谱", description="根据字段映射 SQL 分析字段级数据血缘关系，调用 generate_lineage 工具生成 Mermaid 图谱，通过 render 展示"),
]
```

**Step 2: 验证计划加载**

Run: `cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "from app.agent.graph import DEFAULT_PLAN; print(f'{len(DEFAULT_PLAN)} 步'); print(DEFAULT_PLAN[-1].title)"`
Expected: `7 步` 和 `生成血缘图谱`

**Step 3: Commit**

```bash
git add app/agent/graph.py
git commit -m "feat: add Step 7 lineage graph to DEFAULT_PLAN"
```

---

### Task 5: 更新 E2E 测试

**Files:**
- Modify: `test_e2e.py:130-137`

**Step 1: 在异常溯源步骤后添加 Step 7 测试**

在 `test_e2e.py` 中，找到步骤 6（异常溯源）的 `return` 之后、`finally:` 之前，添加：

```python
        # ── 步骤7: 生成血缘图谱 ──
        if not run_step(ws, 7, "生成血缘图谱",
            "请生成字段级血缘图谱"
        ):
            return
```

**Step 2: 验证测试脚本语法**

Run: `cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "import ast; ast.parse(open('test_e2e.py').read()); print('语法OK')"`
Expected: `语法OK`

**Step 3: Commit**

```bash
git add test_e2e.py
git commit -m "feat: add Step 7 lineage graph to E2E test"
```

---

### Task 6: 集成验证

**Step 1: 验证完整模块导入链**

Run:
```bash
cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "
from app.agent.graph import DEFAULT_PLAN, etl_graph
from app.tools import ALL_TOOLS
from app.tools.lineage import generate_lineage

print(f'计划: {len(DEFAULT_PLAN)} 步')
print(f'工具: {len(ALL_TOOLS)} 个')
print(f'Step 7: {DEFAULT_PLAN[-1].title}')
print(f'generate_lineage in ALL_TOOLS: {generate_lineage in ALL_TOOLS}')
print('集成验证通过')
"
```
Expected: `计划: 7 步`、`工具: 11 个`、`Step 7: 生成血缘图谱`、`generate_lineage in ALL_TOOLS: True`、`集成验证通过`

**Step 2: 验证工具独立调用**

Run:
```bash
cd /Users/zhangqq/Documents/pythonProject/hackathon && python -c "
import json
from app.tools.lineage import generate_lineage

result = generate_lineage.invoke({
    'nodes': [
        {'id': 's1', 'table': 'source_db.orders', 'column': 'quantity'},
        {'id': 's2', 'table': 'source_db.orders', 'column': 'unit_price'},
        {'id': 't1', 'table': 'test_db.order_summary', 'column': 'total_amount'},
    ],
    'edges': [
        {'from': 's1', 'to': 't1', 'label': '× unit_price'},
        {'from': 's2', 'to': 't1', 'label': '× quantity'},
    ],
    'title': '测试血缘图谱',
})
parsed = json.loads(result)
assert parsed['__structured__'] == True
assert 'mermaid' in parsed['payload']['metadata']
mermaid = parsed['payload']['metadata']['mermaid']
assert 'subgraph source_db.orders' in mermaid
assert 'subgraph test_db.order_summary' in mermaid
assert '× unit_price' in mermaid
print('工具调用验证通过')
print(mermaid)
"
```
Expected: 输出 `工具调用验证通过` 和完整的 Mermaid 图谱代码

**Step 3: Commit all（如果前面分步骤有遗漏）**

确认所有改动已提交，运行 `git status` 确认工作区干净。
