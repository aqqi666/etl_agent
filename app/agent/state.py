from __future__ import annotations

import operator
from typing import Annotated, Literal, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field


class ETLStep(BaseModel):
    """ETL 计划中的一个步骤"""

    index: int = Field(description="步骤序号，从 1 开始")
    title: str = Field(description="步骤标题，简明扼要")
    description: str = Field(description="步骤的具体操作描述")
    status: Literal["pending", "in_progress", "completed", "skipped"] = Field(default="pending", description="步骤状态")


class ETLArtifacts(BaseModel):
    """累积的 ETL 工作产物，每步 tool 执行后更新"""

    connection_string: str | None = None
    source_db: str | None = None
    source_table: str | None = None
    source_schema: str | None = None
    source_sample: str | None = None
    target_db: str | None = None
    target_table: str | None = None
    target_ddl: str | None = None
    target_created: bool = False
    field_mapping_rules: list[str] = Field(default_factory=list)
    field_mapping_sql: str | None = None
    mapping_executed: bool = False
    quality_report: str | None = None
    decisions: list[str] = Field(default_factory=list)
    context_summary: str | None = None


class ETLState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    plan: list[ETLStep]
    current_step: int
    past_steps: Annotated[list[tuple[str, str]], operator.add]
    artifacts: ETLArtifacts
    response: str | None
    render_cache: dict  # 跨 ReAct 周期缓存结构化工具结果，供 render 工具使用
    rendered_content: str | None  # render 工具格式化后的 Markdown，由 analyzer 合并到最终 response
