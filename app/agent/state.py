from __future__ import annotations

import operator
from typing import Annotated, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field


class ETLStep(BaseModel):
    """ETL 计划中的一个步骤"""

    index: int
    title: str
    description: str
    status: str = "pending"  # pending / in_progress / completed / skipped


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
