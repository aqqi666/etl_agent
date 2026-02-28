from __future__ import annotations

import json

from pydantic import BaseModel, Field, field_validator

from app.agent.state import ETLStep


class ETLPlan(BaseModel):
    """Planner 输出：ETL 计划"""

    steps: list[ETLStep]
    reasoning: str


class StepObservation(BaseModel):
    """Observer 输出：对 tool 结果的分析"""

    summary: str
    display_text: str  # 模型生成的用户可见 Markdown 内容
    sql_executed: str | None = None
    result_display: str | None = None
    sql_status: str | None = None
    analysis: str | None = None
    sql_explanation: str | None = None
    next_step_hint: str
    missing_info: list[str] | None = None
    artifacts_update: dict = Field(default_factory=dict)

    @field_validator("missing_info", mode="before")
    @classmethod
    def parse_missing_info(cls, v):
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
            return [v]
        return v

    @field_validator("artifacts_update", mode="before")
    @classmethod
    def parse_artifacts_update(cls, v):
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    return parsed
            except (json.JSONDecodeError, TypeError):
                pass
            return {}
        return v


class ReplanDecision(BaseModel):
    """Replanner 输出：继续/调整/完成"""

    action: str  # "continue" | "replan" | "respond" | "ask_user"
    step_complete: bool = True  # ask_user 时：当前步骤是否已完成
    updated_plan: list[ETLStep] | None = None
    response: str | None = None
    question: str | None = None
    reasoning: str
