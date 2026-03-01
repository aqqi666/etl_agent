from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from app.agent.state import ETLStep


class ETLPlan(BaseModel):
    """Planner 输出：ETL 执行计划"""

    steps: list[ETLStep] = Field(description="ETL 计划的步骤列表，每个步骤包含序号、标题、描述和状态")
    reasoning: str = Field(default="", description="生成此计划的推理过程")


class StepObservation(BaseModel):
    """Observer 输出：对工具执行结果的结构化分析"""

    summary: str = Field(description="一句话总结本步骤的执行结果，用于日志和历史记录")
    display_text: str = Field(description="展示给用户的 Markdown 内容，包含 SQL 代码块和数据表格")
    sql_executed: str | None = Field(default=None, description="本步骤执行的 SQL 语句")
    result_display: str | None = Field(default=None, description="SQL 执行结果的 Markdown 表格")
    sql_status: str | None = Field(default=None, description="SQL 执行状态，如成功、失败、影响行数等")
    analysis: str | None = Field(default=None, description="数据异常分析，发现问题时填写")
    sql_explanation: str | None = Field(default=None, description="SQL 逻辑的分段解释，用于映射 SQL 场景")
    next_step_hint: str = Field(default="", description="给 replanner 的内部信号，提示下一步应该做什么")
    missing_info: list[str] | None = Field(default=None, description="需要用户补充的信息列表")
    artifacts_update: dict = Field(default_factory=dict, description="需要更新到 artifacts 中的字段键值对")

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
    """Replanner 输出：决定下一步行动"""

    action: Literal["ask_user", "respond", "replan"] = Field(description="下一步行动：ask_user 向用户提问，respond 结束并回复，replan 重新规划")
    step_complete: bool = Field(default=True, description="当 action 为 ask_user 时，标记当前步骤是否已完成。true 表示完成并推进到下一步，false 表示等待用户确认后继续当前步骤")
    updated_plan: list[ETLStep] | None = Field(default=None, description="当 action 为 replan 时，提供更新后的计划步骤列表")
    response: str | None = Field(default=None, description="当 action 为 respond 时，最终回复给用户的内容")
    question: str | None = Field(default=None, description="当 action 为 ask_user 时，向用户提出的问题")
    reasoning: str = Field(default="", description="做出此决策的推理过程")
