from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from app.agent.state import ETLStep


class StepResult(BaseModel):
    """Analyzer 输出：分析工具执行结果 + 决策下一步行动（合并原 Observer 和 Replanner）"""

    # ── Observer 职责：分析结果 ──
    summary: str = Field(description="一句话总结本步骤的执行结果，用于日志和历史记录")
    display_text: str = Field(description="展示给用户的文字内容。若 render 工具已展示数据则可为空；若未调用 render（如连接测试、SQL执行），则必须包含操作结果。不包含表格和提问")
    artifacts_update: dict = Field(default_factory=dict, description="需要更新到 artifacts 中的字段键值对")

    # ── Replanner 职责：决策下一步 ──
    action: Literal["ask_user", "respond", "replan"] = Field(description="下一步行动：ask_user 向用户提问，respond 结束并回复，replan 重新规划")
    step_complete: bool = Field(default=True, description="当 action 为 ask_user 时，标记当前步骤是否已完成。true 表示完成并推进到下一步，false 表示等待用户确认后继续当前步骤")
    question: str | None = Field(default=None, description="当 action 为 ask_user 时，向用户提出的简短问题（1-2 句话）")
    response: str | None = Field(default=None, description="当 action 为 respond 时，最终回复给用户的内容")
    updated_plan: list[ETLStep] | None = Field(default=None, description="当 action 为 replan 时，提供更新后的计划步骤列表")
    reasoning: str = Field(default="", description="做出此决策的推理过程")

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
