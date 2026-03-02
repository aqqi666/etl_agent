from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    llm_base_url: str = "https://api.openai.com/v1"
    llm_api_key: str = "sk-xxx"
    llm_model: str = "gpt-4o"

    # 各节点可单独指定模型，不设则用 llm_model
    planner_model: str = ""
    executor_model: str = ""
    observer_model: str = ""
    replanner_model: str = ""

    # MOI (MatrixOne Intelligence) 配置
    moi_key: str = ""
    moi_base_url: str = ""  # 如 http://127.0.0.1:8000 或 https://xxx.matrixone.tech

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
