from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    llm_base_url: str = "https://api.openai.com/v1"
    llm_api_key: str = "sk-xxx"
    llm_model: str = "gpt-4o"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
