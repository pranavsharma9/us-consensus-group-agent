from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-4o-mini")
    openai_embedding_model: str = Field(default="text-embedding-3-small")

    snowflake_account: str = Field(default="")
    snowflake_user: str = Field(default="")
    snowflake_password: str = Field(default="")
    snowflake_warehouse: str = Field(default="")
    snowflake_database: str = Field(default="")
    snowflake_schema: str = Field(default="")
    snowflake_role: Optional[str] = Field(default=None)

    log_level: str = Field(default="INFO")
    max_attempts: int = Field(default=5)
    max_agent_steps: int = Field(default=10)
    few_shot_top_k: int = Field(default=2)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
