import os
from functools import lru_cache
from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    airflow_api_url: HttpUrl = Field("http://airflow-webserver:8080/api/v1")
    airflow_api_user: str = Field("airflow")
    airflow_api_pass: str = Field("airflow")
    shared_input_folder: str = Field("/shared/input")
    api_timeout: int = Field(30, ge=1)
    poll_interval_seconds: int = Field(5, ge=1)
    max_poll_attempts: int = Field(120, ge=1)
    log_level: str = Field("INFO")

    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
    )


@lru_cache()
def get_settings() -> Settings:
    """
    Cached settings loader to avoid re-parsing environment variables across requests.
    """
    return Settings()


def ensure_shared_input_exists(path: str) -> None:
    """
    Ensure the shared input folder exists to surface configuration issues early.
    """
    os.makedirs(path, exist_ok=True)
