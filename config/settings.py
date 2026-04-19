# config/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict  # ← v2 import
from pydantic import Field

class Settings(BaseSettings):
    # keep these names to match your env vars/.env
    GOOGLE_SCOPES: list[str] = Field(default=["https://www.googleapis.com/auth/spreadsheets"])
    GOOGLE_CREDENTIALS_FILE: str = Field(...)
    MOVE_DB_PATH: str = Field(...)
    SHEETS_RETRIES: int = 5
    SHEETS_BACKOFF_BASE_SEC: float = 0.6

    # pydantic-settings config (v2)
    model_config = SettingsConfigDict(
        env_file=".env",           # load from .env if present
        env_file_encoding="utf-8",
        extra="ignore",            # ignore unknown env vars
    )

settings = Settings()