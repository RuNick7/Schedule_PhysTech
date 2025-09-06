# app/config.py
from __future__ import annotations

from typing import Optional, List
from pathlib import Path
from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from dotenv import load_dotenv


# --- пути проекта ---
APP_DIR = Path(__file__).resolve().parent          # .../app
ROOT_DIR = APP_DIR.parent                          # корень проекта

# Явно грузим .env из корня + из текущей CWD (на всякий случай)
load_dotenv(ROOT_DIR / ".env")
load_dotenv()  # если вдруг запускаешь из корня — тоже сработает


class Settings(BaseSettings):
    """Глобальные настройки приложения. Читаются из .env/окружения."""
    model_config = SettingsConfigDict(
        case_sensitive=False,   # имена переменных нечувствительны к регистру
    )

    # Telegram
    bot_token: str = Field(alias="BOT_TOKEN")

    # Google Sheets
    spreadsheet_id: str = Field(alias="SPREADSHEET_ID")
    sheet_gid: int = Field(alias="SHEET_GID")
    google_credentials: str = Field("google-credentials.json", alias="GOOGLE_CREDENTIALS")

    # База данных
    db_path: str = Field(str(ROOT_DIR / "app" / "data" / "bot.db"), alias="DB_PATH")

    # Таймзона
    timezone: str = Field("Europe/Moscow", alias="TIMEZONE")

    # Логи
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_file: Optional[str] = Field(None, alias="LOG_FILE")

    # --- нормализация путей относительно корня проекта ---
    @field_validator("google_credentials", "db_path", "log_file", mode="before")
    @classmethod
    def _expand_path(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        p = Path(v)
        if not p.is_absolute():
            p = ROOT_DIR / p
        return str(p)

    # --- базовые проверки и дружелюбные сообщения ---
    @field_validator("bot_token")
    @classmethod
    def _validate_bot_token(cls, v: str) -> str:
        if not v or v.lower().startswith("your_telegram_bot_token"):
            raise ValueError("BOT_TOKEN не задан в .env (или задан заглушкой).")
        return v

    @field_validator("spreadsheet_id")
    @classmethod
    def _validate_spreadsheet_id(cls, v: str) -> str:
        if not v:
            raise ValueError("SPREADSHEET_ID не задан в .env.")
        return v


def _build_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as e:
        # соберём читаемый список недостающих ключей
        missing: List[str] = []
        for err in e.errors():
            if err.get("type") == "missing":
                loc = err.get("loc") or []
                if loc:
                    missing.append(str(loc[0]))
        msg = "Отсутствуют переменные окружения: " + ", ".join(missing) if missing else str(e)
        msg += "\nПроверь, что файл .env лежит в корне проекта и содержит, как минимум:\n" \
               "  BOT_TOKEN=...\n  SPREADSHEET_ID=...\n  SHEET_GID=0\n" \
               f"Текущий корень: {ROOT_DIR}"
        raise RuntimeError(msg) from e


settings = _build_settings()
