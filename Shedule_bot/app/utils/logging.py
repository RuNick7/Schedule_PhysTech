# app/utils/logging.py
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
try:
    import zoneinfo
except Exception:
    zoneinfo = None  # py<3.9 fallback, но у нас 3.10+

_DEF_FMT = "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
_DEF_DATEFMT = "%Y-%m-%d %H:%M:%S.%f"  # мс покажем через срез

class TZFormatter(logging.Formatter):
    """Форматтер с поддержкой IANA-таймзоны."""
    def __init__(self, fmt=_DEF_FMT, datefmt=_DEF_DATEFMT, tz_name: str | None = None):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._tz = zoneinfo.ZoneInfo(tz_name) if (tz_name and zoneinfo) else None

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, self._tz) if self._tz else datetime.fromtimestamp(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            s = dt.strftime(_DEF_DATEFMT)
        # усечём микросекунды до миллисекунд
        if "%f" in (datefmt or _DEF_DATEFMT):
            s = s[:-3]
        return s

def setup_logging():
    """
    Инициализация логирования:
    - уровень из LOG_LEVEL (по умолчанию INFO)
    - таймзона из TIMEZONE (если не задана — системная)
    - опционально лог в файл с ротацией: LOG_FILE (например, ./app/data/app.log)
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    tz_name = os.getenv("TIMEZONE")  # можно задать, например, Europe/Moscow
    log_file = os.getenv("LOG_FILE")  # если задан — включим ротацию

    root = logging.getLogger()
    # избегаем повторных хендлеров при повторном вызове
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    root.setLevel(level)

    fmt = TZFormatter(tz_name=tz_name)

    # Console
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    # File (optional, с ротацией ~5 МБ * 3 бэкапа)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_h = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        file_h.setFormatter(fmt)
        root.addHandler(file_h)

    # Подкрутим «шумные» логгеры
    logging.getLogger("aiogram").setLevel(level)
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Удобный фабричный метод для модулей."""
    return logging.getLogger(name)
