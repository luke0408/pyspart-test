from datetime import date, datetime
from zoneinfo import ZoneInfo

from app.core.settings import get_settings


def get_timezone_name() -> str:
    return get_settings().SPARK_TIMEZONE


def now_in_timezone(timezone_name: str) -> datetime:
    return datetime.now(tz=ZoneInfo(timezone_name))


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)
