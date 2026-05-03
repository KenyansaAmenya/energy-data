from datetime import datetime, timezone
from typing import Optional, Union

from dateutil import parser


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(date_string: str) -> Optional[datetime]:
    try:
        return parser.isoparse(date_string)
    except (ValueError, TypeError):
        return None


def format_iso(dt: datetime) -> str:
    return dt.isoformat()


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def generate_batch_id() -> str:
    return f"batch_{utc_now().strftime('%Y%m%d_%H%M%S')}_{utc_now().microsecond}"