from __future__ import annotations

from datetime import date, datetime
import json
from typing import Any


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(v) for v in value]
    try:
        return str(value)
    except Exception:
        return None


def dumps_json_safe(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(json_safe(value), ensure_ascii=False, separators=(",", ":"))
