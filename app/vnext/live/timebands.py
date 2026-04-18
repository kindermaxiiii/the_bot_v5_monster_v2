from __future__ import annotations

from app.vnext.live.models import TimeBand


def classify_time_band(minute: int) -> TimeBand:
    if minute <= 25:
        return "EARLY"
    if minute <= 70:
        return "MID"
    return "LATE"
