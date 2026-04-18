from __future__ import annotations

from app.vnext.live.timebands import classify_time_band


def test_timebands_are_classified_cleanly() -> None:
    assert classify_time_band(8) == "EARLY"
    assert classify_time_band(45) == "MID"
    assert classify_time_band(82) == "LATE"
