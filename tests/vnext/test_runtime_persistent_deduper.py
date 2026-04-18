from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.vnext.runtime.deduper import Deduper
from tests.vnext.test_runtime_deduper import build_publishable_result


def test_persistent_deduper_survives_reload() -> None:
    result = build_publishable_result()
    first = Deduper(cooldown_seconds=180)
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    first.mark_seen(result, now)
    state = first.snapshot_records(now)

    reloaded = Deduper(cooldown_seconds=180)
    reloaded.load_records(state, now + timedelta(seconds=30))

    assert reloaded.is_duplicate(result, now + timedelta(seconds=30)) is True
    assert reloaded.duplicate_origin(result, now + timedelta(seconds=30)) == "deduped_persistent"


def test_persistent_deduper_cleanup_ttl() -> None:
    result = build_publishable_result()
    deduper = Deduper(cooldown_seconds=60)
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    deduper.mark_seen(result, now)
    deduper.cleanup_expired(now + timedelta(seconds=120))

    assert deduper.is_duplicate(result, now + timedelta(seconds=120)) is False
    assert deduper.snapshot_records(now + timedelta(seconds=120)) == ()
