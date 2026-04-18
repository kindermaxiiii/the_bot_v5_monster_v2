from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.models import RuntimeCounters, RuntimeCycleResult


def test_export_cycle_jsonl() -> None:
    cycle = RuntimeCycleResult(
        cycle_id=1,
        timestamp_utc=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        counters=RuntimeCounters(
            fixture_count_seen=2,
            computed_publish_count=1,
            deduped_count=0,
            notified_count=1,
            silent_count=0,
            unsent_shadow_count=0,
            notifier_attempt_count=1,
        ),
        payloads=(),
        refusal_summaries=(),
        notifier_mode="aggregate",
    )
    path = Path(f"exports/vnext/test_runtime_exporter_{uuid4().hex}.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    export_cycle_jsonl(path, cycle)

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["cycle_id"] == 1
    assert data["fixture_count_seen"] == 2
    assert data["pipeline_publish_count"] == 1
    assert data["deduped_count"] == 0
    assert data["notified_count"] == 1
    assert data["unsent_shadow_count"] == 0
    assert data["notifier_attempt_count"] == 1
    assert data["notifier_mode"] == "aggregate"
