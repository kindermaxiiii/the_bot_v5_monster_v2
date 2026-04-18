from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from app.vnext.runtime.models import RuntimeCycleResult


def export_cycle_jsonl(path: Path, cycle: RuntimeCycleResult) -> None:
    payload = {
        "cycle_id": cycle.cycle_id,
        "timestamp_utc": cycle.timestamp_utc.isoformat(),
        "fixture_count_seen": cycle.counters.fixture_count_seen,
        "pipeline_publish_count": cycle.counters.computed_publish_count,
        "deduped_count": cycle.counters.deduped_count,
        "notified_count": cycle.counters.notified_count,
        "silent_count": cycle.counters.silent_count,
        "unsent_shadow_count": cycle.counters.unsent_shadow_count,
        "notifier_attempt_count": cycle.counters.notifier_attempt_count,
        "payloads": [asdict(payload) for payload in cycle.payloads],
        "refusal_summaries": cycle.refusal_summaries,
        "fixture_audits": list(cycle.fixture_audits),
        "publication_records": list(cycle.publication_records),
        "ops_flags": list(cycle.ops_flags),
        "notifier_mode": cycle.notifier_mode,
        "source": cycle.source,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
