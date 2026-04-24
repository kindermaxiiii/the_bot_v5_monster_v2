from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.vnext.runtime.models import RuntimeCycleResult


def _jsonify(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonify(asdict(value))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _jsonify(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(item) for item in value]
    return value


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
        "payloads": [_jsonify(payload) for payload in cycle.payloads],
        "refusal_summaries": _jsonify(cycle.refusal_summaries),
        "fixture_audits": [_jsonify(item) for item in cycle.fixture_audits],
        "publication_records": [_jsonify(item) for item in cycle.publication_records],
        "ops_flags": list(cycle.ops_flags),
        "notifier_mode": cycle.notifier_mode,
        "source": cycle.source,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")