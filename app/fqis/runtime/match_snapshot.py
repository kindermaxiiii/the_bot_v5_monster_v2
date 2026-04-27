from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.contracts.enums import ThesisKey
from app.fqis.runtime.batch_shadow import build_demo_shadow_inputs
from app.fqis.runtime.shadow import FqisShadowInput


@dataclass(slots=True, frozen=True)
class FqisMatchSnapshotBuildResult:
    output_path: Path
    record_count: int
    total_offer_count: int
    event_ids: tuple[int, ...]


def build_match_snapshot_record(
    shadow_input: FqisShadowInput,
    *,
    source_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_id = int(shadow_input.live_match_row["event_id"])

    return {
        "schema_version": 1,
        "snapshot_type": "fqis_match_level",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "event_id": event_id,
        "live_match_row": dict(shadow_input.live_match_row),
        "live_offer_rows": tuple(dict(row) for row in shadow_input.live_offer_rows),
        "p_real_by_thesis": _serialize_p_real_by_thesis(shadow_input.p_real_by_thesis),
        "source_audit": source_audit or {},
    }


def build_demo_match_snapshot_records() -> tuple[dict[str, Any], ...]:
    records: list[dict[str, Any]] = []

    for index, shadow_input in enumerate(build_demo_shadow_inputs(), start=1):
        records.append(
            build_match_snapshot_record(
                shadow_input,
                source_audit={
                    "source": "demo",
                    "builder": "build_demo_match_snapshot_records",
                    "batch_index": index,
                },
            )
        )

    return tuple(records)


def write_match_snapshot_jsonl(
    records: tuple[dict[str, Any], ...],
    output_path: Path,
) -> FqisMatchSnapshotBuildResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    event_ids = tuple(int(record["event_id"]) for record in records)
    total_offer_count = sum(len(record["live_offer_rows"]) for record in records)

    return FqisMatchSnapshotBuildResult(
        output_path=output_path,
        record_count=len(records),
        total_offer_count=total_offer_count,
        event_ids=event_ids,
    )


def _serialize_p_real_by_thesis(
    p_real_by_thesis: dict[ThesisKey, dict[str, float]],
) -> dict[str, dict[str, float]]:
    return {
        thesis_key.value: {
            str(intent_key): float(probability)
            for intent_key, probability in intent_probabilities.items()
        }
        for thesis_key, intent_probabilities in p_real_by_thesis.items()
    }