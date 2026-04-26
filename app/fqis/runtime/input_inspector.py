from __future__ import annotations

from pathlib import Path
from typing import Any

from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl


def inspect_shadow_input_file(path: Path) -> dict[str, Any]:
    inputs = load_shadow_inputs_from_jsonl(path)

    event_ids = [item.live_match_row["event_id"] for item in inputs]
    total_offer_count = sum(len(item.live_offer_rows) for item in inputs)

    thesis_keys = sorted(
        {
            thesis_key.value
            for item in inputs
            for thesis_key in item.p_real_by_thesis.keys()
        }
    )

    duplicate_event_ids = sorted(
        {
            event_id
            for event_id in event_ids
            if event_ids.count(event_id) > 1
        }
    )

    return {
        "status": "ok",
        "path": str(path),
        "match_count": len(inputs),
        "event_ids": event_ids,
        "duplicate_event_ids": duplicate_event_ids,
        "total_offer_count": total_offer_count,
        "thesis_keys": thesis_keys,
        "has_duplicates": len(duplicate_event_ids) > 0,
    }

    