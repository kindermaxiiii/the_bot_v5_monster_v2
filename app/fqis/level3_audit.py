from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


LEVEL3_AUDIT_COLUMNS = [
    "timestamp_utc",
    "fixture_id",
    "state",
    "events_available",
    "stats_available",
    "production_eligible",
    "research_eligible",
    "live_staking_allowed",
    "vetoes",
    "reason",
]


def append_level3_audit_row(
    *,
    path: Path,
    fixture_id: str,
    state: str,
    events_available: bool,
    stats_available: bool,
    production_eligible: bool,
    research_eligible: bool,
    live_staking_allowed: bool,
    vetoes: Iterable[str],
    reason: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEVEL3_AUDIT_COLUMNS)
        if not exists:
            writer.writeheader()

        writer.writerow(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "fixture_id": fixture_id,
                "state": state,
                "events_available": int(events_available),
                "stats_available": int(stats_available),
                "production_eligible": int(production_eligible),
                "research_eligible": int(research_eligible),
                "live_staking_allowed": int(live_staking_allowed),
                "vetoes": "|".join(vetoes),
                "reason": reason,
            }
        )
