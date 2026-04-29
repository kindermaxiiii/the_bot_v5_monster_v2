from __future__ import annotations

import csv
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"

REQUIRED_FIELDS = [
    "research_data_tier",
    "promotion_allowed",
]


def truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def infer_tier(row: dict[str, Any]) -> str:
    existing = str(row.get("research_data_tier") or "").strip()
    if existing:
        return existing

    mode = str(row.get("l3_data_mode") or "").strip().upper()
    state_ready = truthy(row.get("l3_state_ready"))
    trade_ready = truthy(row.get("l3_trade_ready"))

    if mode == "EVENTS_PLUS_STATS":
        return "STRICT_EVENTS_PLUS_STATS"

    if mode == "EVENTS_ONLY":
        return "EVENTS_ONLY_RESEARCH"

    # Legacy fallback: older research rows were usually strict trade-ready rows.
    bucket = str(row.get("research_bucket") or "").upper()
    if bucket in {"UNDER_0_5_RESEARCH", "UNDER_1_5_RESEARCH", "UNDER_2_5_RESEARCH"}:
        if trade_ready:
            return "STRICT_EVENTS_PLUS_STATS"
        if state_ready:
            return "EVENTS_ONLY_RESEARCH"

    return "LEGACY_UNKNOWN_DATA_TIER"


def normalize_bucket(row: dict[str, Any], tier: str) -> str:
    bucket = str(row.get("research_bucket") or "").strip()

    if not bucket:
        selection = str(row.get("selection") or "").upper()
        if "UNDER 0.5" in selection:
            bucket = "UNDER_0_5_RESEARCH"
        elif "UNDER 1.5" in selection:
            bucket = "UNDER_1_5_RESEARCH"
        elif "UNDER 2.5" in selection:
            bucket = "UNDER_2_5_RESEARCH"
        elif "UNDER" in selection:
            bucket = "UNDER_GENERAL_RESEARCH"
        elif "OVER" in selection:
            bucket = "OVER_RESEARCH"
        else:
            bucket = "MARKET_RESEARCH"

    if bucket.startswith(("STRICT_", "EVENTS_ONLY_", "LEGACY_")):
        return bucket

    if tier == "STRICT_EVENTS_PLUS_STATS":
        return f"STRICT_{bucket}"

    if tier == "EVENTS_ONLY_RESEARCH":
        return f"EVENTS_ONLY_{bucket}"

    return f"LEGACY_{bucket}"


def promotion_allowed(tier: str) -> str:
    if tier == "STRICT_EVENTS_PLUS_STATS":
        return "committee_only"

    if tier == "EVENTS_ONLY_RESEARCH":
        return "false"

    return "false"


def main() -> int:
    if not LEDGER.exists():
        print(f"MISSING: {LEDGER}")
        return 1

    backup = LEDGER.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    shutil.copy2(LEDGER, backup)

    with LEDGER.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = list(reader.fieldnames or [])

    for field in REQUIRED_FIELDS:
        if field not in fields:
            fields.append(field)

    if "research_bucket" not in fields:
        fields.append("research_bucket")

    for row in rows:
        tier = infer_tier(row)
        row["research_data_tier"] = tier
        row["research_bucket"] = normalize_bucket(row, tier)

        if not str(row.get("promotion_allowed") or "").strip():
            row["promotion_allowed"] = promotion_allowed(tier)

        if not str(row.get("paper_only") or "").strip():
            row["paper_only"] = "true"

    with LEDGER.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})

    print({
        "status": "READY",
        "ledger": str(LEDGER),
        "backup": str(backup),
        "rows": len(rows),
    })

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
