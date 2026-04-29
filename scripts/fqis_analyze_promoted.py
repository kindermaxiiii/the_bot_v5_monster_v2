from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_promoted_ledger.csv"


def main() -> None:
    if not LEDGER.exists():
        print({"status": "NO_FILE", "file": str(LEDGER)})
        return

    with LEDGER.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    total = len(rows)
    if total == 0:
        print({"status": "EMPTY", "file": str(LEDGER)})
        return

    edges = [_float_value(row.get("edge")) for row in rows]
    expected_values = [_float_value(row.get("expected_value")) for row in rows]
    settled_rows = [row for row in rows if str(row.get("settlement_status", "")).upper() == "SETTLED"]
    pnl_values = [_float_value(row.get("pnl_unit")) for row in settled_rows]

    output = {
        "status": "OK",
        "bets": total,
        "settled": len(settled_rows),
        "pending": total - len(settled_rows),
        "avg_edge_%": round(_average(edges) * 100.0, 3),
        "avg_expected_value_%": round(_average(expected_values) * 100.0, 3),
        "pnl_unit": round(sum(pnl_values), 4),
        "file": str(LEDGER),
    }
    print(output)


def _average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _float_value(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0

    is_percent = text.endswith("%")
    text = text.rstrip("%").strip().replace(",", ".")

    try:
        number = float(text)
    except ValueError:
        return 0.0

    return number / 100.0 if is_percent else number


if __name__ == "__main__":
    main()
