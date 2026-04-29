from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SIGNALS_FILE = ROOT / "data" / "pipeline" / "api_sports" / "fqis_paper_signals" / "latest_paper_signals.md"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_promoted_ledger.csv"

PROMOTED_MARKDOWN_FIELDS = (
    "fixture_id",
    "match",
    "score",
    "minute",
    "side",
    "line",
    "selection",
    "odds_decimal",
    "model_probability",
    "edge",
    "expected_value",
    "vetoes",
)

LEDGER_FIELDS = (
    "promoted_key",
    "observed_at_utc",
    "fixture_id",
    "match",
    "score",
    "minute",
    "side",
    "line",
    "selection",
    "odds_decimal",
    "model_probability",
    "edge",
    "expected_value",
    "vetoes",
    "settlement_status",
    "result_status",
    "pnl_unit",
    "final_total_goals",
    "fixture_status_short",
)


def main() -> None:
    existing_rows, seen_keys, seen_fixture_ids, dropped_existing_rows = _read_existing_rows()
    promoted_rows = _parse_promoted_rows()

    new_rows: list[dict[str, str]] = []
    skipped_existing_fixture = 0
    for row in promoted_rows:
        key = row["promoted_key"]
        fixture_id = row.get("fixture_id", "")
        if key in seen_keys:
            continue
        if fixture_id and fixture_id in seen_fixture_ids:
            skipped_existing_fixture += 1
            continue
        seen_keys.add(key)
        if fixture_id:
            seen_fixture_ids.add(fixture_id)
        new_rows.append(row)

    all_rows = existing_rows + new_rows
    _write_rows(all_rows)

    print(
        {
            "status": "READY",
            "parsed_promoted": len(promoted_rows),
            "new_promoted": len(new_rows),
            "skipped_existing_fixture": skipped_existing_fixture,
            "total": len(all_rows),
            "dropped_existing_rows": dropped_existing_rows,
            "file": str(LEDGER),
        }
    )


def _parse_promoted_rows() -> list[dict[str, str]]:
    if not SIGNALS_FILE.exists():
        return []

    rows: list[dict[str, str]] = []
    active = False

    for raw_line in SIGNALS_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.upper().startswith("## PROMOTED"):
            active = True
            continue
        if active and line.startswith("## "):
            break
        if not active or not line.startswith("|"):
            continue

        cells = [_clean_cell(cell) for cell in line.split("|")[1:-1]]
        if _is_markdown_header(cells):
            continue
        if len(cells) != len(PROMOTED_MARKDOWN_FIELDS):
            continue
        if all(cell == "-" for cell in cells):
            continue

        parsed = dict(zip(PROMOTED_MARKDOWN_FIELDS, cells))
        row = _ledger_row_from_markdown(parsed)
        rows.append(row)

    return rows


def _ledger_row_from_markdown(row: dict[str, str]) -> dict[str, str]:
    normalized = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "fixture_id": row.get("fixture_id", ""),
        "match": row.get("match", ""),
        "score": row.get("score", ""),
        "minute": _normalize_integer(row.get("minute", "")),
        "side": row.get("side", "").upper(),
        "line": _normalize_decimal(row.get("line", "")),
        "selection": row.get("selection", ""),
        "odds_decimal": _normalize_decimal(row.get("odds_decimal", "")),
        "model_probability": _normalize_decimal(row.get("model_probability", "")),
        "edge": _normalize_decimal(row.get("edge", "")),
        "expected_value": _normalize_decimal(row.get("expected_value", "")),
        "vetoes": row.get("vetoes", ""),
        "settlement_status": "PENDING",
        "result_status": "",
        "pnl_unit": "",
        "final_total_goals": "",
        "fixture_status_short": "",
    }
    normalized["promoted_key"] = _make_promoted_key(normalized)
    return {field: normalized.get(field, "") for field in LEDGER_FIELDS}


def _read_existing_rows() -> tuple[list[dict[str, str]], set[str], set[str], int]:
    if not LEDGER.exists():
        return [], set(), set(), 0

    rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    seen_fixture_ids: set[str] = set()
    dropped = 0

    with LEDGER.open("r", encoding="utf-8", newline="") as handle:
        for raw_row in csv.DictReader(handle):
            normalized = _normalize_existing_row(raw_row)
            key = normalized.get("promoted_key", "")
            if not key:
                dropped += 1
                continue
            if key in seen_keys:
                dropped += 1
                continue
            seen_keys.add(key)
            fixture_id = normalized.get("fixture_id", "")
            if fixture_id and fixture_id in seen_fixture_ids:
                dropped += 1
                continue
            if fixture_id:
                seen_fixture_ids.add(fixture_id)
            rows.append(normalized)

    return rows, seen_keys, seen_fixture_ids, dropped


def _normalize_existing_row(row: dict[str, Any]) -> dict[str, str]:
    normalized = {field: str(row.get(field, "") or "").strip() for field in LEDGER_FIELDS}
    if not normalized["expected_value"] and row.get("ev") not in (None, ""):
        normalized["expected_value"] = _normalize_decimal(row.get("ev", ""))

    for numeric_field in ("line", "odds_decimal", "model_probability", "edge", "expected_value"):
        normalized[numeric_field] = _normalize_decimal(normalized[numeric_field])

    normalized["minute"] = _normalize_integer(normalized["minute"])
    normalized["side"] = normalized["side"].upper()

    if not normalized["promoted_key"]:
        if _has_minimum_identity(normalized):
            normalized["promoted_key"] = _make_promoted_key(normalized)

    return normalized


def _has_minimum_identity(row: dict[str, str]) -> bool:
    return bool(row.get("fixture_id") and row.get("selection") and row.get("expected_value"))


def _write_rows(rows: list[dict[str, str]]) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in LEDGER_FIELDS})


def _make_promoted_key(row: dict[str, str]) -> str:
    parts = (
        row.get("fixture_id", ""),
        row.get("match", ""),
        row.get("minute", ""),
        row.get("side", ""),
        row.get("line", ""),
        row.get("selection", ""),
        row.get("odds_decimal", ""),
        row.get("model_probability", ""),
        row.get("edge", ""),
        row.get("expected_value", ""),
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def _is_markdown_header(cells: list[str]) -> bool:
    if not cells:
        return True
    first = cells[0].strip().lower()
    if first in {"fixture_id", "fixture", "match"}:
        return True
    return all(set(cell.strip()) <= {"-", ":"} for cell in cells if cell.strip())


def _clean_cell(value: str) -> str:
    return value.strip()


def _normalize_integer(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return str(int(float(text.replace(",", "."))))
    except ValueError:
        return text


def _normalize_decimal(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return ""

    is_percent = text.endswith("%")
    text = text.rstrip("%").strip().replace(",", ".")

    try:
        number = float(text)
    except ValueError:
        return text

    if is_percent:
        number /= 100.0

    rendered = f"{number:.6f}".rstrip("0").rstrip(".")
    return rendered if rendered not in {"", "-0"} else "0"


if __name__ == "__main__":
    main()
