from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fqis_research_settlement import FINISHED_STATUS, fnum, safe_int, settled_result

LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_promoted_ledger.csv"
CANDIDATES_LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"

SETTLEMENT_COLUMNS = (
    "settlement_status",
    "result_status",
    "pnl_unit",
    "final_total_goals",
    "fixture_status_short",
)

LOCAL_JSON_SOURCES = (
    ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.json",
    ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.json",
    ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "latest_level3_live_state.json",
)

LOCAL_GLOBS = (
    "data/pipeline/api_sports/decision_bridge_live/**/inplay_fixtures.json",
    "data/pipeline/api_sports/raw_market_monitor/**/inplay_fixtures.json",
    "data/pipeline/api_sports/orchestrator*/**/inplay_fixtures.json",
)


@dataclass(frozen=True)
class FixtureResult:
    fixture_id: str
    fixture_status_short: str
    final_total_goals: int | None
    source_path: str
    source_mtime: float

    @property
    def is_finished(self) -> bool:
        return self.fixture_status_short.upper() in FINISHED_STATUS

    @property
    def has_score(self) -> bool:
        return self.final_total_goals is not None


def main() -> int:
    rows, fieldnames = read_rows(LEDGER)
    if not rows:
        print(
            {
                "status": "NO_ROWS",
                "ledger": str(LEDGER),
                "settled": 0,
                "pending": 0,
                "missing_results": 0,
            }
        )
        return 0

    pending_fixture_ids = {
        str(row.get("fixture_id") or "").strip()
        for row in rows
        if str(row.get("settlement_status") or "PENDING").upper() == "PENDING"
        and str(row.get("fixture_id") or "").strip()
    }
    local_results = load_local_results(pending_fixture_ids)

    settled_count = 0
    still_pending_count = 0
    missing_results_count = 0
    not_finished_count = 0

    updated_rows: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        status = str(updated.get("settlement_status") or "PENDING").upper()
        fixture_id = str(updated.get("fixture_id") or "").strip()

        if status != "PENDING" or not fixture_id:
            updated_rows.append(updated)
            continue

        fixture_result = local_results.get(fixture_id)
        if fixture_result is None:
            missing_results_count += 1
            still_pending_count += 1
            updated["settlement_status"] = "PENDING"
            updated_rows.append(updated)
            continue

        if fixture_result.final_total_goals is not None:
            updated["final_total_goals"] = str(fixture_result.final_total_goals)
        if fixture_result.fixture_status_short:
            updated["fixture_status_short"] = fixture_result.fixture_status_short

        if not fixture_result.is_finished:
            not_finished_count += 1
            still_pending_count += 1
            updated["settlement_status"] = "PENDING"
            updated["result_status"] = ""
            updated["pnl_unit"] = ""
            updated_rows.append(updated)
            continue

        if fixture_result.final_total_goals is None:
            missing_results_count += 1
            still_pending_count += 1
            updated["settlement_status"] = "PENDING"
            updated["result_status"] = ""
            updated["pnl_unit"] = ""
            updated_rows.append(updated)
            continue

        result_status, pnl_unit = settled_result(
            str(updated.get("side") or ""),
            fixture_result.final_total_goals,
            fnum(updated.get("line")),
            fnum(updated.get("odds_decimal")),
        )
        updated["settlement_status"] = "SETTLED" if result_status in {"WIN", "LOSS", "PUSH"} else "PENDING"
        updated["result_status"] = result_status if updated["settlement_status"] == "SETTLED" else ""
        updated["pnl_unit"] = pnl_unit if updated["settlement_status"] == "SETTLED" else ""

        if updated["settlement_status"] == "SETTLED":
            settled_count += 1
        else:
            still_pending_count += 1

        updated_rows.append(updated)

    write_rows(LEDGER, updated_rows, fieldnames)

    summary = {
        "status": "READY",
        "ledger": str(LEDGER),
        "rows": len(updated_rows),
        "pending_fixture_ids": len(pending_fixture_ids),
        "local_results_found": len(local_results),
        "settled": settled_count,
        "pending": still_pending_count,
        "not_finished": not_finished_count,
        "missing_results": missing_results_count,
        "candidates_ledger_touched": False,
    }
    print(summary)
    return 0


def read_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def write_rows(path: Path, rows: list[dict[str, Any]], original_fields: list[str]) -> None:
    fieldnames = list(original_fields)
    for column in SETTLEMENT_COLUMNS:
        if column not in fieldnames:
            fieldnames.append(column)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def load_local_results(fixture_ids: set[str]) -> dict[str, FixtureResult]:
    if not fixture_ids:
        return {}

    candidates: list[FixtureResult] = []

    for row in read_csv_source(CANDIDATES_LEDGER):
        result = fixture_result_from_mapping(row, source_path=str(CANDIDATES_LEDGER), source_mtime=file_mtime(CANDIDATES_LEDGER))
        if result and result.fixture_id in fixture_ids:
            candidates.append(result)

    for path in iter_json_sources():
        for item in iter_json_source(path):
            result = fixture_result_from_mapping(item, source_path=str(path), source_mtime=file_mtime(path))
            if result and result.fixture_id in fixture_ids:
                candidates.append(result)

    best_by_fixture: dict[str, FixtureResult] = {}
    for candidate in candidates:
        current = best_by_fixture.get(candidate.fixture_id)
        if current is None or result_rank(candidate) > result_rank(current):
            best_by_fixture[candidate.fixture_id] = candidate

    return best_by_fixture


def iter_json_sources() -> Iterable[Path]:
    seen: set[Path] = set()
    for path in LOCAL_JSON_SOURCES:
        if path.exists() and path not in seen:
            seen.add(path)
            yield path

    for pattern in LOCAL_GLOBS:
        for path in ROOT.glob(pattern):
            if path.exists() and path not in seen:
                seen.add(path)
                yield path


def iter_json_source(path: Path) -> Iterable[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return []

    return iter_mappings(payload)


def iter_mappings(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_mappings(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_mappings(child)


def read_csv_source(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return []

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def fixture_result_from_mapping(row: dict[str, Any], *, source_path: str, source_mtime: float) -> FixtureResult | None:
    fixture_id = resolve_fixture_id(row)
    if not fixture_id:
        return None

    status_short = resolve_status_short(row)
    total_goals = resolve_total_goals(row)

    if not status_short:
        return None

    return FixtureResult(
        fixture_id=fixture_id,
        fixture_status_short=status_short,
        final_total_goals=total_goals,
        source_path=source_path,
        source_mtime=source_mtime,
    )


def resolve_fixture_id(row: dict[str, Any]) -> str:
    for key in ("fixture_id", "event_id", "match_id"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()

    fixture = row.get("fixture")
    if isinstance(fixture, dict) and fixture.get("id") not in (None, ""):
        return str(fixture["id"]).strip()

    return ""


def resolve_status_short(row: dict[str, Any]) -> str:
    for key in ("fixture_status_short", "status_short"):
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip().upper()

    status = row.get("status")
    if isinstance(status, dict) and status.get("short") not in (None, ""):
        return str(status["short"]).strip().upper()
    if isinstance(status, str) and len(status.strip()) <= 5:
        return status.strip().upper()

    fixture = row.get("fixture")
    if isinstance(fixture, dict):
        fixture_status = fixture.get("status")
        if isinstance(fixture_status, dict) and fixture_status.get("short") not in (None, ""):
            return str(fixture_status["short"]).strip().upper()

    return ""


def resolve_total_goals(row: dict[str, Any]) -> int | None:
    total = safe_int(row.get("final_total_goals"))
    if total is not None:
        return total

    home, away = resolve_home_away_goals(row)
    if home is not None and away is not None:
        return home + away

    return None


def resolve_home_away_goals(row: dict[str, Any]) -> tuple[int | None, int | None]:
    home = first_int(row, ("final_home_goals", "home_goals", "goals_home", "score_home", "home_score_final", "home_final_score", "home_score"))
    away = first_int(row, ("final_away_goals", "away_goals", "goals_away", "score_away", "away_score_final", "away_final_score", "away_score"))
    if home is not None or away is not None:
        return home, away

    goals = row.get("goals")
    if isinstance(goals, dict):
        return safe_int(goals.get("home")), safe_int(goals.get("away"))

    return None, None


def first_int(row: dict[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = safe_int(row.get(key))
        if value is not None:
            return value
    return None


def result_rank(result: FixtureResult) -> tuple[int, int, float]:
    return (
        1 if result.is_finished else 0,
        1 if result.has_score else 0,
        1 if result.fixture_status_short else 0,
        result.source_mtime,
    )


def file_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


if __name__ == "__main__":
    raise SystemExit(main())
