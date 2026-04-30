from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
DECISION_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
LIVE_DECISIONS_JSON = DECISION_DIR / "latest_live_decisions.json"
RESEARCH_CANDIDATES_JSON = RESEARCH_DIR / "latest_research_candidates.json"
RESEARCH_CANDIDATES_LEDGER = RESEARCH_DIR / "research_candidates_ledger.csv"
MONITOR_JSON = ORCH_DIR / "latest_tonight_shadow_monitor.json"
OUT_JSON = ORCH_DIR / "latest_live_freshness_report.json"
OUT_MD = ORCH_DIR / "latest_live_freshness_report.md"

LEDGER_OLD_REVIEW_MINUTES = 180.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def mtime_utc(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()


def parse_dt(value: str | None) -> datetime | None:
    try:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def int_value(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return default


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def unique_values(values: list[Any]) -> list[Any]:
    cleaned = [value for value in values if value is not None and value != ""]
    return sorted(set(cleaned), key=lambda item: str(item))


def ledger_stats(path: Path) -> dict[str, Any]:
    stats = {
        "ledger_rows_total": 0,
        "latest_snapshot_key_count": None,
        "latest_signal_key_count": None,
        "latest_fixture_id_count": None,
    }
    if not path.exists():
        return stats

    snapshots: set[str] = set()
    signals: set[str] = set()
    fixtures: set[str] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            stats["ledger_rows_total"] += 1
            if row.get("snapshot_key"):
                snapshots.add(str(row["snapshot_key"]))
            if row.get("signal_key"):
                signals.add(str(row["signal_key"]))
            if row.get("fixture_id"):
                fixtures.add(str(row["fixture_id"]))

    stats["latest_snapshot_key_count"] = len(snapshots)
    stats["latest_signal_key_count"] = len(signals)
    stats["latest_fixture_id_count"] = len(fixtures)
    return stats


def monitor_read(monitor: dict[str, Any]) -> dict[str, Any]:
    rows = monitor.get("rows") or []
    if not isinstance(rows, list):
        rows = []
    rows = [row for row in rows if isinstance(row, dict)]

    run_dirs = unique_values([row.get("run_dir") for row in rows])
    decision_counts = unique_values([row.get("decisions_total") for row in rows])
    candidate_counts = unique_values([row.get("candidates_this_cycle") for row in rows])
    new_snapshot_counts = unique_values([row.get("new_snapshots_appended") for row in rows])
    post_quarantine_pnls = unique_values([row.get("post_quarantine_pnl") for row in rows])
    fixture_pnls = unique_values([
        row.get("fixture_pnl")
        if row.get("fixture_pnl") is not None
        else row.get("fixture_pnl_unit")
        for row in rows
    ])

    return {
        "rows": rows,
        "monitor_cycles_completed": int_value(monitor.get("cycles_completed"), len(rows)),
        "unique_run_dirs_in_monitor": run_dirs,
        "unique_decisions_total_in_monitor": decision_counts,
        "unique_candidates_this_cycle_in_monitor": candidate_counts,
        "unique_new_snapshots_appended_in_monitor": new_snapshot_counts,
        "unique_post_quarantine_pnl_in_monitor": post_quarantine_pnls,
        "unique_fixture_pnl_in_monitor": fixture_pnls,
    }


def old_ledger_flag(generated_at_utc: str, ledger_mtime: str | None) -> bool:
    generated = parse_dt(generated_at_utc)
    ledger_dt = parse_dt(ledger_mtime)
    if generated is None or ledger_dt is None:
        return False
    age_minutes = (generated - ledger_dt).total_seconds() / 60.0
    return age_minutes > LEDGER_OLD_REVIEW_MINUTES


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    full_cycle = read_json(FULL_CYCLE_JSON)
    live_decisions = read_json(LIVE_DECISIONS_JSON)
    research_candidates = read_json(RESEARCH_CANDIDATES_JSON)
    monitor = read_json(MONITOR_JSON) if MONITOR_JSON.exists() else {}

    missing_inputs = []
    if full_cycle.get("missing") or full_cycle.get("error"):
        missing_inputs.append(str(FULL_CYCLE_JSON))
    if live_decisions.get("missing") or live_decisions.get("error"):
        missing_inputs.append(str(LIVE_DECISIONS_JSON))
    if research_candidates.get("missing") or research_candidates.get("error"):
        missing_inputs.append(str(RESEARCH_CANDIDATES_JSON))
    if not RESEARCH_CANDIDATES_LEDGER.exists():
        missing_inputs.append(str(RESEARCH_CANDIDATES_LEDGER))

    live_summary = live_decisions.get("summary") or {}
    decisions = live_decisions.get("decisions") or []
    research_summary = research_candidates.get("summary") or {}
    monitor_info = monitor_read(monitor)

    decisions_total = int_value(live_summary.get("decisions_total"), len(decisions) if isinstance(decisions, list) else 0)
    groups_total = int_value(live_summary.get("groups_total"))
    groups_priced = int_value(live_summary.get("groups_priced"))
    groups_skipped_no_level3 = int_value(live_summary.get("groups_skipped_no_level3"))
    candidates_this_cycle = int_value(research_summary.get("candidates_this_cycle"))
    new_snapshots_appended = int_value(research_summary.get("new_snapshots_appended"))

    flags: list[str] = []
    historical_metric_static_review: list[str] = []
    if live_decisions.get("missing") or live_decisions.get("error"):
        flags.append("MISSING_LIVE_DECISIONS")
    if research_candidates.get("missing") or research_candidates.get("error"):
        flags.append("MISSING_RESEARCH_CANDIDATES")
    if decisions_total == 0:
        flags.append("ZERO_DECISIONS")
    if candidates_this_cycle == 0:
        flags.append("ZERO_CANDIDATES_THIS_CYCLE")
    if new_snapshots_appended == 0:
        flags.append("ZERO_NEW_SNAPSHOTS_THIS_CYCLE")

    rows = monitor_info["rows"]
    if len(rows) > 1 and len(monitor_info["unique_run_dirs_in_monitor"]) <= 1:
        flags.append("MONITOR_RUN_DIR_NOT_CHANGING")
    if len(rows) > 1 and len(monitor_info["unique_decisions_total_in_monitor"]) == 1:
        flags.append("MONITOR_DECISION_COUNTS_NOT_CHANGING")

    ledger_mtime = mtime_utc(RESEARCH_CANDIDATES_LEDGER)
    if old_ledger_flag(generated_at_utc, ledger_mtime):
        flags.append("LEDGER_MTIME_OLD_REVIEW")
    if len(rows) > 1 and len(monitor_info["unique_post_quarantine_pnl_in_monitor"]) == 1:
        historical_metric_static_review.append("CONSTANT_POST_QUARANTINE_PNL_REVIEW")
    if len(rows) > 1 and len(monitor_info["unique_fixture_pnl_in_monitor"]) == 1:
        historical_metric_static_review.append("CONSTANT_FIXTURE_PNL_REVIEW")

    status = "MISSING_INPUTS" if missing_inputs else ("STALE_REVIEW" if flags else "READY")
    if not flags and status == "READY":
        flags.append("OK_FRESH_LIVE_CYCLE")

    payload = {
        "status": status,
        "generated_at_utc": generated_at_utc,
        "full_cycle_run_dir": full_cycle.get("run_dir"),
        "decisions_total": decisions_total,
        "groups_total": groups_total,
        "groups_priced": groups_priced,
        "groups_skipped_no_level3": groups_skipped_no_level3,
        "candidates_this_cycle": candidates_this_cycle,
        "new_snapshots_appended": new_snapshots_appended,
        "latest_decision_file_mtime_utc": mtime_utc(LIVE_DECISIONS_JSON),
        "latest_research_candidates_mtime_utc": mtime_utc(RESEARCH_CANDIDATES_JSON),
        "ledger_mtime_utc": ledger_mtime,
        **ledger_stats(RESEARCH_CANDIDATES_LEDGER),
        "monitor_cycles_completed": monitor_info["monitor_cycles_completed"] if monitor else None,
        "unique_run_dirs_in_monitor": monitor_info["unique_run_dirs_in_monitor"] if monitor else [],
        "unique_decisions_total_in_monitor": monitor_info["unique_decisions_total_in_monitor"] if monitor else [],
        "unique_candidates_this_cycle_in_monitor": monitor_info["unique_candidates_this_cycle_in_monitor"] if monitor else [],
        "unique_new_snapshots_appended_in_monitor": monitor_info["unique_new_snapshots_appended_in_monitor"] if monitor else [],
        "freshness_flags": flags,
        "live_freshness_flags": flags,
        "historical_metric_static_review": historical_metric_static_review,
        "economic_static_review": historical_metric_static_review,
        "all_review_flags": [*flags, *historical_metric_static_review],
        "missing_inputs": missing_inputs,
        "read": {
            "safety_readiness": "UNCHANGED_BY_FRESHNESS_REPORT",
            "data_freshness": status,
            "economic_performance": (
                "HISTORICAL_STATIC_REVIEW_ONLY_NOT_LIVE_FRESHNESS"
                if historical_metric_static_review
                else "NO_HISTORICAL_STATIC_REVIEW"
            ),
        },
    }
    return payload


def markdown_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "[]"
    return str(value)


def write_markdown(payload: dict[str, Any]) -> None:
    fields = [
        "status",
        "full_cycle_run_dir",
        "decisions_total",
        "groups_total",
        "groups_priced",
        "groups_skipped_no_level3",
        "candidates_this_cycle",
        "new_snapshots_appended",
        "ledger_rows_total",
        "latest_snapshot_key_count",
        "latest_signal_key_count",
        "latest_fixture_id_count",
        "monitor_cycles_completed",
        "unique_run_dirs_in_monitor",
        "unique_decisions_total_in_monitor",
        "unique_candidates_this_cycle_in_monitor",
        "unique_new_snapshots_appended_in_monitor",
        "freshness_flags",
        "historical_metric_static_review",
        "all_review_flags",
    ]

    read = payload.get("read") or {}
    lines = [
        "# FQIS Live Freshness Report",
        "",
        f"- Generated at UTC: `{payload.get('generated_at_utc')}`",
        f"- Safety readiness: **{read.get('safety_readiness')}**",
        f"- Data freshness: **{read.get('data_freshness')}**",
        f"- Economic performance: **{read.get('economic_performance')}**",
        "",
        "## Fields",
        "",
        *[f"- {field}: **{markdown_value(payload.get(field))}**" for field in fields],
    ]

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_outputs(payload: dict[str, Any]) -> None:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    write_markdown(payload)


def main() -> int:
    payload = build_payload()
    write_outputs(payload)
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 2 if payload["status"] == "MISSING_INPUTS" else 0


if __name__ == "__main__":
    raise SystemExit(main())
