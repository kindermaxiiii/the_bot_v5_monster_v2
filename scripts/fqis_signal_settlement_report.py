from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
RESEARCH_SETTLEMENT_JSON = RESEARCH_DIR / "latest_research_settlement.json"
FIXTURE_LEVEL_RESEARCH_JSON = RESEARCH_DIR / "latest_fixture_level_research_report.json"
OUT_JSON = RESEARCH_DIR / "latest_signal_settlement_report.json"
OUT_MD = RESEARCH_DIR / "latest_signal_settlement_report.md"

SAFETY_BLOCK = {
    "paper_only": True,
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
}

SIGNAL_ROW_FIELDS = [
    "canonical_alert_key",
    "alert_key",
    "fixture_id",
    "match",
    "league",
    "market",
    "selection",
    "research_bucket",
    "data_tier",
    "minute",
    "minute_bucket",
    "score_at_signal",
    "odds_taken",
    "odds_latest",
    "odds",
    "p_model",
    "implied_probability",
    "edge_prob",
    "ev_real",
    "lifecycle",
    "paper_action",
    "bucket_policy_action",
    "red_flags",
    "settlement_status",
    "result_status",
    "final_score",
    "paper_stake",
    "paper_pnl",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            return {"error": "JSON_ROOT_NOT_OBJECT", "path": str(path)}
        return payload
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def safe_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    return text or default


def clean_key(value: Any) -> str:
    return str(value or "").strip()


def records_from_ranker(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for field in ("grouped_ranked_alerts", "ranked_alerts", "raw_ranked_alerts", "top_ranked_alerts"):
        records = payload.get(field)
        if isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]
    return []


def records_from_dedupe(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for field in ("new_alert_records", "updated_alert_records", "raw_new_alert_records", "alert_records"):
        values = payload.get(field)
        if isinstance(values, list):
            records.extend(record for record in values if isinstance(record, dict))
    return records


def signal_records(ranker: dict[str, Any], dedupe: dict[str, Any]) -> list[dict[str, Any]]:
    records = records_from_ranker(ranker)
    if not records:
        records = records_from_dedupe(dedupe)

    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for record in records:
        key = clean_key(record.get("canonical_alert_key") or record.get("alert_key"))
        if not key:
            key = json.dumps(record, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def parse_score_string(value: Any) -> tuple[str, int] | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"(\d+)\s*[-:]\s*(\d+)", text)
    if not match:
        return None
    home = int(match.group(1))
    away = int(match.group(2))
    return f"{home}-{away}", home + away


def first_int(row: dict[str, Any], fields: tuple[str, ...]) -> int | None:
    for field in fields:
        number = fnum(row.get(field))
        if number is not None:
            return int(number)
    return None


def final_score_from_row(row: dict[str, Any]) -> dict[str, Any] | None:
    for field in ("final_score", "score_final", "full_time_score", "ft_score"):
        parsed = parse_score_string(row.get(field))
        if parsed:
            display, total = parsed
            return {"display": display, "total_goals": total, "source_field": field}

    home = first_int(
        row,
        (
            "final_home_goals",
            "home_goals",
            "goals_home",
            "score_home",
            "home_score_final",
            "home_final_score",
            "home_score",
        ),
    )
    away = first_int(
        row,
        (
            "final_away_goals",
            "away_goals",
            "goals_away",
            "score_away",
            "away_score_final",
            "away_final_score",
            "away_score",
        ),
    )
    if home is not None and away is not None:
        return {"display": f"{home}-{away}", "total_goals": home + away, "source_field": "final_home_away_goals"}

    total = first_int(row, ("final_total_goals", "total_goals_final", "goals_total"))
    if total is not None:
        return {"display": f"TOTAL_GOALS={total}", "total_goals": total, "source_field": "final_total_goals"}

    return None


def iter_dict_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    rows: list[dict[str, Any]] = []
    for field in ("rows", "records", "fixtures", "fixture_rows", "concentration"):
        values = payload.get(field)
        if isinstance(values, list):
            rows.extend(item for item in values if isinstance(item, dict))
    return rows


def final_score_index(payloads: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for source_name, payload in payloads.items():
        if payload.get("missing") or payload.get("error"):
            continue
        for row in iter_dict_rows(payload):
            score = final_score_from_row(row)
            if not score:
                continue
            score = {**score, "source": source_name}
            fixture_id = clean_key(row.get("fixture_id") or row.get("event_id") or row.get("match_id"))
            match = clean_key(row.get("match"))
            if fixture_id:
                index.setdefault(f"fixture:{fixture_id}", score)
            if match:
                index.setdefault(f"match:{match.lower()}", score)
    return index


def resolve_final_score(record: dict[str, Any], index: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    own_score = final_score_from_row(record)
    if own_score:
        return {**own_score, "source": "signal_record"}

    fixture_id = clean_key(record.get("fixture_id") or record.get("event_id") or record.get("match_id"))
    if fixture_id and f"fixture:{fixture_id}" in index:
        return index[f"fixture:{fixture_id}"]

    match = clean_key(record.get("match"))
    if match and f"match:{match.lower()}" in index:
        return index[f"match:{match.lower()}"]
    return None


def parse_total_goals_selection(selection: Any) -> tuple[str, float] | None:
    text = str(selection or "").strip()
    match = re.match(r"^(over|under)\s+(\d+(?:\.\d+)?)$", text, flags=re.IGNORECASE)
    if not match:
        return None
    line = float(match.group(2))
    if abs(line * 10 % 10 - 5) > 1e-9:
        return None
    return match.group(1).upper(), line


def is_total_goals_ft(record: dict[str, Any]) -> bool:
    market = str(record.get("market") or record.get("market_key") or "").strip().lower()
    market_key = str(record.get("market_key") or "").strip().upper()
    return market_key == "OU_FT" or ("total goals" in market and "ft" in market)


def odds_taken(record: dict[str, Any]) -> float | None:
    for field in ("odds_taken", "odds", "odds_latest", "odds_first", "odds_decimal"):
        odds = fnum(record.get(field))
        if odds is not None and odds > 1.0:
            return odds
    return None


def settle_result(record: dict[str, Any], final_score: dict[str, Any] | None) -> tuple[str, str]:
    if not final_score:
        return "UNSETTLED", "UNKNOWN"
    if not is_total_goals_ft(record):
        return "UNKNOWN", "UNKNOWN"

    parsed_selection = parse_total_goals_selection(record.get("selection"))
    if not parsed_selection:
        return "UNKNOWN", "UNKNOWN"

    side, line = parsed_selection
    total_goals = int(final_score["total_goals"])
    if side == "OVER":
        if total_goals > line:
            return "SETTLED", "WIN"
        if total_goals < line:
            return "SETTLED", "LOSS"
        return "SETTLED", "PUSH"

    if total_goals < line:
        return "SETTLED", "WIN"
    if total_goals > line:
        return "SETTLED", "LOSS"
    return "SETTLED", "PUSH"


def paper_pnl(result_status: str, odds: float | None) -> float:
    if result_status == "WIN" and odds is not None:
        return round(odds - 1.0, 6)
    if result_status == "LOSS":
        return -1.0
    return 0.0


def signal_row(record: dict[str, Any], index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    final_score = resolve_final_score(record, index)
    settlement_status, result_status = settle_result(record, final_score)
    taken = odds_taken(record)
    row = {
        "canonical_alert_key": record.get("canonical_alert_key"),
        "alert_key": record.get("alert_key"),
        "fixture_id": record.get("fixture_id"),
        "match": record.get("match"),
        "league": record.get("league"),
        "market": record.get("market") or record.get("market_key"),
        "selection": record.get("selection"),
        "research_bucket": record.get("research_bucket"),
        "data_tier": record.get("data_tier") or record.get("research_data_tier"),
        "minute": record.get("minute"),
        "minute_bucket": record.get("minute_bucket"),
        "score_at_signal": record.get("score") or record.get("score_at_signal"),
        "odds_taken": taken,
        "odds_latest": fnum(record.get("odds_latest")),
        "odds": fnum(record.get("odds")) or taken,
        "p_model": fnum(record.get("p_model") or record.get("calibrated_probability") or record.get("raw_probability")),
        "implied_probability": fnum(record.get("implied_probability")),
        "edge_prob": fnum(record.get("edge_prob") or record.get("edge")),
        "ev_real": fnum(record.get("ev_real") or record.get("expected_value")),
        "lifecycle": record.get("alert_lifecycle_status") or record.get("lifecycle") or record.get("dedupe_status"),
        "paper_action": record.get("paper_action"),
        "bucket_policy_action": record.get("bucket_policy_action"),
        "red_flags": record.get("red_flags") if isinstance(record.get("red_flags"), list) else ([] if not record.get("red_flags") else [record.get("red_flags")]),
        "settlement_status": settlement_status,
        "result_status": result_status,
        "final_score": (final_score or {}).get("display"),
        "final_score_source": (final_score or {}).get("source"),
        "paper_stake": 1.0,
        "paper_pnl": paper_pnl(result_status, taken),
    }
    return {field: row.get(field) for field in [*SIGNAL_ROW_FIELDS, "final_score_source"]}


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    settled = [row for row in rows if row.get("settlement_status") == "SETTLED"]
    wins = [row for row in settled if row.get("result_status") == "WIN"]
    losses = [row for row in settled if row.get("result_status") == "LOSS"]
    pushes = [row for row in settled if row.get("result_status") == "PUSH"]
    unknown = [row for row in rows if row.get("result_status") == "UNKNOWN"]
    unsettled = [row for row in rows if row.get("settlement_status") == "UNSETTLED"]
    pnl_total = round(sum(float(row.get("paper_pnl") or 0.0) for row in settled), 6)
    roi = round(pnl_total / len(settled), 6) if settled else 0.0
    return {
        "total_signals": len(rows),
        "settled_signals": len(settled),
        "unsettled_signals": len(unsettled),
        "win_count": len(wins),
        "loss_count": len(losses),
        "push_count": len(pushes),
        "unknown_count": len(unknown),
        "paper_pnl_total": pnl_total,
        "paper_roi": roi,
        "avg_odds_taken": mean([float(row["odds_taken"]) for row in rows if row.get("odds_taken") is not None]),
    }


def group_summary(rows: list[dict[str, Any]], key_name: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(safe_text(row.get(key_name)), []).append(row)
    return {key: summarize(group_rows) for key, group_rows in sorted(grouped.items())}


def compound_group_summary(rows: list[dict[str, Any]], key_names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = "||".join(safe_text(row.get(key_name)) for key_name in key_names)
        grouped.setdefault(key, []).append(row)
    return {key: summarize(group_rows) for key, group_rows in sorted(grouped.items())}


def build_report(
    *,
    ranker_path: Path = PAPER_ALERT_RANKER_JSON,
    dedupe_path: Path = PAPER_ALERT_DEDUPE_JSON,
    research_settlement_path: Path = RESEARCH_SETTLEMENT_JSON,
    fixture_level_path: Path = FIXTURE_LEVEL_RESEARCH_JSON,
) -> dict[str, Any]:
    generated_at_utc = utc_now()
    ranker = read_json(ranker_path)
    dedupe = read_json(dedupe_path)
    research_settlement = read_json(research_settlement_path)
    fixture_level = read_json(fixture_level_path)

    warnings: list[str] = []
    for name, payload in {
        "paper_alert_ranker": ranker,
        "paper_alert_dedupe": dedupe,
        "research_settlement": research_settlement,
        "fixture_level_research": fixture_level,
    }.items():
        if payload.get("missing"):
            warnings.append(f"MISSING_INPUT:{name}")
        if payload.get("error"):
            warnings.append(f"READ_ERROR:{name}")

    records = signal_records(ranker, dedupe)
    score_index = final_score_index(
        {
            "research_settlement": research_settlement,
            "fixture_level_research": fixture_level,
        }
    )
    rows = [signal_row(record, score_index) for record in records]
    summary = summarize(rows)

    if not records:
        warnings.append("NO_SIGNAL_RECORDS")
    if records and not score_index:
        warnings.append("NO_FINAL_SCORE_ARTIFACTS_AVAILABLE")
    if records and summary["settled_signals"] == 0:
        warnings.append("NO_SETTLED_SIGNAL_ROWS")

    if ranker.get("missing") and dedupe.get("missing"):
        status = "REVIEW"
    elif not records:
        status = "EMPTY"
    elif summary["settled_signals"] == 0:
        status = "REVIEW"
    else:
        status = "READY"

    return {
        "mode": "FQIS_SIGNAL_SETTLEMENT_REPORT",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "source_files": {
            "paper_alert_ranker": str(ranker_path),
            "paper_alert_dedupe": str(dedupe_path),
            "research_settlement": str(research_settlement_path),
            "fixture_level_research": str(fixture_level_path),
        },
        "source_files_used": [str(ranker_path), str(dedupe_path), str(research_settlement_path), str(fixture_level_path)],
        "settlement_scope": "Signal-level paper rows; only Total Goals FT half-goal selections are settled when final score is available.",
        "rows": rows,
        "by_research_bucket": group_summary(rows, "research_bucket"),
        "by_market": group_summary(rows, "market"),
        "by_selection": group_summary(rows, "selection"),
        "by_research_bucket_market_selection": compound_group_summary(rows, ("research_bucket", "market", "selection")),
        "warning_flags": sorted(set(warnings)),
        "safety": dict(SAFETY_BLOCK),
        **summary,
        **SAFETY_BLOCK,
    }


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# FQIS Signal Settlement Report",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION",
        "",
        "## Summary",
        "",
        f"- Status: **{report.get('status')}**",
        f"- Generated at UTC: `{report.get('generated_at_utc')}`",
        f"- Total signals: **{report.get('total_signals', 0)}**",
        f"- Settled signals: **{report.get('settled_signals', 0)}**",
        f"- Unsettled signals: **{report.get('unsettled_signals', 0)}**",
        f"- Win / loss / push / unknown: **{report.get('win_count', 0)} / {report.get('loss_count', 0)} / {report.get('push_count', 0)} / {report.get('unknown_count', 0)}**",
        f"- Paper PnL total: **{report.get('paper_pnl_total', 0)}u**",
        f"- Paper ROI: **{report.get('paper_roi', 0)}**",
        "",
        "## Warning Flags",
        "",
    ]
    flags = report.get("warning_flags") or []
    if flags:
        lines.extend(f"- {safe_text(flag)}" for flag in flags)
    else:
        lines.append("- NONE")

    lines += [
        "",
        "## By Selection",
        "",
        "| Selection | Signals | Settled | Wins | Losses | Pushes | PnL | ROI |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for selection, metrics in (report.get("by_selection") or {}).items():
        lines.append(
            "| {selection} | {total} | {settled} | {wins} | {losses} | {pushes} | {pnl} | {roi} |".format(
                selection=safe_text(selection),
                total=metrics.get("total_signals", 0),
                settled=metrics.get("settled_signals", 0),
                wins=metrics.get("win_count", 0),
                losses=metrics.get("loss_count", 0),
                pushes=metrics.get("push_count", 0),
                pnl=metrics.get("paper_pnl_total", 0),
                roi=metrics.get("paper_roi", 0),
            )
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build FQIS signal-level paper settlement report.")
    parser.add_argument("--ranker-path", default=str(PAPER_ALERT_RANKER_JSON))
    parser.add_argument("--dedupe-path", default=str(PAPER_ALERT_DEDUPE_JSON))
    parser.add_argument("--research-settlement-path", default=str(RESEARCH_SETTLEMENT_JSON))
    parser.add_argument("--fixture-level-path", default=str(FIXTURE_LEVEL_RESEARCH_JSON))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-md", default=str(OUT_MD))
    args = parser.parse_args()

    report = build_report(
        ranker_path=Path(args.ranker_path),
        dedupe_path=Path(args.dedupe_path),
        research_settlement_path=Path(args.research_settlement_path),
        fixture_level_path=Path(args.fixture_level_path),
    )
    write_json_report(report, Path(args.output_json))
    write_markdown_report(report, Path(args.output_md))

    print(
        json.dumps(
            {
                "status": report["status"],
                "total_signals": report["total_signals"],
                "settled_signals": report["settled_signals"],
                "paper_roi": report["paper_roi"],
                "output_json": str(Path(args.output_json)),
                "output_md": str(Path(args.output_md)),
                "can_execute_real_bets": False,
                "can_enable_live_staking": False,
                "can_mutate_ledger": False,
                "promotion_allowed": False,
            },
            indent=2,
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
