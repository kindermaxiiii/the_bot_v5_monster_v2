from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
OUT_JSON = ORCH_DIR / "latest_clv_tracker_report.json"
OUT_MD = ORCH_DIR / "latest_clv_tracker_report.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
}

INSUFFICIENT_SAMPLE_THRESHOLD = 30


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {"error": "JSON_ROOT_NOT_OBJECT", "path": str(path)}
        return payload
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(str(value).replace(",", ".").strip())
    except Exception:
        return None
    if number <= 1.0:
        return None
    return number


def safe_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    return text or default


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def implied_probability(odds: float) -> float:
    return 1.0 / odds


def empty_summary(total_records: int = 0) -> dict[str, Any]:
    return {
        "total_records": total_records,
        "eligible_records": 0,
        "odds_first_mean": None,
        "odds_latest_mean": None,
        "odds_delta_mean": None,
        "odds_delta_pct_mean": None,
        "implied_first_mean": None,
        "implied_latest_mean": None,
        "implied_delta_mean": None,
        "favorable_move_count": 0,
        "unfavorable_move_count": 0,
        "flat_move_count": 0,
        "favorable_move_rate": 0.0,
        "warning_flags": ["NO_ELIGIBLE_ODDS_MOVEMENT_RECORDS"],
    }


def movement_record(record: dict[str, Any]) -> dict[str, Any] | None:
    odds_first = fnum(record.get("odds_first"))
    odds_latest = fnum(record.get("odds_latest"))
    if odds_first is None or odds_latest is None:
        return None

    odds_delta = odds_latest - odds_first
    odds_delta_pct = odds_delta / odds_first
    implied_first = implied_probability(odds_first)
    implied_latest = implied_probability(odds_latest)
    implied_delta = implied_latest - implied_first

    if abs(odds_delta) < 1e-12:
        direction = "FLAT"
    elif odds_latest < odds_first:
        direction = "FAVORABLE"
    else:
        direction = "UNFAVORABLE"

    return {
        "alert_key": record.get("alert_key"),
        "canonical_alert_key": record.get("canonical_alert_key"),
        "fixture_id": record.get("fixture_id"),
        "market": record.get("market"),
        "selection": record.get("selection"),
        "research_bucket": record.get("research_bucket"),
        "minute_bucket": record.get("minute_bucket"),
        "bucket_policy_action": record.get("bucket_policy_action"),
        "odds_first": round(odds_first, 6),
        "odds_latest": round(odds_latest, 6),
        "odds_delta": round(odds_delta, 6),
        "odds_delta_pct": round(odds_delta_pct, 6),
        "implied_first": round(implied_first, 6),
        "implied_latest": round(implied_latest, 6),
        "implied_delta": round(implied_delta, 6),
        "movement_direction": direction,
    }


def summarize_movements(records: list[dict[str, Any]], *, total_records: int | None = None) -> dict[str, Any]:
    total = len(records) if total_records is None else total_records
    if not records:
        return empty_summary(total)

    favorable = sum(1 for record in records if record["movement_direction"] == "FAVORABLE")
    unfavorable = sum(1 for record in records if record["movement_direction"] == "UNFAVORABLE")
    flat = sum(1 for record in records if record["movement_direction"] == "FLAT")
    warning_flags: list[str] = []
    if len(records) < INSUFFICIENT_SAMPLE_THRESHOLD:
        warning_flags.append(f"INSUFFICIENT_SAMPLE:{len(records)}<{INSUFFICIENT_SAMPLE_THRESHOLD}")

    return {
        "total_records": total,
        "eligible_records": len(records),
        "odds_first_mean": mean([float(record["odds_first"]) for record in records]),
        "odds_latest_mean": mean([float(record["odds_latest"]) for record in records]),
        "odds_delta_mean": mean([float(record["odds_delta"]) for record in records]),
        "odds_delta_pct_mean": mean([float(record["odds_delta_pct"]) for record in records]),
        "implied_first_mean": mean([float(record["implied_first"]) for record in records]),
        "implied_latest_mean": mean([float(record["implied_latest"]) for record in records]),
        "implied_delta_mean": mean([float(record["implied_delta"]) for record in records]),
        "favorable_move_count": favorable,
        "unfavorable_move_count": unfavorable,
        "flat_move_count": flat,
        "favorable_move_rate": round(favorable / len(records), 6) if records else 0.0,
        "warning_flags": warning_flags,
    }


def group_summary(records: list[dict[str, Any]], key_name: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        key = safe_text(record.get(key_name))
        grouped.setdefault(key, []).append(record)
    return {
        key: summarize_movements(group_records)
        for key, group_records in sorted(grouped.items())
    }


def compound_group_summary(records: list[dict[str, Any]], key_names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        key = "||".join(safe_text(record.get(key_name)) for key_name in key_names)
        grouped.setdefault(key, []).append(record)
    return {
        key: summarize_movements(group_records)
        for key, group_records in sorted(grouped.items())
    }


def ranker_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for field in ("ranked_alerts", "raw_ranked_alerts", "grouped_ranked_alerts", "top_ranked_alerts"):
        records = payload.get(field)
        if isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]
    return []


def build_report(input_path: Path = PAPER_ALERT_RANKER_JSON) -> dict[str, Any]:
    generated_at_utc = utc_now()
    source = read_json(input_path)
    warnings: list[str] = []

    if source.get("missing"):
        records: list[dict[str, Any]] = []
        warnings.append("MISSING_PAPER_ALERT_RANKER")
    elif source.get("error"):
        records = []
        warnings.append("PAPER_ALERT_RANKER_READ_ERROR")
    else:
        records = ranker_records(source)
        if not records:
            warnings.append("NO_PAPER_ALERT_RECORDS")

    movement_records = [
        movement
        for record in records
        if (movement := movement_record(record)) is not None
    ]
    summary = summarize_movements(movement_records, total_records=len(records))
    warning_flags = sorted(set([*warnings, *summary["warning_flags"]]))

    if source.get("missing") or source.get("error"):
        status = "REVIEW"
    elif not records:
        status = "EMPTY"
    elif not movement_records:
        status = "REVIEW"
    else:
        status = "READY"

    return {
        "mode": "FQIS_PROXY_CLV_TRACKER_REPORT",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "description": "Observed odds movement / proxy CLV for paper alerts. This is not official closing-line value unless true closing odds are supplied.",
        "source_files_used": [str(input_path)],
        "source_files": {
            "paper_alert_ranker": str(input_path),
        },
        "total_records": len(records),
        "eligible_records": len(movement_records),
        "by_market": group_summary(movement_records, "market"),
        "by_selection": group_summary(movement_records, "selection"),
        "by_research_bucket": group_summary(movement_records, "research_bucket"),
        "by_minute_bucket": group_summary(movement_records, "minute_bucket"),
        "by_bucket_policy_action": group_summary(movement_records, "bucket_policy_action"),
        "by_research_bucket_market": compound_group_summary(movement_records, ("research_bucket", "market")),
        "by_research_bucket_market_selection": compound_group_summary(
            movement_records,
            ("research_bucket", "market", "selection"),
        ),
        "by_research_bucket_market_selection_minute_bucket": compound_group_summary(
            movement_records,
            ("research_bucket", "market", "selection", "minute_bucket"),
        ),
        "movement_records": movement_records,
        **summary,
        "warning_flags": warning_flags,
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
    }


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# FQIS Proxy CLV Tracker",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION",
        "",
        "> Observed odds movement / proxy CLV. Favorable means latest observed odds are lower than first observed odds for the same paper signal.",
        "",
        "## Summary",
        "",
        f"- Status: **{report.get('status')}**",
        f"- Generated at UTC: `{report.get('generated_at_utc')}`",
        f"- Total records: **{report.get('total_records', 0)}**",
        f"- Eligible records: **{report.get('eligible_records', 0)}**",
        f"- Favorable / unfavorable / flat: **{report.get('favorable_move_count', 0)} / {report.get('unfavorable_move_count', 0)} / {report.get('flat_move_count', 0)}**",
        f"- Favorable move rate: **{float(report.get('favorable_move_rate') or 0.0):.2%}**",
        f"- Odds first mean: **{report.get('odds_first_mean')}**",
        f"- Odds latest mean: **{report.get('odds_latest_mean')}**",
        f"- Odds delta mean: **{report.get('odds_delta_mean')}**",
        f"- Implied delta mean: **{report.get('implied_delta_mean')}**",
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
        "## By Research Bucket",
        "",
        "| Bucket | Eligible | Favorable | Unfavorable | Flat | Favorable rate | Odds delta mean |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]

    for bucket, metrics in (report.get("by_research_bucket") or {}).items():
        lines.append(
            "| {bucket} | {eligible} | {fav} | {unfav} | {flat} | {rate:.2%} | {delta} |".format(
                bucket=safe_text(bucket),
                eligible=metrics.get("eligible_records", 0),
                fav=metrics.get("favorable_move_count", 0),
                unfav=metrics.get("unfavorable_move_count", 0),
                flat=metrics.get("flat_move_count", 0),
                rate=float(metrics.get("favorable_move_rate") or 0.0),
                delta=metrics.get("odds_delta_mean"),
            )
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build FQIS paper-alert observed odds movement / proxy CLV report.")
    parser.add_argument("--input-path", default=str(PAPER_ALERT_RANKER_JSON))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-md", default=str(OUT_MD))
    args = parser.parse_args()

    report = build_report(Path(args.input_path))
    write_json_report(report, Path(args.output_json))
    write_markdown_report(report, Path(args.output_md))

    print(json.dumps({
        "status": report["status"],
        "total_records": report["total_records"],
        "eligible_records": report["eligible_records"],
        "output_json": str(Path(args.output_json)),
        "output_md": str(Path(args.output_md)),
        "can_execute_real_bets": False,
        "can_enable_live_staking": False,
        "can_mutate_ledger": False,
        "promotion_allowed": False,
    }, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
