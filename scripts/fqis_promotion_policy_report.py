from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
CLV_TRACKER_JSON = ORCH_DIR / "latest_clv_tracker_report.json"
CALIBRATION_JSON = RESEARCH_DIR / "latest_calibration_report.json"
BUCKET_POLICY_JSON = RESEARCH_DIR / "latest_bucket_policy_audit.json"
OUT_JSON = ORCH_DIR / "latest_promotion_policy_report.json"
OUT_MD = ORCH_DIR / "latest_promotion_policy_report.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
}

MIN_SAMPLE_SIZE = 100
MIN_SETTLED_SAMPLE_SIZE = 100
MIN_FAVORABLE_MOVE_RATE = 0.55
MAX_BRIER_SCORE = 0.25
MAX_ABSOLUTE_CALIBRATION_ERROR = 0.10
MIN_ROI = 0.0


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
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def safe_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    return text or default


def records_from_ranker(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for field in ("ranked_alerts", "raw_ranked_alerts", "grouped_ranked_alerts", "top_ranked_alerts"):
        records = payload.get(field)
        if isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]
    return []


def group_key(bucket: str, market: str, selection: str) -> str:
    return f"{safe_text(bucket)}||{safe_text(market)}||{safe_text(selection)}"


def grouped_alerts(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        grouped.setdefault(
            group_key(record.get("research_bucket"), record.get("market"), record.get("selection")),
            [],
        ).append(record)
    return grouped


def collect_group_keys(records: list[dict[str, Any]], clv: dict[str, Any], calibration: dict[str, Any]) -> list[str]:
    keys = set(grouped_alerts(records))
    keys.update((clv.get("by_research_bucket_market_selection") or {}).keys())
    keys.update((calibration.get("by_research_bucket_market_selection") or {}).keys())
    for legacy_key in (clv.get("by_research_bucket_market") or {}).keys():
        keys.add(f"{legacy_key}||UNKNOWN")
    for legacy_key in (calibration.get("by_research_bucket_market") or {}).keys():
        keys.add(f"{legacy_key}||UNKNOWN")
    return sorted(keys)


def split_group_key(key: str) -> tuple[str, str, str]:
    parts = key.split("||")
    while len(parts) < 3:
        parts.append("UNKNOWN")
    bucket, market, selection = parts[:3]
    return bucket or "UNKNOWN", market or "UNKNOWN", selection or "UNKNOWN"


def count_hard_red_flags(records: list[dict[str, Any]]) -> int:
    count = 0
    for record in records:
        red_flags = record.get("red_flags") or []
        if isinstance(red_flags, list):
            count += len(red_flags)
        elif red_flags:
            count += 1
        if record.get("bucket_policy_action") == "KILL_OR_QUARANTINE_BUCKET":
            count += 1
    return count


def data_tier_readiness(records: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for record in records:
        tier = safe_text(record.get("data_tier"))
        counts[tier] = counts.get(tier, 0) + 1
    total = sum(counts.values())
    strict = counts.get("STRICT_EVENTS_PLUS_STATS", 0)
    ready = total > 0 and strict == total
    return {
        "status": "READY" if ready else "NOT_STRICT_READY",
        "total": total,
        "strict_events_plus_stats": strict,
        "events_only_research": counts.get("EVENTS_ONLY_RESEARCH", 0),
        "unknown": counts.get("UNKNOWN", 0),
        "counts": counts,
    }


def bucket_policy_for(bucket: str, bucket_policy: dict[str, Any]) -> dict[str, Any]:
    policies = bucket_policy.get("buckets") or {}
    if not isinstance(policies, dict):
        return {}
    return policies.get(bucket) or {}


def metric_for(payload: dict[str, Any], key: str, bucket: str, market: str, selection: str) -> dict[str, Any] | None:
    if safe_text(selection) != "UNKNOWN":
        table = payload.get("by_research_bucket_market_selection") or {}
        if isinstance(table, dict) and isinstance(table.get(key), dict):
            return table[key]
        return None

    for field, lookup_key in (
        ("by_research_bucket_market", f"{bucket}||{market}"),
        ("by_research_bucket", bucket),
        ("by_market", market),
    ):
        table = payload.get(field) or {}
        if isinstance(table, dict) and isinstance(table.get(lookup_key), dict):
            return table[lookup_key]
    return None


def add_blocker(blockers: list[str], condition: bool, code: str) -> None:
    if condition and code not in blockers:
        blockers.append(code)


def evaluate_group(
    key: str,
    alerts: list[dict[str, Any]],
    clv: dict[str, Any],
    calibration: dict[str, Any],
    bucket_policy: dict[str, Any],
) -> dict[str, Any]:
    bucket, market, selection = split_group_key(key)
    blockers: list[str] = []
    hard_red_flags = count_hard_red_flags(alerts)
    data_tier = data_tier_readiness(alerts)
    policy = bucket_policy_for(bucket, bucket_policy)
    clv_metric = metric_for(clv, key, bucket, market, selection)
    calibration_metric = metric_for(calibration, key, bucket, market, selection)

    clv_status = clv.get("status") if isinstance(clv, dict) else None
    calibration_status = calibration.get("status") if isinstance(calibration, dict) else None
    sample_size = max(
        len(alerts),
        int((clv_metric or {}).get("total_records") or 0),
        int((clv_metric or {}).get("eligible_records") or 0),
    )
    settled_sample_size = int((calibration_metric or {}).get("eligible_settled_rows") or 0)
    roi = fnum(policy.get("roi"))
    favorable_move_rate = fnum((clv_metric or {}).get("favorable_move_rate"))
    clv_eligible = int((clv_metric or {}).get("eligible_records") or 0)
    brier = fnum((calibration_metric or {}).get("brier_score"))
    calibration_error = fnum((calibration_metric or {}).get("absolute_calibration_error"))

    add_blocker(blockers, hard_red_flags > 0, "HARD_RED_FLAGS_PRESENT")
    add_blocker(blockers, policy.get("action") == "KILL_OR_QUARANTINE_BUCKET", "BUCKET_POLICY_QUARANTINE")
    add_blocker(blockers, clv.get("missing") or clv.get("error") or clv_status not in {"READY"}, "PROXY_CLV_MISSING_OR_NOT_READY")
    add_blocker(blockers, clv_metric is None or clv_eligible <= 0, "PROXY_CLV_GROUP_MISSING")
    add_blocker(blockers, calibration.get("missing") or calibration.get("error") or calibration_status not in {"READY"}, "CALIBRATION_MISSING_OR_NOT_READY")
    add_blocker(blockers, calibration_metric is None or settled_sample_size <= 0, "CALIBRATION_GROUP_MISSING")
    add_blocker(blockers, sample_size < MIN_SAMPLE_SIZE, "SAMPLE_SIZE_TOO_SMALL")
    add_blocker(blockers, settled_sample_size < MIN_SETTLED_SAMPLE_SIZE, "SETTLED_SAMPLE_SIZE_TOO_SMALL")
    add_blocker(blockers, roi is None, "ROI_MISSING")
    add_blocker(blockers, roi is not None and roi <= MIN_ROI, "ROI_NOT_POSITIVE")
    add_blocker(blockers, favorable_move_rate is None or favorable_move_rate < MIN_FAVORABLE_MOVE_RATE, "PROXY_CLV_NOT_FAVORABLE_ENOUGH")
    add_blocker(blockers, brier is None or brier > MAX_BRIER_SCORE, "BRIER_SCORE_MISSING_OR_TOO_HIGH")
    add_blocker(
        blockers,
        calibration_error is None or calibration_error > MAX_ABSOLUTE_CALIBRATION_ERROR,
        "CALIBRATION_ERROR_MISSING_OR_TOO_HIGH",
    )
    add_blocker(blockers, data_tier["status"] != "READY", "DATA_TIER_NOT_STRICT_EVENTS_PLUS_STATS")

    paper_elite_candidate = not blockers
    if hard_red_flags > 0 or policy.get("action") == "KILL_OR_QUARANTINE_BUCKET":
        recommended_state = "QUARANTINE"
    elif paper_elite_candidate:
        recommended_state = "PAPER_ELITE_CANDIDATE"
    elif sample_size >= MIN_SAMPLE_SIZE // 2 and clv_metric is not None:
        recommended_state = "WATCHLIST"
    else:
        recommended_state = "KEEP_RESEARCH"

    final_verdict = "PAPER_ELITE_CANDIDATE_REVIEW" if paper_elite_candidate else "NO_PROMOTION_KEEP_RESEARCH"

    return {
        "evaluation_key": key,
        "research_bucket": bucket,
        "market": market,
        "selection": selection,
        "paper_elite_candidate": paper_elite_candidate,
        "promotion_allowed": False,
        "recommended_state": recommended_state,
        "blockers": blockers,
        "sample_size": sample_size,
        "settled_sample_size": settled_sample_size,
        "roi": roi,
        "proxy_clv": clv_metric,
        "calibration_status": calibration_status or "MISSING",
        "calibration": calibration_metric,
        "hard_red_flag_counts": hard_red_flags,
        "data_tier_readiness": data_tier,
        "bucket_policy": policy,
        "final_verdict": final_verdict,
    }


def build_report(
    *,
    ranker_path: Path = PAPER_ALERT_RANKER_JSON,
    clv_path: Path = CLV_TRACKER_JSON,
    calibration_path: Path = CALIBRATION_JSON,
    bucket_policy_path: Path = BUCKET_POLICY_JSON,
) -> dict[str, Any]:
    generated_at_utc = utc_now()
    ranker = read_json(ranker_path)
    clv = read_json(clv_path)
    calibration = read_json(calibration_path)
    bucket_policy = read_json(bucket_policy_path)
    ranker_records = records_from_ranker(ranker)
    alerts_by_group = grouped_alerts(ranker_records)
    keys = collect_group_keys(ranker_records, clv, calibration)

    evaluations = [
        evaluate_group(key, alerts_by_group.get(key, []), clv, calibration, bucket_policy)
        for key in keys
    ]
    paper_elite_candidate_count = sum(1 for item in evaluations if item["paper_elite_candidate"])
    missing_inputs = [
        name
        for name, payload in {
            "paper_alert_ranker": ranker,
            "clv_tracker": clv,
            "calibration": calibration,
            "bucket_policy": bucket_policy,
        }.items()
        if payload.get("missing") or payload.get("error")
    ]
    warning_flags = []
    if missing_inputs:
        warning_flags.append("MISSING_INPUTS:" + ",".join(missing_inputs))
    if not evaluations:
        warning_flags.append("NO_BUCKET_MARKET_EVALUATIONS")

    status = "REVIEW" if missing_inputs or not evaluations else "READY"
    final_verdict = "PAPER_ELITE_CANDIDATE_REVIEW" if paper_elite_candidate_count else "NO_PROMOTION_KEEP_RESEARCH"

    return {
        "mode": "FQIS_PROMOTION_POLICY_REPORT",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "source_files_used": [str(ranker_path), str(clv_path), str(calibration_path), str(bucket_policy_path)],
        "source_files": {
            "paper_alert_ranker": str(ranker_path),
            "clv_tracker": str(clv_path),
            "calibration": str(calibration_path),
            "bucket_policy": str(bucket_policy_path),
        },
        "policy_thresholds": {
            "min_sample_size": MIN_SAMPLE_SIZE,
            "min_settled_sample_size": MIN_SETTLED_SAMPLE_SIZE,
            "min_favorable_move_rate": MIN_FAVORABLE_MOVE_RATE,
            "max_brier_score": MAX_BRIER_SCORE,
            "max_absolute_calibration_error": MAX_ABSOLUTE_CALIBRATION_ERROR,
            "min_roi": MIN_ROI,
        },
        "promotion_allowed": False,
        "promotion_allowed_count": 0,
        "paper_elite_candidate_count": paper_elite_candidate_count,
        "recommended_state_counts": count_values(evaluations, "recommended_state"),
        "final_verdict": final_verdict,
        "evaluations": evaluations,
        "warning_flags": warning_flags,
        "safety": {
            **SAFETY_BLOCK,
        },
        **SAFETY_BLOCK,
        "promotion_allowed": False,
    }


def count_values(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = safe_text(row.get(field))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# FQIS Promotion Policy Report",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION",
        "",
        "## Final Verdict",
        "",
        f"- Status: **{report.get('status')}**",
        f"- Verdict: **{report.get('final_verdict')}**",
        f"- Promotion allowed: **{report.get('promotion_allowed')}**",
        f"- Promotion allowed count: **{report.get('promotion_allowed_count', 0)}**",
        f"- Paper elite candidate count: **{report.get('paper_elite_candidate_count', 0)}**",
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
        "## Evaluations",
        "",
        "| Bucket | Market | Selection | State | Candidate | Allowed | Sample | Settled | ROI | Red flags | Blockers |",
        "|---|---|---|---|---|---|---:|---:|---:|---:|---|",
    ]
    for item in report.get("evaluations") or []:
        lines.append(
            "| {bucket} | {market} | {selection} | {state} | {candidate} | {allowed} | {sample} | {settled} | {roi} | {flags} | {blockers} |".format(
                bucket=safe_text(item.get("research_bucket")),
                market=safe_text(item.get("market")),
                selection=safe_text(item.get("selection")),
                state=safe_text(item.get("recommended_state")),
                candidate=item.get("paper_elite_candidate"),
                allowed=item.get("promotion_allowed"),
                sample=item.get("sample_size", 0),
                settled=item.get("settled_sample_size", 0),
                roi=item.get("roi"),
                flags=item.get("hard_red_flag_counts", 0),
                blockers=", ".join(item.get("blockers") or []) or "NONE",
            )
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build strict FQIS paper promotion governance report.")
    parser.add_argument("--ranker-path", default=str(PAPER_ALERT_RANKER_JSON))
    parser.add_argument("--clv-path", default=str(CLV_TRACKER_JSON))
    parser.add_argument("--calibration-path", default=str(CALIBRATION_JSON))
    parser.add_argument("--bucket-policy-path", default=str(BUCKET_POLICY_JSON))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-md", default=str(OUT_MD))
    args = parser.parse_args()

    report = build_report(
        ranker_path=Path(args.ranker_path),
        clv_path=Path(args.clv_path),
        calibration_path=Path(args.calibration_path),
        bucket_policy_path=Path(args.bucket_policy_path),
    )
    write_json_report(report, Path(args.output_json))
    write_markdown_report(report, Path(args.output_md))

    print(json.dumps({
        "status": report["status"],
        "promotion_allowed": report["promotion_allowed"],
        "final_verdict": report["final_verdict"],
        "output_json": str(Path(args.output_json)),
        "output_md": str(Path(args.output_md)),
        "can_execute_real_bets": False,
        "can_enable_live_staking": False,
        "can_mutate_ledger": False,
    }, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
