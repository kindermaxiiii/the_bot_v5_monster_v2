from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
PAPER_ALERT_DEDUPE_JSON = ORCH_DIR / "latest_paper_alert_dedupe.json"
GO_NO_GO_JSON = ORCH_DIR / "latest_go_no_go_report.json"
SHADOW_READINESS_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
OUT_JSON = ORCH_DIR / "latest_operator_paper_decision_sheet.json"
OUT_MD = ORCH_DIR / "latest_operator_paper_decision_sheet.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def truthy_flag(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


def find_true_flags(payload: Any, names: set[str], prefix: str = "") -> list[str]:
    flags: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if str(key) in names and truthy_flag(value):
                flags.append(path)
            flags.extend(find_true_flags(value, names, path))
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            flags.extend(find_true_flags(item, names, f"{prefix}[{index}]"))
    return flags


def safe_text(value: Any) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ").strip()


def fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return safe_text(value)


def table_row(alert: dict[str, Any]) -> dict[str, Any]:
    return {
        "Rank": alert.get("rank"),
        "Fixture": alert.get("fixture_id"),
        "Match": alert.get("match") or alert.get("fixture_id"),
        "Minute": alert.get("minute"),
        "Score": alert.get("score"),
        "Market": alert.get("market"),
        "Selection": alert.get("selection"),
        "Odds latest": alert.get("odds_latest", alert.get("odds")),
        "Edge latest": alert.get("edge_latest", alert.get("edge_prob")),
        "EV latest": alert.get("ev_latest", alert.get("ev_real")),
        "Tier": alert.get("data_tier"),
        "Bucket action": alert.get("bucket_policy_action"),
        "Lifecycle": alert.get("alert_lifecycle_status"),
        "Operator note": alert.get("operator_note"),
    }


def inspect_next(alerts: list[dict[str, Any]], freshness: dict[str, Any], ranker: dict[str, Any]) -> list[str]:
    warning = "PAPER ONLY / NO REAL BET / NO STAKE / NO EXECUTION."
    items: list[str] = []
    if alerts:
        items.append(f"Inspect rank 1 through {min(10, len(alerts))} for data tier, bucket action, odds sanity, and red flags. {warning}")
    else:
        items.append(f"No useful ranked paper alerts; inspect rejected / blocked summary before acting. {warning}")

    live_flags = [str(flag) for flag in freshness.get("freshness_flags") or [] if flag != "OK_FRESH_LIVE_CYCLE"]
    historical_flags = [str(flag) for flag in freshness.get("historical_metric_static_review") or []]
    if live_flags:
        items.append(f"Live freshness review flags present: {', '.join(live_flags)}. {warning}")
    elif historical_flags:
        items.append(f"Historical static review only: {', '.join(historical_flags)}. {warning}")
    else:
        items.append(f"Live freshness is clean for paper review. {warning}")

    if (ranker.get("rejected_blocked_summary") or {}).get("rejected_count"):
        items.append(f"Check rejected buckets for recurring model/data issues. {warning}")
    return items


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    ranker = read_json(PAPER_ALERT_RANKER_JSON)
    dedupe = read_json(PAPER_ALERT_DEDUPE_JSON)
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow = read_json(SHADOW_READINESS_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)

    inputs = {
        "paper_alert_ranker": ranker,
        "paper_alert_dedupe": dedupe,
        "go_no_go": go_no_go,
        "shadow_readiness": shadow,
        "live_freshness": freshness,
    }

    reasons: list[str] = []
    for name, payload in inputs.items():
        if payload.get("missing") or payload.get("error"):
            reasons.append(f"MISSING_INPUT:{name}")

    if ranker.get("status") == "BLOCKED":
        reasons.append("PAPER_ALERT_RANKER_BLOCKED")
    if dedupe.get("status") == "BLOCKED":
        reasons.append("PAPER_ALERT_DEDUPE_BLOCKED")
    if go_no_go.get("status") != "READY":
        reasons.append("GO_NO_GO_NOT_READY")
    if shadow.get("status") != "READY" or shadow.get("shadow_state") != "SHADOW_READY":
        reasons.append("SHADOW_READINESS_NOT_READY")
    if freshness.get("status") == "MISSING_INPUTS":
        reasons.append("LIVE_FRESHNESS_MISSING_INPUTS")

    unsafe_names = {
        "live_staking_allowed",
        "level3_live_staking_allowed",
        "can_execute_real_bets",
        "can_enable_live_staking",
        "can_mutate_ledger",
        "promotion_allowed",
    }
    unsafe_hits: list[str] = []
    for name, payload in inputs.items():
        unsafe_hits.extend(f"{name}:{hit}" for hit in find_true_flags(payload, unsafe_names))
    if unsafe_hits:
        reasons.append("UNSAFE_TRUE_FLAGS:" + ",".join(unsafe_hits[:20]))

    ranked_alerts = ranker.get("raw_ranked_alerts") or ranker.get("ranked_alerts") or []
    if not isinstance(ranked_alerts, list):
        ranked_alerts = []
        reasons.append("RANKED_ALERTS_NOT_LIST")
    ranked_alerts = [alert for alert in ranked_alerts if isinstance(alert, dict)]
    grouped_alerts = ranker.get("grouped_ranked_alerts") or ranker.get("top_ranked_alerts") or ranked_alerts
    if not isinstance(grouped_alerts, list):
        grouped_alerts = []
        reasons.append("GROUPED_ALERTS_NOT_LIST")
    grouped_alerts = [alert for alert in grouped_alerts if isinstance(alert, dict)]
    top_alerts = grouped_alerts[:10]
    table = [table_row(alert) for alert in top_alerts]

    safety_state = {
        "go_no_go_state": go_no_go.get("go_no_go_state"),
        "shadow_state": shadow.get("shadow_state"),
        "promotion_allowed": go_no_go.get("promotion_allowed"),
        "live_staking_allowed": go_no_go.get("live_staking_allowed"),
        "can_execute_real_bets": shadow.get("can_execute_real_bets") or ranker.get("can_execute_real_bets"),
        "can_enable_live_staking": shadow.get("can_enable_live_staking") or ranker.get("can_enable_live_staking"),
        "can_mutate_ledger": shadow.get("can_mutate_ledger") or ranker.get("can_mutate_ledger"),
        "paper_only": True,
    }

    status = "BLOCKED" if reasons else "READY"
    return {
        "mode": "FQIS_OPERATOR_PAPER_DECISION_SHEET",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "warning": "PAPER ONLY / NO REAL BET / NO STAKE / NO EXECUTION",
        "reasons": reasons,
        "safety": dict(SAFETY_BLOCK),
        "source_files": {
            "paper_alert_ranker": str(PAPER_ALERT_RANKER_JSON),
            "paper_alert_dedupe": str(PAPER_ALERT_DEDUPE_JSON),
            "go_no_go": str(GO_NO_GO_JSON),
            "shadow_readiness": str(SHADOW_READINESS_JSON),
            "live_freshness": str(LIVE_FRESHNESS_JSON),
        },
        "top_ranked_alert_count": len(top_alerts),
        "ranked_alert_count": len(ranked_alerts),
        "raw_ranked_alert_count": len(ranked_alerts),
        "grouped_ranked_alert_count": len(grouped_alerts),
        "new_ranked_alert_count": ranker.get("new_ranked_alert_count") or 0,
        "updated_ranked_alert_count": ranker.get("updated_ranked_alert_count") or 0,
        "repeated_ranked_alert_count": ranker.get("repeated_ranked_alert_count") or 0,
        "new_paper_alerts": dedupe.get("new_alerts") or 0,
        "raw_new_paper_alerts": dedupe.get("raw_new_alerts") or dedupe.get("new_alerts") or 0,
        "new_canonical_alerts": dedupe.get("new_canonical_alerts") or 0,
        "updated_canonical_alerts": dedupe.get("updated_canonical_alerts") or 0,
        "repeated_canonical_alerts": dedupe.get("repeated_canonical_alerts") or 0,
        "material_updates": dedupe.get("material_updates") or 0,
        "repeated_paper_alerts": dedupe.get("repeated_alerts") or 0,
        "top_paper_alerts": top_alerts,
        "operator_table": table,
        "rejected_blocked_summary": ranker.get("rejected_blocked_summary") or {},
        "safety_state": safety_state,
        "freshness_state": {
            "status": freshness.get("status"),
            "freshness_flags": freshness.get("freshness_flags") or [],
            "historical_metric_static_review": freshness.get("historical_metric_static_review") or [],
            "decisions_total": freshness.get("decisions_total"),
            "candidates_this_cycle": freshness.get("candidates_this_cycle"),
            "new_snapshots_appended": freshness.get("new_snapshots_appended"),
        },
        "what_to_inspect_next": inspect_next(top_alerts, freshness, ranker),
        "unsafe_flag_paths": unsafe_hits,
        **SAFETY_BLOCK,
    }


def write_markdown(payload: dict[str, Any]) -> None:
    safety = payload.get("safety_state") or {}
    freshness = payload.get("freshness_state") or {}
    rejected = payload.get("rejected_blocked_summary") or {}
    lines = [
        "# FQIS Operator Paper Decision Sheet",
        "",
        "## PAPER ONLY WARNING",
        "",
        "**PAPER ONLY / NO REAL BET / NO STAKE / NO EXECUTION**",
        "",
        "## Top paper alerts",
        "",
        f"- Raw ranked alert count: **{payload.get('raw_ranked_alert_count', payload.get('ranked_alert_count', 0))}**",
        f"- Grouped ranked alert count: **{payload.get('grouped_ranked_alert_count', 0)}**",
        "",
        "| Rank | Fixture | Match | Minute | Score | Market | Selection | Odds latest | EV latest | Edge latest | Tier | Bucket action | Lifecycle | Operator note |",
        "|---:|---|---|---:|---|---|---|---:|---:|---:|---|---|---|---|",
    ]

    rows = payload.get("operator_table") or []
    if not rows:
        lines.append("|  |  | No useful ranked paper alerts |  |  |  |  |  |  |  |  |  |  | PAPER ONLY / NO REAL BET / NO STAKE / NO EXECUTION |")
    else:
        for row in rows:
            lines.append(
                "| {Rank} | {Fixture} | {Match} | {Minute} | {Score} | {Market} | {Selection} | {Odds_latest} | {EV_latest} | {Edge_latest} | {Tier} | {Bucket_action} | {Lifecycle} | {Operator_note} |".format(
                    Rank=fmt(row.get("Rank")),
                    Fixture=fmt(row.get("Fixture")),
                    Match=fmt(row.get("Match")),
                    Minute=fmt(row.get("Minute")),
                    Score=fmt(row.get("Score")),
                    Market=fmt(row.get("Market")),
                    Selection=fmt(row.get("Selection")),
                    Odds_latest=fmt(row.get("Odds latest")),
                    Edge_latest=fmt(row.get("Edge latest")),
                    EV_latest=fmt(row.get("EV latest")),
                    Tier=fmt(row.get("Tier")),
                    Bucket_action=fmt(row.get("Bucket action")),
                    Lifecycle=fmt(row.get("Lifecycle")),
                    Operator_note=fmt(row.get("Operator note")),
                )
            )

    lines += [
        "",
        "## Rejected / blocked summary",
        "",
        f"- rejected_count: **{rejected.get('rejected_count', 0)}**",
    ]
    for item in rejected.get("top_rejection_reasons") or []:
        lines.append(f"- {item.get('count')}: {safe_text(item.get('reason'))}")

    lines += [
        "",
        "## Safety state",
        "",
        f"- Go/no-go: **{safety.get('go_no_go_state')}**",
        f"- Shadow: **{safety.get('shadow_state')}**",
        f"- Promotion allowed: **{safety.get('promotion_allowed')}**",
        f"- Live staking allowed: **{safety.get('live_staking_allowed')}**",
        f"- Can execute real bets: **{safety.get('can_execute_real_bets')}**",
        f"- Can enable live staking: **{safety.get('can_enable_live_staking')}**",
        f"- Can mutate ledger: **{safety.get('can_mutate_ledger')}**",
        "",
        "## Freshness state",
        "",
        f"- Status: **{freshness.get('status')}**",
        f"- Live freshness flags: **{', '.join(str(flag) for flag in freshness.get('freshness_flags') or []) or 'NONE'}**",
        f"- Historical static review: **{', '.join(str(flag) for flag in freshness.get('historical_metric_static_review') or []) or 'NONE'}**",
        f"- Decisions total: **{freshness.get('decisions_total')}**",
        f"- Candidates this cycle: **{freshness.get('candidates_this_cycle')}**",
        f"- New snapshots appended: **{freshness.get('new_snapshots_appended')}**",
        "",
        "## What to inspect next",
        "",
    ]
    for item in payload.get("what_to_inspect_next") or []:
        lines.append(f"- {safe_text(item)}")

    if payload.get("reasons"):
        lines += ["", "## Block reasons", ""]
        lines.extend(f"- {safe_text(reason)}" for reason in payload.get("reasons") or [])

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
    return 0 if payload["status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
