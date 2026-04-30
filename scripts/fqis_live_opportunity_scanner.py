from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
DECISION_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
LEVEL3_DIR = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

FULL_CYCLE_JSON = ORCH_DIR / "latest_full_cycle_report.json"
LIVE_FRESHNESS_JSON = ORCH_DIR / "latest_live_freshness_report.json"
PAPER_SIGNAL_EXPORT_JSON = ORCH_DIR / "latest_paper_signal_export.json"
PAPER_ALERT_RANKER_JSON = ORCH_DIR / "latest_paper_alert_ranker.json"
GO_NO_GO_JSON = ORCH_DIR / "latest_go_no_go_report.json"
SHADOW_READINESS_JSON = ORCH_DIR / "latest_shadow_readiness_report.json"
LEVEL3_STATS_COVERAGE_DIAGNOSTIC_JSON = ORCH_DIR / "latest_level3_stats_coverage_diagnostic.json"

LIVE_DECISIONS_JSON = DECISION_DIR / "latest_live_decisions.json"
OPERATOR_REPORT_JSON = DECISION_DIR / "latest_operator_report.json"
FINAL_PIPELINE_AUDIT_JSON = DECISION_DIR / "latest_final_pipeline_audit.json"
LEVEL3_STATE_JSON = LEVEL3_DIR / "latest_level3_live_state.json"
RESEARCH_CANDIDATES_JSON = RESEARCH_DIR / "latest_research_candidates.json"

OUT_JSON = ORCH_DIR / "latest_live_opportunity_scanner.json"
OUT_MD = ORCH_DIR / "latest_live_opportunity_scanner.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
}

OPERATOR_READS = {
    "LIVE_WINDOW_EMPTY",
    "SCORE_ONLY_NO_LEVEL3_STATE",
    "NEGATIVE_VALUE_ONLY",
    "FILTERS_TOO_STRICT_REVIEW",
    "DATA_PROVIDER_COVERAGE_REVIEW",
    "HEALTHY_NO_VALUE_WINDOW",
    "EVENTS_ONLY_RESEARCH_NO_STATS",
    "UNKNOWN_REVIEW",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        return {"payload": payload, "non_object_json": True, "path": str(path)}
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def int_value(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).replace(",", ".").strip()))
    except Exception:
        return default


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def present(value: Any) -> bool:
    return value is not None and value != ""


def first_present(*values: Any, default: Any = None) -> Any:
    for value in values:
        if present(value):
            return value
    return default


def safe_text(value: Any) -> str:
    return str(value or "").replace("|", "/").replace("\n", " ").strip()


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


def reports_payload(full_cycle: dict[str, Any], name: str) -> dict[str, Any]:
    reports = full_cycle.get("reports") or {}
    payload = reports.get(name) or {}
    return payload if isinstance(payload, dict) else {}


def summary(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("summary") or {}
    return value if isinstance(value, dict) else {}


def compact_stats_coverage_diagnostic(payload: dict[str, Any]) -> dict[str, Any]:
    diag_summary = summary(payload)
    return {
        "status": payload.get("status"),
        "fixtures_seen": int_value(diag_summary.get("fixtures_seen")),
        "events_available": int_value(diag_summary.get("events_available")),
        "raw_stats_available": int_value(diag_summary.get("raw_stats_available")),
        "parsed_stats_available": int_value(diag_summary.get("parsed_stats_available")),
        "events_only_no_stats": int_value(diag_summary.get("events_only_no_stats")),
        "stats_parser_empty": int_value(diag_summary.get("stats_parser_empty")),
        "stats_endpoint_missing": int_value(diag_summary.get("stats_endpoint_missing")),
    }


def decisions_from(payload: dict[str, Any]) -> list[dict[str, Any]]:
    decisions = payload.get("decisions") or []
    if not isinstance(decisions, list):
        return []
    return [decision for decision in decisions if isinstance(decision, dict)]


def final_operational_status(decision: dict[str, Any]) -> str:
    if decision.get("vetoes"):
        return "NO_BET"
    return str(decision.get("real_status") or "NO_BET")


def has_negative_value_veto(decision: dict[str, Any]) -> bool:
    return any(str(veto) == "non_positive_edge" for veto in decision.get("vetoes") or [])


def research_data_tier(decision: dict[str, Any]) -> str:
    payload = decision.get("payload") or {}
    mode = str(payload.get("level3_data_mode") or "")
    if payload.get("level3_trade_ready") is True and mode == "EVENTS_PLUS_STATS":
        return "STRICT_EVENTS_PLUS_STATS"
    if payload.get("level3_state_ready") is True and mode == "EVENTS_ONLY":
        return "EVENTS_ONLY_RESEARCH"
    return "REJECTED_DATA_TIER"


def passes_research_timing_policy(decision: dict[str, Any]) -> bool:
    minute = fnum(decision.get("minute"), 0.0)
    side = str(decision.get("side") or "").upper()
    line = fnum(decision.get("line"), 0.0)

    if minute <= 0:
        return False

    if side == "UNDER":
        if minute < 8:
            return False
        if line <= 0.5 and minute < 55:
            return False
        if line <= 1.5 and minute < 45:
            return False
        if line <= 2.5 and minute < 20:
            return False

    if side == "OVER" and minute < 8:
        return False

    return True


def computed_research_rejections(decisions: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "final_status_rejected": 0,
        "non_positive_edge_or_ev_rejected": 0,
        "timing_policy_rejected": 0,
        "negative_value_veto_rejected": 0,
        "data_tier_rejected": 0,
    }

    for decision in decisions:
        if final_operational_status(decision) != "NO_BET":
            counts["final_status_rejected"] += 1
            continue
        if fnum(decision.get("edge")) <= 0 or fnum(decision.get("expected_value")) <= 0:
            counts["non_positive_edge_or_ev_rejected"] += 1
            continue
        if not passes_research_timing_policy(decision):
            counts["timing_policy_rejected"] += 1
            continue
        if has_negative_value_veto(decision):
            counts["negative_value_veto_rejected"] += 1
            continue
        if research_data_tier(decision) == "REJECTED_DATA_TIER":
            counts["data_tier_rejected"] += 1
            continue

    return counts


def decision_rejection_reason(decision: dict[str, Any]) -> str:
    payload = decision.get("payload") or {}
    reasons = [
        payload.get("final_pipeline_reason"),
        payload.get("level3_route_reason"),
        payload.get("primary_veto"),
        ", ".join(str(veto) for veto in decision.get("vetoes") or []),
    ]
    cleaned = [safe_text(reason) for reason in reasons if safe_text(reason)]
    return "; ".join(dict.fromkeys(cleaned)) or "UNKNOWN"


def computed_top_rejection_reasons(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[str] = Counter()
    for decision in decisions:
        payload = decision.get("payload") or {}
        final_pipeline = str(payload.get("final_pipeline") or payload.get("level3_pipeline") or "").lower()
        if final_pipeline == "reject" or decision.get("publication_status") == "BLOCKED" or decision.get("vetoes"):
            counter[decision_rejection_reason(decision)] += 1

    return [
        {"reason": reason, "count": count}
        for reason, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]


def fallback_rejection_reasons(paper_ranker: dict[str, Any]) -> list[dict[str, Any]]:
    rejected = paper_ranker.get("rejected_blocked_summary") or {}
    reasons = rejected.get("top_rejection_reasons") or []
    if not isinstance(reasons, list):
        return []
    return [item for item in reasons if isinstance(item, dict)]


def level3_counts_from_decisions(decisions: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "state_ready": 0,
        "trade_ready": 0,
        "events_available": 0,
        "stats_available": 0,
        "score_only": 0,
    }
    for decision in decisions:
        payload = decision.get("payload") or {}
        if payload.get("level3_state_ready") is True:
            counts["state_ready"] += 1
        if payload.get("level3_trade_ready") is True:
            counts["trade_ready"] += 1
        if payload.get("level3_events_available") is True:
            counts["events_available"] += 1
        if payload.get("level3_stats_available") is True:
            counts["stats_available"] += 1
        if str(payload.get("level3_data_mode") or payload.get("level3_gate_state") or "").upper() == "SCORE_ONLY":
            counts["score_only"] += 1
    return counts


def classify_operator_read(metrics: dict[str, Any]) -> str:
    live_fixtures_seen = int_value(metrics.get("live_fixtures_seen"))
    groups_total = int_value(metrics.get("groups_total"))
    decisions_total = int_value(metrics.get("decisions_total"))
    candidates_this_cycle = int_value(metrics.get("candidates_this_cycle"))
    official_decisions = int_value(metrics.get("official_decisions"))
    watchlist_decisions = int_value(metrics.get("watchlist_decisions"))
    score_only_decisions = int_value(metrics.get("score_only_decisions"))
    level3_state_ready_count = int_value(metrics.get("level3_state_ready_count"))
    level3_trade_ready_count = int_value(metrics.get("level3_trade_ready_count"))
    level3_events_available_count = int_value(metrics.get("level3_events_available_count"))
    level3_stats_available_count = int_value(metrics.get("level3_stats_available_count"))
    rejected_by_non_positive_edge_ev = int_value(metrics.get("rejected_by_non_positive_edge_ev"))
    rejected_by_timing_policy = int_value(metrics.get("rejected_by_timing_policy"))
    rejected_by_data_tier = int_value(metrics.get("rejected_by_data_tier"))
    rejected_by_final_status = int_value(metrics.get("rejected_by_final_status"))
    rejected_by_negative_value_veto = int_value(metrics.get("rejected_by_negative_value_veto"))

    if live_fixtures_seen <= 0 and groups_total <= 0 and decisions_total <= 0:
        return "LIVE_WINDOW_EMPTY"

    if live_fixtures_seen > 0 and groups_total <= 0 and decisions_total <= 0:
        return "DATA_PROVIDER_COVERAGE_REVIEW"

    if (
        candidates_this_cycle > 0
        and level3_state_ready_count > 0
        and level3_events_available_count > 0
        and level3_trade_ready_count == 0
        and level3_stats_available_count == 0
    ):
        return "EVENTS_ONLY_RESEARCH_NO_STATS"

    if decisions_total > 0 and candidates_this_cycle == 0 and score_only_decisions > 0 and level3_state_ready_count == 0:
        return "SCORE_ONLY_NO_LEVEL3_STATE"

    if (
        decisions_total > 0
        and candidates_this_cycle == 0
        and rejected_by_non_positive_edge_ev >= decisions_total
        and rejected_by_timing_policy == 0
        and rejected_by_data_tier == 0
        and rejected_by_final_status == 0
        and rejected_by_negative_value_veto == 0
    ):
        return "NEGATIVE_VALUE_ONLY"

    if (
        decisions_total > 0
        and candidates_this_cycle == 0
        and (
            rejected_by_data_tier > 0
            or level3_state_ready_count == 0
            or level3_events_available_count == 0
            or level3_stats_available_count == 0
        )
    ):
        return "DATA_PROVIDER_COVERAGE_REVIEW"

    if (
        decisions_total > 0
        and candidates_this_cycle == 0
        and (rejected_by_timing_policy > 0 or rejected_by_final_status > 0 or rejected_by_negative_value_veto > 0)
    ):
        return "FILTERS_TOO_STRICT_REVIEW"

    if (
        decisions_total > 0
        and candidates_this_cycle == 0
        and official_decisions == 0
        and watchlist_decisions == 0
        and level3_trade_ready_count > 0
    ):
        return "HEALTHY_NO_VALUE_WINDOW"

    return "UNKNOWN_REVIEW"


def merged_summary(
    *,
    full_cycle: dict[str, Any],
    live_decisions: dict[str, Any],
    research_candidates: dict[str, Any],
    level3_state: dict[str, Any],
    freshness: dict[str, Any],
    final_pipeline_audit: dict[str, Any],
    paper_ranker: dict[str, Any],
) -> dict[str, Any]:
    live_summary = summary(live_decisions) or summary(reports_payload(full_cycle, "live_decisions"))
    research_summary = summary(research_candidates) or summary(reports_payload(full_cycle, "research_candidates"))
    level3_summary = summary(level3_state)
    daily_audit = reports_payload(full_cycle, "daily_audit")
    daily_level3 = daily_audit.get("level3_summary") or {}
    daily_diag = daily_audit.get("research_diagnostics") or {}
    freshness_report = freshness if isinstance(freshness, dict) else {}
    final_audit = final_pipeline_audit or reports_payload(full_cycle, "final_pipeline_audit")
    gate_state_counts = final_audit.get("level3_gate_state_counts") or {}

    decisions = decisions_from(live_decisions)
    decision_l3_counts = level3_counts_from_decisions(decisions)
    computed_rejections = computed_research_rejections(decisions)

    fixture_ids = {
        str(first_present(decision.get("fixture_id"), (decision.get("payload") or {}).get("fixture_id")))
        for decision in decisions
        if first_present(decision.get("fixture_id"), (decision.get("payload") or {}).get("fixture_id"))
    }

    live_fixtures_seen = len(fixture_ids)
    if live_fixtures_seen <= 0:
        live_fixtures_seen = int_value(
            first_present(
                live_summary.get("live_fixtures_seen"),
                level3_summary.get("fixtures_inspected"),
                daily_level3.get("fixtures_inspected"),
                live_summary.get("groups_total"),
                default=0,
            )
        )

    groups_total = int_value(first_present(live_summary.get("groups_total"), freshness_report.get("groups_total"), default=0))
    groups_priced = int_value(first_present(live_summary.get("groups_priced"), freshness_report.get("groups_priced"), default=0))
    decisions_total = int_value(
        first_present(live_summary.get("decisions_total"), freshness_report.get("decisions_total"), len(decisions), default=0)
    )

    score_only_decisions = int_value(
        first_present(
            gate_state_counts.get("SCORE_ONLY"),
            gate_state_counts.get("score_only"),
            decision_l3_counts["score_only"],
            default=0,
        )
    )

    computed_top = computed_top_rejection_reasons(decisions)
    top_rejection_reasons = computed_top or fallback_rejection_reasons(paper_ranker)

    return {
        "live_fixtures_seen": live_fixtures_seen,
        "groups_total": groups_total,
        "groups_priced": groups_priced,
        "decisions_total": decisions_total,
        "official_decisions": int_value(first_present(live_summary.get("official_decisions"), default=0)),
        "watchlist_decisions": int_value(first_present(live_summary.get("watchlist_decisions"), default=0)),
        "blocked_decisions": int_value(first_present(live_summary.get("blocked_decisions"), default=0)),
        "candidates_this_cycle": int_value(
            first_present(
                research_summary.get("candidates_this_cycle"),
                freshness_report.get("candidates_this_cycle"),
                daily_diag.get("candidates_this_cycle"),
                default=0,
            )
        ),
        "new_snapshots_appended": int_value(
            first_present(
                research_summary.get("new_snapshots_appended"),
                freshness_report.get("new_snapshots_appended"),
                daily_diag.get("new_snapshots_appended"),
                default=0,
            )
        ),
        "level3_state_ready_count": int_value(
            first_present(
                live_summary.get("level3_state_ready"),
                level3_summary.get("state_ready"),
                level3_summary.get("ready_states"),
                daily_level3.get("state_ready"),
                daily_level3.get("ready_states"),
                decision_l3_counts["state_ready"],
                default=0,
            )
        ),
        "level3_trade_ready_count": int_value(
            first_present(
                live_summary.get("level3_trade_ready"),
                level3_summary.get("trade_ready"),
                daily_level3.get("trade_ready"),
                decision_l3_counts["trade_ready"],
                default=0,
            )
        ),
        "level3_events_available_count": int_value(
            first_present(
                live_summary.get("level3_events_available"),
                level3_summary.get("events_available"),
                daily_level3.get("events_available"),
                decision_l3_counts["events_available"],
                default=0,
            )
        ),
        "level3_stats_available_count": int_value(
            first_present(
                live_summary.get("level3_stats_available"),
                level3_summary.get("stats_available"),
                daily_level3.get("stats_available"),
                decision_l3_counts["stats_available"],
                default=0,
            )
        ),
        "score_only_decisions": score_only_decisions,
        "rejected_by_non_positive_edge_ev": int_value(
            first_present(
                research_summary.get("non_positive_edge_or_ev_rejected"),
                daily_diag.get("non_positive_edge_or_ev_rejected"),
                computed_rejections["non_positive_edge_or_ev_rejected"],
                default=0,
            )
        ),
        "rejected_by_timing_policy": int_value(
            first_present(
                research_summary.get("timing_policy_rejected"),
                daily_diag.get("timing_policy_rejected"),
                computed_rejections["timing_policy_rejected"],
                default=0,
            )
        ),
        "rejected_by_data_tier": int_value(
            first_present(
                research_summary.get("data_tier_rejected"),
                daily_diag.get("data_tier_rejected"),
                computed_rejections["data_tier_rejected"],
                default=0,
            )
        ),
        "rejected_by_final_status": int_value(
            first_present(
                research_summary.get("final_status_rejected"),
                daily_diag.get("final_status_rejected"),
                computed_rejections["final_status_rejected"],
                default=0,
            )
        ),
        "rejected_by_negative_value_veto": int_value(
            first_present(
                research_summary.get("negative_value_veto_rejected"),
                daily_diag.get("negative_value_veto_rejected"),
                computed_rejections["negative_value_veto_rejected"],
                default=0,
            )
        ),
        "top_rejection_reasons": top_rejection_reasons,
    }


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    full_cycle = read_json(FULL_CYCLE_JSON)
    freshness = read_json(LIVE_FRESHNESS_JSON)
    paper_export = read_json(PAPER_SIGNAL_EXPORT_JSON)
    paper_ranker = read_json(PAPER_ALERT_RANKER_JSON)
    go_no_go = read_json(GO_NO_GO_JSON)
    shadow_readiness = read_json(SHADOW_READINESS_JSON)
    live_decisions = read_json(LIVE_DECISIONS_JSON)
    operator_report = read_json(OPERATOR_REPORT_JSON)
    final_pipeline_audit = read_json(FINAL_PIPELINE_AUDIT_JSON)
    level3_state = read_json(LEVEL3_STATE_JSON)
    research_candidates = read_json(RESEARCH_CANDIDATES_JSON)
    level3_stats_coverage_diagnostic = read_json(LEVEL3_STATS_COVERAGE_DIAGNOSTIC_JSON)

    inputs = {
        "latest_full_cycle_report": full_cycle,
        "latest_live_freshness_report": freshness,
        "latest_paper_signal_export": paper_export,
        "latest_paper_alert_ranker": paper_ranker,
        "latest_go_no_go_report": go_no_go,
        "latest_shadow_readiness_report": shadow_readiness,
        "latest_live_decisions": live_decisions,
        "latest_operator_report": operator_report,
        "latest_final_pipeline_audit": final_pipeline_audit,
        "latest_level3_live_state": level3_state,
        "latest_research_candidates": research_candidates,
    }

    missing_inputs = [
        name
        for name, payload in inputs.items()
        if isinstance(payload, dict) and (payload.get("missing") or payload.get("error"))
    ]

    metrics = merged_summary(
        full_cycle=full_cycle,
        live_decisions=live_decisions,
        research_candidates=research_candidates,
        level3_state=level3_state,
        freshness=freshness,
        final_pipeline_audit=final_pipeline_audit,
        paper_ranker=paper_ranker,
    )

    operator_read = classify_operator_read(metrics)
    if operator_read not in OPERATOR_READS:
        operator_read = "UNKNOWN_REVIEW"

    freshness_flags = list(
        dict.fromkeys(
            str(flag)
            for flag in [
                *(freshness.get("freshness_flags") or []),
                *(freshness.get("live_freshness_flags") or []),
                *(freshness.get("historical_metric_static_review") or []),
            ]
            if str(flag)
        )
    )
    go_no_go_reasons = [str(reason) for reason in go_no_go.get("reasons") or []]

    unsafe_names = {
        "live_staking_allowed",
        "level3_live_staking_allowed",
        "can_execute_real_bets",
        "can_enable_live_staking",
        "can_mutate_ledger",
        "promotion_allowed",
    }
    unsafe_flag_paths: list[str] = []
    for name, payload in inputs.items():
        unsafe_flag_paths.extend(f"{name}:{path}" for path in find_true_flags(payload, unsafe_names))

    payload = {
        "mode": "FQIS_LIVE_OPPORTUNITY_SCANNER",
        "status": "READY" if not missing_inputs else "READY_WITH_MISSING_OPTIONAL_INPUTS",
        "generated_at_utc": generated_at_utc,
        "operator_read": operator_read,
        **metrics,
        "level3_stats_coverage_diagnostic": compact_stats_coverage_diagnostic(level3_stats_coverage_diagnostic),
        "go_no_go_reasons": go_no_go_reasons,
        "freshness_flags": freshness_flags,
        "missing_inputs": missing_inputs,
        "unsafe_source_flag_paths": unsafe_flag_paths,
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
        "source_files": {
            "latest_full_cycle_report": str(FULL_CYCLE_JSON),
            "latest_live_freshness_report": str(LIVE_FRESHNESS_JSON),
            "latest_paper_signal_export": str(PAPER_SIGNAL_EXPORT_JSON),
            "latest_paper_alert_ranker": str(PAPER_ALERT_RANKER_JSON),
            "latest_go_no_go_report": str(GO_NO_GO_JSON),
            "latest_shadow_readiness_report": str(SHADOW_READINESS_JSON),
            "latest_live_decisions": str(LIVE_DECISIONS_JSON),
            "latest_operator_report": str(OPERATOR_REPORT_JSON),
            "latest_final_pipeline_audit": str(FINAL_PIPELINE_AUDIT_JSON),
            "latest_level3_live_state": str(LEVEL3_STATE_JSON),
            "latest_research_candidates": str(RESEARCH_CANDIDATES_JSON),
            "latest_level3_stats_coverage_diagnostic": str(LEVEL3_STATS_COVERAGE_DIAGNOSTIC_JSON),
        },
        "read": {
            "purpose": "DIAGNOSTIC_ONLY",
            "decision_path_mutated": False,
            "thresholds_changed": False,
            "stake_sizing_performed": False,
            "ledger_mutation_performed": False,
            "bookmaker_execution_performed": False,
        },
    }
    return payload


def markdown_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "[]"
    return str(value)


def write_markdown(payload: dict[str, Any]) -> None:
    fields = [
        "live_fixtures_seen",
        "groups_total",
        "groups_priced",
        "decisions_total",
        "official_decisions",
        "watchlist_decisions",
        "blocked_decisions",
        "candidates_this_cycle",
        "new_snapshots_appended",
        "level3_state_ready_count",
        "level3_trade_ready_count",
        "level3_events_available_count",
        "level3_stats_available_count",
        "score_only_decisions",
        "rejected_by_non_positive_edge_ev",
        "rejected_by_timing_policy",
        "rejected_by_data_tier",
        "rejected_by_final_status",
        "rejected_by_negative_value_veto",
        "freshness_flags",
        "go_no_go_reasons",
    ]
    level3_stats_diag = payload.get("level3_stats_coverage_diagnostic") or {}

    lines = [
        "# FQIS Live Opportunity Scanner",
        "",
        "DIAGNOSTIC ONLY | NO REAL BET | NO STAKE | NO EXECUTION | NO LEDGER MUTATION",
        "",
        f"- status: **{payload.get('status')}**",
        f"- generated_at_utc: `{payload.get('generated_at_utc')}`",
        f"- operator_read: **{payload.get('operator_read')}**",
        f"- can_execute_real_bets: **{payload.get('can_execute_real_bets')}**",
        f"- can_enable_live_staking: **{payload.get('can_enable_live_staking')}**",
        f"- can_mutate_ledger: **{payload.get('can_mutate_ledger')}**",
        f"- live_staking_allowed: **{payload.get('live_staking_allowed')}**",
        f"- promotion_allowed: **{payload.get('promotion_allowed')}**",
        "",
        "## Cycle Counts",
        "",
        *[f"- {field}: **{markdown_value(payload.get(field))}**" for field in fields],
        "",
        "## Top Rejection Reasons",
        "",
    ]

    top_reasons = payload.get("top_rejection_reasons") or []
    if not top_reasons:
        lines.append("- NONE")
    else:
        for item in top_reasons:
            lines.append(f"- {item.get('count', 0)}: {safe_text(item.get('reason'))}")

    if payload.get("missing_inputs"):
        lines += [
            "",
            "## Missing Inputs",
            "",
            *[f"- {safe_text(item)}" for item in payload.get("missing_inputs") or []],
        ]

    lines += [
        "",
        "## Level 3 Stats Coverage Diagnostic",
        "",
        f"- Status: **{level3_stats_diag.get('status')}**",
        f"- Fixtures/events/raw stats/parsed stats: **{level3_stats_diag.get('fixtures_seen', 0)} / {level3_stats_diag.get('events_available', 0)} / {level3_stats_diag.get('raw_stats_available', 0)} / {level3_stats_diag.get('parsed_stats_available', 0)}**",
        f"- Events-only no stats: **{level3_stats_diag.get('events_only_no_stats', 0)}**",
        f"- Stats parser empty: **{level3_stats_diag.get('stats_parser_empty', 0)}**",
        f"- Stats endpoint missing: **{level3_stats_diag.get('stats_endpoint_missing', 0)}**",
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
    print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
