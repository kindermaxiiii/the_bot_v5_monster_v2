import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
DEDUPE_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
RANKER_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_ranker.py"
SHEET_SCRIPT = ROOT / "scripts" / "fqis_operator_paper_decision_sheet.py"
DISCORD_SCRIPT = ROOT / "scripts" / "fqis_discord_paper_payload.py"
SCANNER_SCRIPT = ROOT / "scripts" / "fqis_live_opportunity_scanner.py"
SCRIPT = ROOT / "scripts" / "fqis_operator_shadow_console.py"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.md"
FRESHNESS_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_freshness_report.json"
SCANNER_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_opportunity_scanner.json"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
SAFETY_FLAGS = [
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
]
ALLOWED_OPERATOR_READS = {
    "LIVE_WINDOW_EMPTY",
    "SCORE_ONLY_NO_LEVEL3_STATE",
    "NEGATIVE_VALUE_ONLY",
    "FILTERS_TOO_STRICT_REVIEW",
    "DATA_PROVIDER_COVERAGE_REVIEW",
    "HEALTHY_NO_VALUE_WINDOW",
    "EVENTS_ONLY_RESEARCH_NO_STATS",
    "UNKNOWN_REVIEW",
}
SCANNER_METRIC_FIELDS = [
    "live_fixtures_seen",
    "groups_total",
    "groups_priced",
    "decisions_total",
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
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_script(path: Path) -> None:
    subprocess.run(
        [sys.executable, str(path)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def controlled_scanner_fixture() -> dict:
    payload = {
        "mode": "FQIS_LIVE_OPPORTUNITY_SCANNER",
        "status": "READY",
        "generated_at_utc": "2026-04-30T00:00:00+00:00",
        "operator_read": "EVENTS_ONLY_RESEARCH_NO_STATS",
        "live_fixtures_seen": 4,
        "groups_total": 4,
        "groups_priced": 4,
        "decisions_total": 6,
        "candidates_this_cycle": 2,
        "new_snapshots_appended": 0,
        "level3_state_ready_count": 2,
        "level3_trade_ready_count": 0,
        "level3_events_available_count": 2,
        "level3_stats_available_count": 0,
        "score_only_decisions": 1,
        "rejected_by_non_positive_edge_ev": 2,
        "rejected_by_timing_policy": 1,
        "rejected_by_data_tier": 1,
        "rejected_by_final_status": 0,
        "rejected_by_negative_value_veto": 1,
        "top_rejection_reasons": [
            {"count": 2, "reason": "level3_not_trade_ready_without_statistics"},
            {"count": 1, "reason": "non_positive_edge"},
            {"count": 1, "reason": "timing_policy_review"},
        ],
        "read": {
            "purpose": "DIAGNOSTIC_ONLY",
            "decision_path_mutated": False,
            "thresholds_changed": False,
            "stake_sizing_performed": False,
            "ledger_mutation_performed": False,
            "bookmaker_execution_performed": False,
        },
        "safety": {},
    }
    for flag in SAFETY_FLAGS:
        payload[flag] = False
        payload["safety"][flag] = False
    return payload


def write_controlled_scanner_fixture() -> None:
    SCANNER_JSON.write_text(
        json.dumps(controlled_scanner_fixture(), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def test_operator_shadow_console_compiles_runs_and_never_live_ready():
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(RANKER_SCRIPT)
    run_script(SHEET_SCRIPT)
    run_script(DISCORD_SCRIPT)
    before = sha256(LEDGER)
    scanner_original = SCANNER_JSON.read_text(encoding="utf-8") if SCANNER_JSON.exists() else None
    try:
        write_controlled_scanner_fixture()
        run_script(SCRIPT)
    finally:
        if scanner_original is None:
            SCANNER_JSON.unlink(missing_ok=True)
        else:
            SCANNER_JSON.write_text(scanner_original, encoding="utf-8")

    assert sha256(LEDGER) == before
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["operator_state"] in {"PAPER_READY", "PAPER_REVIEW", "PAPER_BLOCKED"}
    assert payload["operator_state"] != "LIVE_READY"
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False
    assert payload["paper_alert_ranker_status"] == "READY"
    assert payload["operator_paper_decision_sheet_status"] == "READY"
    assert "top_ranked_alert_count" in payload
    scanner = payload["live_opportunity_scanner"]
    assert scanner["status"] == "READY"
    assert scanner["operator_read"] == "EVENTS_ONLY_RESEARCH_NO_STATS"
    for field in SCANNER_METRIC_FIELDS:
        assert field in scanner
    assert len(scanner["top_rejection_reasons"]) == 3
    for flag in SAFETY_FLAGS:
        assert scanner[flag] is False
    assert scanner["read"]["ledger_mutation_performed"] is False
    assert scanner["read"]["bookmaker_execution_performed"] is False
    assert "monitor_artifact_generated_at_utc" in payload
    assert (payload.get("monitor") or {}).get("monitor_context") in {
        "NO_MONITOR_CONTEXT",
        "PARTIAL_MONITOR_CONTEXT",
        "FINAL_MONITOR_CONTEXT",
    }
    assert OUT_MD.exists()
    markdown = OUT_MD.read_text(encoding="utf-8")
    assert "Live Opportunity Scanner" in markdown
    assert "EVENTS_ONLY_RESEARCH_NO_STATS" in markdown


def test_operator_shadow_console_live_scanner_smoke_preserves_safety_and_ledger():
    before = sha256(LEDGER)

    run_script(SCANNER_SCRIPT)

    assert sha256(LEDGER) == before
    payload = json.loads(SCANNER_JSON.read_text(encoding="utf-8"))
    assert payload["operator_read"] in ALLOWED_OPERATOR_READS
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False


def test_operator_shadow_console_can_be_ready_with_only_historical_static_freshness_review():
    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(RANKER_SCRIPT)
    run_script(SHEET_SCRIPT)
    run_script(DISCORD_SCRIPT)

    original = FRESHNESS_JSON.read_text(encoding="utf-8")
    scanner_original = SCANNER_JSON.read_text(encoding="utf-8")
    try:
        freshness = json.loads(original)
        freshness["status"] = "STALE_REVIEW"
        freshness["freshness_flags"] = ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        freshness["historical_metric_static_review"] = ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        freshness["live_freshness_flags"] = ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        FRESHNESS_JSON.write_text(json.dumps(freshness, indent=2, sort_keys=True), encoding="utf-8")
        write_controlled_scanner_fixture()

        run_script(SCRIPT)
        payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        if payload["ranked_alert_count"] > 0:
            assert payload["operator_state"] == "PAPER_READY"
        assert payload["live_opportunity_scanner"]["operator_read"] == "EVENTS_ONLY_RESEARCH_NO_STATS"
        assert payload["freshness"]["live_review_flags"] == []
        assert payload["freshness"]["only_historical_static_review"] is True
        assert payload["can_execute_real_bets"] is False
        assert payload["can_enable_live_staking"] is False
        assert payload["can_mutate_ledger"] is False
    finally:
        FRESHNESS_JSON.write_text(original, encoding="utf-8")
        SCANNER_JSON.write_text(scanner_original, encoding="utf-8")
