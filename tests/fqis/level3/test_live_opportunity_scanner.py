import hashlib
import importlib.util
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCANNER_SCRIPT = ROOT / "scripts" / "fqis_live_opportunity_scanner.py"
FULL_CYCLE_SCRIPT = ROOT / "scripts" / "fqis_run_full_audit_cycle.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_opportunity_scanner.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_opportunity_scanner.md"
FULL_CYCLE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.json"
FULL_CYCLE_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.md"

SAFETY_FLAGS = [
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_scanner_module():
    spec = importlib.util.spec_from_file_location("fqis_live_opportunity_scanner", SCANNER_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_live_opportunity_scanner_compiles_runs_outputs_safety_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCANNER_SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(SCANNER_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    assert OUT_MD.exists()

    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["operator_read"] in {
        "LIVE_WINDOW_EMPTY",
        "SCORE_ONLY_NO_LEVEL3_STATE",
        "NEGATIVE_VALUE_ONLY",
        "FILTERS_TOO_STRICT_REVIEW",
        "DATA_PROVIDER_COVERAGE_REVIEW",
        "HEALTHY_NO_VALUE_WINDOW",
        "EVENTS_ONLY_RESEARCH_NO_STATS",
        "UNKNOWN_REVIEW",
    }
    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False


def test_live_opportunity_scanner_classifies_score_only_and_negative_value():
    scanner = load_scanner_module()

    score_only = scanner.classify_operator_read(
        {
            "live_fixtures_seen": 1,
            "groups_total": 1,
            "decisions_total": 2,
            "candidates_this_cycle": 0,
            "score_only_decisions": 2,
            "level3_state_ready_count": 0,
            "rejected_by_non_positive_edge_ev": 0,
        }
    )
    assert score_only == "SCORE_ONLY_NO_LEVEL3_STATE"

    negative_value = scanner.classify_operator_read(
        {
            "live_fixtures_seen": 1,
            "groups_total": 1,
            "decisions_total": 3,
            "candidates_this_cycle": 0,
            "score_only_decisions": 0,
            "level3_state_ready_count": 3,
            "level3_trade_ready_count": 3,
            "level3_events_available_count": 3,
            "level3_stats_available_count": 3,
            "rejected_by_non_positive_edge_ev": 3,
            "rejected_by_timing_policy": 0,
            "rejected_by_data_tier": 0,
            "rejected_by_final_status": 0,
            "rejected_by_negative_value_veto": 0,
        }
    )
    assert negative_value == "NEGATIVE_VALUE_ONLY"


def test_live_opportunity_scanner_classifies_events_only_research_no_stats():
    scanner = load_scanner_module()

    operator_read = scanner.classify_operator_read(
        {
            "live_fixtures_seen": 4,
            "groups_total": 4,
            "groups_priced": 4,
            "decisions_total": 4,
            "candidates_this_cycle": 2,
            "level3_state_ready_count": 2,
            "level3_events_available_count": 2,
            "level3_trade_ready_count": 0,
            "level3_stats_available_count": 0,
        }
    )

    assert operator_read == "EVENTS_ONLY_RESEARCH_NO_STATS"
    assert operator_read in scanner.OPERATOR_READS


def test_full_cycle_includes_live_opportunity_scanner_once_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(FULL_CYCLE_SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(FULL_CYCLE_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert sha256(LEDGER) == before
    payload = json.loads(FULL_CYCLE_JSON.read_text(encoding="utf-8"))
    labels = [step.get("label") for step in payload.get("steps") or []]
    assert sum(1 for label in labels if "live_opportunity_scanner" in str(label)) == 1

    reports = payload.get("reports") or {}
    assert "live_opportunity_scanner" in reports
    scanner_report = reports["live_opportunity_scanner"]
    for flag in SAFETY_FLAGS:
        assert scanner_report[flag] is False
        assert scanner_report["safety"][flag] is False

    report = FULL_CYCLE_MD.read_text(encoding="utf-8")
    assert report.count("## Live Opportunity Scanner") == 1
