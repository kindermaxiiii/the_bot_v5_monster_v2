import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_run_full_audit_cycle.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
FULL_CYCLE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.json"
FULL_CYCLE_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.md"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_full_cycle_includes_operator_signal_layer_once_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(SCRIPT)],
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
    for label in [
        "19_paper_signal_export",
        "20_paper_alert_dedupe",
        "21_paper_alert_ranker",
        "21b_signal_settlement_report",
        "21c_calibration_audit_report",
        "21d_clv_tracker_report",
        "21e_promotion_policy_report",
        "22_operator_paper_decision_sheet",
        "23_discord_paper_payload",
        "24_operator_shadow_console",
        "25_shadow_session_quality_report",
        "26_final_operator_readiness_dashboard",
    ]:
        assert labels.count(label) == 1

    report = FULL_CYCLE_MD.read_text(encoding="utf-8")
    for section in [
        "## Live Freshness",
        "## Paper Signal Export",
        "## Paper Alert Dedupe",
        "## Paper Alert Ranker",
        "## Signal Settlement Report",
        "## Proxy CLV Tracker",
        "## Calibration Audit",
        "## Promotion Policy",
        "## Operator Paper Decision Sheet",
        "## Discord Paper Payload",
        "## Operator Shadow Console",
        "## Shadow Session Quality",
        "## Final Operator Readiness Dashboard",
    ]:
        assert report.count(section) == 1

    reports = payload.get("reports") or {}
    for name in [
        "paper_signal_export",
        "paper_alert_dedupe",
        "paper_alert_ranker",
        "signal_settlement",
        "clv_tracker",
        "calibration",
        "promotion_policy",
        "operator_paper_decision_sheet",
        "discord_paper_payload",
        "operator_shadow_console",
        "shadow_session_quality",
        "final_operator_readiness_dashboard",
    ]:
        assert name in reports
    assert reports["paper_alert_ranker"]["can_execute_real_bets"] is False
    assert reports["paper_alert_ranker"]["can_enable_live_staking"] is False
    assert reports["paper_alert_ranker"]["can_mutate_ledger"] is False
    assert reports["clv_tracker"]["can_execute_real_bets"] is False
    assert reports["clv_tracker"]["can_enable_live_staking"] is False
    assert reports["clv_tracker"]["can_mutate_ledger"] is False
    assert reports["signal_settlement"]["can_execute_real_bets"] is False
    assert reports["signal_settlement"]["can_enable_live_staking"] is False
    assert reports["signal_settlement"]["can_mutate_ledger"] is False
    assert reports["signal_settlement"]["promotion_allowed"] is False
    assert reports["calibration"]["can_execute_real_bets"] is False
    assert reports["calibration"]["can_enable_live_staking"] is False
    assert reports["calibration"]["can_mutate_ledger"] is False
    assert reports["promotion_policy"]["can_execute_real_bets"] is False
    assert reports["promotion_policy"]["can_enable_live_staking"] is False
    assert reports["promotion_policy"]["can_mutate_ledger"] is False
    assert reports["promotion_policy"]["promotion_allowed"] is False
    assert payload["clv_tracker_status"] in {"READY", "REVIEW", "EMPTY"}
    assert payload["signal_settlement_status"] in {"READY", "REVIEW", "EMPTY"}
    assert "settled_signals" in payload
    assert "paper_roi" in payload
    assert payload["calibration_status"] in {"READY", "REVIEW", "EMPTY"}
    assert payload["promotion_policy_status"] in {"READY", "REVIEW"}
    assert payload["promotion_policy_verdict"] in {
        "NO_PROMOTION_KEEP_RESEARCH",
        "PAPER_ELITE_CANDIDATE_REVIEW",
    }
    assert reports["operator_paper_decision_sheet"]["can_execute_real_bets"] is False
    assert reports["operator_paper_decision_sheet"]["can_enable_live_staking"] is False
    assert reports["operator_paper_decision_sheet"]["can_mutate_ledger"] is False
    assert reports["operator_shadow_console"]["operator_state"] in {
        "PAPER_READY",
        "PAPER_REVIEW",
        "PAPER_BLOCKED",
    }
    assert reports["shadow_session_quality"]["quality_state"] in {
        "SESSION_GREEN",
        "SESSION_REVIEW",
        "SESSION_BLOCKED",
        "NO_MONITOR_SESSION_AVAILABLE",
    }
    assert reports["final_operator_readiness_dashboard"]["can_execute_real_bets"] is False
    assert reports["final_operator_readiness_dashboard"]["can_enable_live_staking"] is False
    assert reports["final_operator_readiness_dashboard"]["can_mutate_ledger"] is False
    assert reports["final_operator_readiness_dashboard"]["promotion_allowed"] is False
    assert reports["final_operator_readiness_dashboard"]["live_execution_enabled"] is False
