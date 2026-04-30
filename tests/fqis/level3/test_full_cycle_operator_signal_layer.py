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
        "22_operator_paper_decision_sheet",
        "23_discord_paper_payload",
        "24_operator_shadow_console",
    ]:
        assert labels.count(label) == 1

    report = FULL_CYCLE_MD.read_text(encoding="utf-8")
    for section in [
        "## Live Freshness",
        "## Paper Signal Export",
        "## Paper Alert Dedupe",
        "## Paper Alert Ranker",
        "## Operator Paper Decision Sheet",
        "## Discord Paper Payload",
        "## Operator Shadow Console",
    ]:
        assert report.count(section) == 1

    reports = payload.get("reports") or {}
    for name in [
        "paper_signal_export",
        "paper_alert_dedupe",
        "paper_alert_ranker",
        "operator_paper_decision_sheet",
        "discord_paper_payload",
        "operator_shadow_console",
    ]:
        assert name in reports
    assert reports["paper_alert_ranker"]["can_execute_real_bets"] is False
    assert reports["paper_alert_ranker"]["can_enable_live_staking"] is False
    assert reports["paper_alert_ranker"]["can_mutate_ledger"] is False
    assert reports["operator_paper_decision_sheet"]["can_execute_real_bets"] is False
    assert reports["operator_paper_decision_sheet"]["can_enable_live_staking"] is False
    assert reports["operator_paper_decision_sheet"]["can_mutate_ledger"] is False
    assert reports["operator_shadow_console"]["operator_state"] in {
        "PAPER_READY",
        "PAPER_REVIEW",
        "PAPER_BLOCKED",
    }
