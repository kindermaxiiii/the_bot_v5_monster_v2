import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MONITOR_SCRIPT = ROOT / "scripts" / "fqis_tonight_shadow_monitor.py"
DIGEST_SCRIPT = ROOT / "scripts" / "fqis_tonight_shadow_digest.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
MONITOR_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_tonight_shadow_monitor.json"
DIGEST_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_tonight_shadow_digest.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_tonight_shadow_monitor_digest_scripts_compile():
    py_compile.compile(str(MONITOR_SCRIPT), doraise=True)
    py_compile.compile(str(DIGEST_SCRIPT), doraise=True)


def test_tonight_shadow_monitor_summary_and_digest_are_paper_only():
    before = sha256(LEDGER)

    subprocess.run(
        [sys.executable, str(MONITOR_SCRIPT), "--cycles", "1", "--sleep-seconds", "0"],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    subprocess.run(
        [sys.executable, str(DIGEST_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert sha256(LEDGER) == before

    monitor = json.loads(MONITOR_JSON.read_text(encoding="utf-8"))
    summary = monitor.get("summary")
    assert isinstance(summary, dict)
    assert summary["all_ledger_preserved"] is True
    assert summary["any_real_bets_enabled"] is False
    assert summary["any_live_staking_enabled"] is False
    assert "total_new_paper_alerts" in summary
    assert "total_repeated_paper_alerts" in summary
    assert "unique_operator_states" in summary

    digest = json.loads(DIGEST_JSON.read_text(encoding="utf-8"))
    assert digest["verdict"] in {
        "SHADOW_SESSION_CLEAN",
        "SHADOW_SESSION_CLEAN_WITH_STALE_REVIEW",
        "SHADOW_SESSION_CLEAN_WITH_PAPER_ALERTS",
        "SHADOW_SESSION_STOPPED",
        "SHADOW_SESSION_INVALID",
    }
    assert "final_operator_state" in digest
    assert "total_new_paper_alerts" in digest
    assert digest["any_real_bets_enabled"] is False
