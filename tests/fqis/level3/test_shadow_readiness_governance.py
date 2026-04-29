import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
FULL_CYCLE = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.json"
FULL_CYCLE_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.md"
GO_NO_GO = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_go_no_go_report.json"
SHADOW = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_shadow_readiness_report.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_script(script_name: str) -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script_name)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def test_full_cycle_shadow_readiness_governance_and_ledger_isolation():
    before = sha256(LEDGER)

    run_script("fqis_run_full_audit_cycle.py")
    after_full_cycle = sha256(LEDGER)

    run_script("fqis_shadow_readiness_report.py")
    after_shadow = sha256(LEDGER)

    assert after_full_cycle == before
    assert after_shadow == before

    full_cycle = json.loads(FULL_CYCLE.read_text(encoding="utf-8"))
    labels = [step["label"] for step in full_cycle["steps"]]
    assert labels.count("17_shadow_readiness_report") == 1

    report_md = FULL_CYCLE_MD.read_text(encoding="utf-8")
    assert report_md.count("## Shadow Readiness") == 1

    go_no_go = json.loads(GO_NO_GO.read_text(encoding="utf-8"))
    shadow = json.loads(SHADOW.read_text(encoding="utf-8"))

    assert shadow["shadow_state"] in {"SHADOW_READY", "SHADOW_BLOCKED"}
    assert shadow["shadow_state"] != "LIVE_READY"
    if go_no_go["live_staking_allowed"] is False:
        assert shadow["shadow_state"] != "LIVE_READY"

    assert shadow["can_execute_real_bets"] is False
    assert shadow["can_enable_live_staking"] is False
    assert shadow["can_mutate_ledger"] is False
