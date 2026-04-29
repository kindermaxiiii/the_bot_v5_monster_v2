import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
GO_NO_GO = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_go_no_go_report.json"


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


def test_bucket_policy_config_is_dry_run_governance_locked():
    payload = json.loads((ROOT / "config" / "fqis_bucket_policy.json").read_text(encoding="utf-8-sig"))

    assert payload["dry_run"] is True
    assert payload["mode"] == "DRY_RUN_ONLY"
    assert payload["enforce_quarantine"] is False
    assert payload["ledger_mutation_allowed"] is False
    assert payload["live_staking_allowed"] is False


def test_quarantine_scripts_do_not_mutate_research_candidates_ledger():
    before = sha256(LEDGER)

    run_script("fqis_bucket_quarantine_dry_run.py")
    after_quarantine = sha256(LEDGER)

    run_script("fqis_post_quarantine_pnl_simulation.py")
    after_pnl_simulation = sha256(LEDGER)

    assert after_quarantine == before
    assert after_pnl_simulation == before


def test_go_no_go_report_blocks_live_ready_while_live_staking_false():
    run_script("fqis_go_no_go_report.py")
    payload = json.loads(GO_NO_GO.read_text(encoding="utf-8"))

    assert payload["status"] == "READY"
    assert payload["promotion_allowed"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["simulation_only"] is True
    assert payload["go_no_go_state"] != "LIVE_READY"
    assert "CONFIG_DRY_RUN_NOT_TRUE" not in payload["reasons"]
    assert "CONFIG_ENFORCE_QUARANTINE_NOT_FALSE" not in payload["reasons"]
    assert "CONFIG_LEDGER_MUTATION_NOT_FALSE" not in payload["reasons"]
