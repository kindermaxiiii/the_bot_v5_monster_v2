import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_dedupe.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_dedupe.md"
STATE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "paper_alert_state.json"


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


def test_paper_alert_dedupe_runs_twice_persists_state_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script(EXPORT_SCRIPT)
    run_script(SCRIPT)
    first = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    run_script(SCRIPT)
    second = json.loads(OUT_JSON.read_text(encoding="utf-8"))

    assert sha256(LEDGER) == before
    assert STATE_JSON.exists()
    assert OUT_MD.exists()
    assert first["status"] == "READY"
    assert second["status"] == "READY"
    assert second["repeated_alerts"] >= first["repeated_alerts"] or second["state_size"] >= first["state_size"]
    assert second["can_execute_real_bets"] is False
    assert second["can_enable_live_staking"] is False
    assert second["can_mutate_ledger"] is False
