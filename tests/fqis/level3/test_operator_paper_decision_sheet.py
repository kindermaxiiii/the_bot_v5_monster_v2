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
SCRIPT = ROOT / "scripts" / "fqis_operator_paper_decision_sheet.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_paper_decision_sheet.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_paper_decision_sheet.md"


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


def test_operator_paper_decision_sheet_compiles_runs_preserves_ledger_and_warns():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(RANKER_SCRIPT)
    run_script(SCRIPT)

    assert sha256(LEDGER) == before
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["paper_only"] is True
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False

    md = OUT_MD.read_text(encoding="utf-8")
    for text in ["PAPER ONLY", "NO REAL BET", "NO STAKE", "NO EXECUTION"]:
        assert text in md
    for section in [
        "## PAPER ONLY WARNING",
        "## Top paper alerts",
        "## Rejected / blocked summary",
        "## Safety state",
        "## Freshness state",
        "## What to inspect next",
    ]:
        assert md.count(section) == 1
