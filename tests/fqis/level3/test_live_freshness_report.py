import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
FRESHNESS_SCRIPT = ROOT / "scripts" / "fqis_live_freshness_report.py"
FULL_CYCLE_SCRIPT = ROOT / "scripts" / "fqis_run_full_audit_cycle.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
FRESHNESS_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_freshness_report.json"
FRESHNESS_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_freshness_report.md"
FULL_CYCLE_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.json"
FULL_CYCLE_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_full_cycle_report.md"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_live_freshness_report_compiles_runs_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(FRESHNESS_SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(FRESHNESS_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert sha256(LEDGER) == before
    payload = json.loads(FRESHNESS_JSON.read_text(encoding="utf-8"))
    assert payload["status"] in {"READY", "STALE_REVIEW", "MISSING_INPUTS"}
    assert isinstance(payload["freshness_flags"], list)
    assert FRESHNESS_MD.exists()


def test_full_cycle_includes_live_freshness_once_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(FULL_CYCLE_SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(FULL_CYCLE_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert sha256(LEDGER) == before
    payload = json.loads(FULL_CYCLE_JSON.read_text(encoding="utf-8"))
    labels = [step.get("label") for step in payload.get("steps") or []]
    assert "18_live_freshness_report" in labels
    report = FULL_CYCLE_MD.read_text(encoding="utf-8")
    assert report.count("## Live Freshness") == 1
