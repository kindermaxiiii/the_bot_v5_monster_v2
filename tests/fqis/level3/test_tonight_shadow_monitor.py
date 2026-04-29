import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_tonight_shadow_monitor.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
MONITOR_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_tonight_shadow_monitor.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_tonight_shadow_monitor_exists_and_compiles():
    assert SCRIPT.exists()
    py_compile.compile(str(SCRIPT), doraise=True)


def test_tonight_shadow_monitor_one_cycle_is_paper_only_and_ledger_isolated():
    before = sha256(LEDGER)

    subprocess.run(
        [sys.executable, str(SCRIPT), "--cycles", "1", "--sleep-seconds", "0"],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    after = sha256(LEDGER)
    assert after == before

    payload = json.loads(MONITOR_JSON.read_text(encoding="utf-8"))
    assert payload["status"] in {"READY", "STOPPED"}
    assert payload["cycles_completed"] >= 1

    for row in payload["rows"]:
        assert row["can_execute_real_bets"] is False
        assert row["can_enable_live_staking"] is False
        assert row["live_staking_allowed"] is False
        assert row["promotion_allowed"] is False
