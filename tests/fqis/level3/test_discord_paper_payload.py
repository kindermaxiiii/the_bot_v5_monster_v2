import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
DEDUPE_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
SCRIPT = ROOT / "scripts" / "fqis_discord_paper_payload.py"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_paper_payload.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_paper_payload.md"


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


def test_discord_paper_payload_compiles_runs_and_is_safe():
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(SCRIPT)

    assert OUT_MD.exists()
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert isinstance(payload["sendable"], bool)
    if payload["sendable"]:
        text = payload["content"]
        assert "PAPER ONLY" in text
        assert "NO REAL BET" in text
        assert "NO STAKE" in text
        assert "NO EXECUTION" in text
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
