import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
DEDUPE_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
DISCORD_SCRIPT = ROOT / "scripts" / "fqis_discord_paper_payload.py"
SCRIPT = ROOT / "scripts" / "fqis_operator_shadow_console.py"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.md"


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


def test_operator_shadow_console_compiles_runs_and_never_live_ready():
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(DISCORD_SCRIPT)
    run_script(SCRIPT)

    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["operator_state"] in {"PAPER_READY", "PAPER_REVIEW", "PAPER_BLOCKED"}
    assert payload["operator_state"] != "LIVE_READY"
    assert payload["promotion_allowed"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert OUT_MD.exists()
