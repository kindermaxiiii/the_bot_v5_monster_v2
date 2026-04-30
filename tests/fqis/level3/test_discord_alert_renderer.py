import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_discord_alert_renderer.py"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_alert_renderer.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_alert_renderer.md"
OUT_HTML = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_alert_preview.html"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"

SAFETY_FLAGS = [
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_discord_alert_renderer_runs_preview_only_and_preserves_ledger():
    py_compile.compile(str(SCRIPT), doraise=True)

    before = sha256(LEDGER)

    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    assert OUT_MD.exists()
    assert OUT_HTML.exists()

    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))

    assert payload["mode"] == "FQIS_DISCORD_ALERT_RENDERER"
    assert payload["status"] in {"READY", "REVIEW"}

    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False

    assert payload["read"]["purpose"] == "PRESENTATION_ONLY"
    assert payload["read"]["decision_path_mutated"] is False
    assert payload["read"]["thresholds_changed"] is False
    assert payload["read"]["stake_sizing_performed"] is False
    assert payload["read"]["ledger_mutation_performed"] is False
    assert payload["read"]["bookmaker_execution_performed"] is False
    assert payload["read"]["discord_send_performed"] is False

    html = OUT_HTML.read_text(encoding="utf-8")
    markdown = OUT_MD.read_text(encoding="utf-8")

    assert "FQIS Discord Alert Preview" in html
    assert "PAPER ONLY" in html
    assert "NO REAL BET" in html
    assert "NO STAKE" in html
    assert "NO EXECUTION" in html
    assert "FQIS Discord Alert Renderer" in markdown

    stdout_payload = json.loads(result.stdout)
    assert stdout_payload["discord_send_performed"] is False