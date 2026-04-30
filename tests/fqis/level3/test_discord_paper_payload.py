import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
DEDUPE_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
RANKER_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_ranker.py"
SCRIPT = ROOT / "scripts" / "fqis_discord_paper_payload.py"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_paper_payload.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_discord_paper_payload.md"
RANKER_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_ranker.json"


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
    run_script(RANKER_SCRIPT)
    run_script(SCRIPT)

    assert OUT_MD.exists()
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    ranker = json.loads(RANKER_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert isinstance(payload["sendable"], bool)
    assert payload["alerts_included"] <= 10
    text = payload["content"]
    assert "PAPER ONLY" in text
    assert "NO REAL BET" in text
    assert "NO STAKE" in text
    assert "NO EXECUTION" in text

    ranked = ranker.get("grouped_ranked_alerts") or ranker.get("ranked_alerts") or []
    sendable_ranked = [
        alert
        for alert in ranked
        if alert.get("alert_lifecycle_status") in {"NEW_CANONICAL", "UPDATED_CANONICAL"}
        or alert.get("discord_sendable") is True
    ]
    if sendable_ranked:
        expected_keys = [alert.get("alert_key") for alert in sendable_ranked[:10]]
        actual_keys = [alert.get("alert_key") for alert in payload.get("alert_records") or []]
        assert actual_keys == expected_keys
    elif ranker.get("repeated_ranked_alert_count"):
        assert payload["sendable"] is False
        assert payload["send_reason"] == "NO_SENDABLE_CANONICAL_ALERTS_REPEATS_ONLY"

    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False
