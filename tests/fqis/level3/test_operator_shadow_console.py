import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
DEDUPE_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
RANKER_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_ranker.py"
SHEET_SCRIPT = ROOT / "scripts" / "fqis_operator_paper_decision_sheet.py"
DISCORD_SCRIPT = ROOT / "scripts" / "fqis_discord_paper_payload.py"
SCRIPT = ROOT / "scripts" / "fqis_operator_shadow_console.py"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_operator_shadow_console.md"
FRESHNESS_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_live_freshness_report.json"


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
    run_script(RANKER_SCRIPT)
    run_script(SHEET_SCRIPT)
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
    assert payload["paper_alert_ranker_status"] == "READY"
    assert payload["operator_paper_decision_sheet_status"] == "READY"
    assert "top_ranked_alert_count" in payload
    assert "monitor_artifact_generated_at_utc" in payload
    assert (payload.get("monitor") or {}).get("monitor_context") in {
        "NO_MONITOR_CONTEXT",
        "PARTIAL_MONITOR_CONTEXT",
        "FINAL_MONITOR_CONTEXT",
    }
    assert OUT_MD.exists()


def test_operator_shadow_console_can_be_ready_with_only_historical_static_freshness_review():
    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(RANKER_SCRIPT)
    run_script(SHEET_SCRIPT)
    run_script(DISCORD_SCRIPT)

    original = FRESHNESS_JSON.read_text(encoding="utf-8")
    try:
        freshness = json.loads(original)
        freshness["status"] = "STALE_REVIEW"
        freshness["freshness_flags"] = ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        freshness["historical_metric_static_review"] = ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        freshness["live_freshness_flags"] = ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        FRESHNESS_JSON.write_text(json.dumps(freshness, indent=2, sort_keys=True), encoding="utf-8")

        run_script(SCRIPT)
        payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        if payload["ranked_alert_count"] > 0:
            assert payload["operator_state"] == "PAPER_READY"
        assert payload["freshness"]["live_review_flags"] == []
        assert payload["freshness"]["only_historical_static_review"] is True
        assert payload["can_execute_real_bets"] is False
        assert payload["can_enable_live_staking"] is False
        assert payload["can_mutate_ledger"] is False
    finally:
        FRESHNESS_JSON.write_text(original, encoding="utf-8")
