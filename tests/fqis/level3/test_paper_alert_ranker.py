import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
EXPORT_SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
DEDUPE_SCRIPT = ROOT / "scripts" / "fqis_paper_alert_dedupe.py"
SCRIPT = ROOT / "scripts" / "fqis_paper_alert_ranker.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_ranker.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_alert_ranker.md"


REQUIRED_ALERT_FIELDS = {
    "rank",
    "fixture_id",
    "match",
    "league",
    "minute",
    "score",
    "market",
    "selection",
    "odds",
    "p_model",
    "implied_probability",
    "edge_prob",
    "ev_real",
    "paper_action",
    "final_pipeline",
    "research_bucket",
    "bucket_policy_action",
    "data_tier",
    "reasons",
    "red_flags",
    "operator_note",
}


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


def test_paper_alert_ranker_compiles_runs_preserves_ledger_and_outputs_safe_ranked_alerts():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script(EXPORT_SCRIPT)
    run_script(DEDUPE_SCRIPT)
    run_script(SCRIPT)

    assert sha256(LEDGER) == before
    assert OUT_MD.exists()

    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["paper_only"] is True
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False

    ranked = payload.get("ranked_alerts") or []
    assert payload["ranked_alert_count"] == len(ranked)
    assert payload["raw_ranked_alert_count"] == len(payload.get("raw_ranked_alerts") or ranked)
    assert payload["grouped_ranked_alert_count"] == len(payload.get("grouped_ranked_alerts") or [])
    if not ranked:
        assert payload["top_ranked_alert_count"] == 0
        return
    assert [alert["rank"] for alert in ranked] == list(range(1, len(ranked) + 1))

    for alert in ranked:
        assert REQUIRED_ALERT_FIELDS.issubset(alert)
        assert alert["paper_only"] is True
        assert alert["can_execute_real_bets"] is False
        assert alert["can_enable_live_staking"] is False
        assert alert["can_mutate_ledger"] is False
        assert alert["live_staking_allowed"] is False
        assert alert["promotion_allowed"] is False
        assert "PAPER ONLY" in alert["operator_note"]
        assert "NO REAL BET" in alert["operator_note"]
        assert "NO STAKE" in alert["operator_note"]
        assert "NO EXECUTION" in alert["operator_note"]
