import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_proxy_clv_tracker_report.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_clv_tracker_report.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def test_proxy_clv_tracker_compiles_runs_outputs_safe_report_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script()

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] in {"READY", "REVIEW", "EMPTY"}
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["promotion_allowed"] is False
    assert "observed odds movement" in payload["description"].lower()


def test_proxy_clv_tracker_missing_input_is_review_not_crash(tmp_path: Path):
    output_json = tmp_path / "clv.json"
    output_md = tmp_path / "clv.md"

    run_script(
        "--input-path",
        str(tmp_path / "missing_ranker.json"),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["status"] == "REVIEW"
    assert payload["total_records"] == 0
    assert payload["eligible_records"] == 0
    assert "MISSING_PAPER_ALERT_RANKER" in payload["warning_flags"]


def test_proxy_clv_tracker_fixture_counts_favorable_unfavorable_and_flat(tmp_path: Path):
    input_path = tmp_path / "ranker.json"
    output_json = tmp_path / "clv.json"
    output_md = tmp_path / "clv.md"
    input_path.write_text(
        json.dumps(
            {
                "status": "READY",
                "ranked_alerts": [
                    {
                        "fixture_id": "1",
                        "market": "Total Goals FT",
                        "selection": "Under 2.5",
                        "research_bucket": "STRICT_UNDER_2_5_RESEARCH",
                        "minute_bucket": "40",
                        "bucket_policy_action": "KEEP_RESEARCH_BUCKET",
                        "odds_first": 2.20,
                        "odds_latest": 2.00,
                    },
                    {
                        "fixture_id": "2",
                        "market": "Total Goals FT",
                        "selection": "Over 1.5",
                        "research_bucket": "STRICT_OVER_RESEARCH",
                        "minute_bucket": "55",
                        "bucket_policy_action": "KEEP_RESEARCH_BUCKET",
                        "odds_first": 1.80,
                        "odds_latest": 1.95,
                    },
                    {
                        "fixture_id": "3",
                        "market": "BTTS",
                        "selection": "Yes",
                        "research_bucket": "STRICT_MARKET_RESEARCH",
                        "minute_bucket": "60",
                        "bucket_policy_action": "WATCHLIST_BUCKET",
                        "odds_first": 1.90,
                        "odds_latest": 1.90,
                    },
                    {
                        "fixture_id": "4",
                        "market": "BTTS",
                        "selection": "No",
                        "research_bucket": "STRICT_MARKET_RESEARCH",
                        "minute_bucket": "60",
                        "bucket_policy_action": "WATCHLIST_BUCKET",
                        "odds_first": "",
                        "odds_latest": 2.10,
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    run_script(
        "--input-path",
        str(input_path),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["total_records"] == 4
    assert payload["eligible_records"] == 3
    assert payload["favorable_move_count"] == 1
    assert payload["unfavorable_move_count"] == 1
    assert payload["flat_move_count"] == 1
    assert payload["favorable_move_rate"] == 0.333333
    assert payload["by_market"]["Total Goals FT"]["eligible_records"] == 2
    assert payload["by_selection"]["Under 2.5"]["favorable_move_count"] == 1
