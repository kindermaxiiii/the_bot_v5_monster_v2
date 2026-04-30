import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_signal_settlement_report.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_signal_settlement_report.json"


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


def write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def signal(
    *,
    key: str,
    selection: str,
    odds: float,
    market: str = "Total Goals FT",
) -> dict:
    return {
        "canonical_alert_key": f"fixture-1|{market}|{selection}|research|strict|60|{key}",
        "alert_key": f"fixture-1|{selection}|{odds}",
        "fixture_id": "fixture-1",
        "match": "Home FC vs Away FC",
        "league": "Test League",
        "market": market,
        "selection": selection,
        "research_bucket": "STRICT_TOTALS_RESEARCH",
        "data_tier": "STRICT_EVENTS_PLUS_STATS",
        "minute": 60,
        "minute_bucket": "60",
        "score": "1-1",
        "odds": odds,
        "odds_latest": odds,
        "p_model": 0.62,
        "implied_probability": 1 / odds,
        "edge_prob": 0.08,
        "ev_real": 0.12,
        "alert_lifecycle_status": "NEW_CANONICAL",
        "paper_action": "PAPER_RESEARCH_WATCH",
        "bucket_policy_action": "KEEP_RESEARCH_BUCKET",
        "red_flags": [],
    }


def test_signal_settlement_compiles_runs_safely_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script()

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] in {"READY", "REVIEW", "EMPTY"}
    assert payload["paper_only"] is True
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False
    assert payload["safety"]["promotion_allowed"] is False


def test_signal_settlement_missing_inputs_review_not_crash(tmp_path: Path):
    output_json = tmp_path / "signal_settlement.json"
    output_md = tmp_path / "signal_settlement.md"

    run_script(
        "--ranker-path",
        str(tmp_path / "missing_ranker.json"),
        "--dedupe-path",
        str(tmp_path / "missing_dedupe.json"),
        "--research-settlement-path",
        str(tmp_path / "missing_research.json"),
        "--fixture-level-path",
        str(tmp_path / "missing_fixture.json"),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["status"] == "REVIEW"
    assert payload["total_signals"] == 0
    assert "NO_SIGNAL_RECORDS" in payload["warning_flags"]


def test_signal_settlement_settles_total_goals_half_lines_and_excludes_unknown_from_roi(tmp_path: Path):
    ranker = write_json(
        tmp_path / "ranker.json",
        {
            "grouped_ranked_alerts": [
                signal(key="over-25", selection="Over 2.5", odds=2.0),
                signal(key="under-25", selection="Under 2.5", odds=1.8),
                signal(key="under-35", selection="Under 3.5", odds=1.5),
                signal(key="unknown", selection="Team Goals Yes", odds=3.0),
            ]
        },
    )
    research = write_json(
        tmp_path / "research_settlement.json",
        {
            "rows": [
                {
                    "fixture_id": "fixture-1",
                    "match": "Home FC vs Away FC",
                    "final_home_goals": 2,
                    "final_away_goals": 1,
                    "final_total_goals": 3,
                }
            ]
        },
    )
    output_json = tmp_path / "signal_settlement.json"

    run_script(
        "--ranker-path",
        str(ranker),
        "--dedupe-path",
        str(tmp_path / "missing_dedupe.json"),
        "--research-settlement-path",
        str(research),
        "--fixture-level-path",
        str(tmp_path / "missing_fixture.json"),
        "--output-json",
        str(output_json),
        "--output-md",
        str(tmp_path / "signal_settlement.md"),
    )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["total_signals"] == 4
    assert payload["settled_signals"] == 3
    assert payload["win_count"] == 2
    assert payload["loss_count"] == 1
    assert payload["unknown_count"] == 1
    assert payload["paper_pnl_total"] == 0.5
    assert payload["paper_roi"] == 0.166667

    by_selection = {row["selection"]: row for row in payload["rows"]}
    assert by_selection["Over 2.5"]["result_status"] == "WIN"
    assert by_selection["Under 2.5"]["result_status"] == "LOSS"
    assert by_selection["Under 3.5"]["result_status"] == "WIN"
    assert by_selection["Team Goals Yes"]["settlement_status"] == "UNKNOWN"
    assert by_selection["Team Goals Yes"]["paper_pnl"] == 0.0


def test_signal_settlement_rows_preserve_canonical_fields_and_metrics(tmp_path: Path):
    ranker = write_json(tmp_path / "ranker.json", {"grouped_ranked_alerts": [signal(key="over-25", selection="Over 2.5", odds=2.0)]})
    research = write_json(
        tmp_path / "research_settlement.json",
        {"rows": [{"fixture_id": "fixture-1", "final_score": "2-1"}]},
    )
    output_json = tmp_path / "signal_settlement.json"

    run_script(
        "--ranker-path",
        str(ranker),
        "--dedupe-path",
        str(tmp_path / "missing_dedupe.json"),
        "--research-settlement-path",
        str(research),
        "--fixture-level-path",
        str(tmp_path / "missing_fixture.json"),
        "--output-json",
        str(output_json),
        "--output-md",
        str(tmp_path / "signal_settlement.md"),
    )

    row = json.loads(output_json.read_text(encoding="utf-8"))["rows"][0]
    assert row["canonical_alert_key"]
    assert row["market"] == "Total Goals FT"
    assert row["selection"] == "Over 2.5"
    assert row["odds"] == 2.0
    assert row["p_model"] == 0.62
    assert row["edge_prob"] == 0.08
    assert row["ev_real"] == 0.12
    assert row["final_score"] == "2-1"
    assert row["paper_stake"] == 1.0
    assert row["paper_pnl"] == 1.0
