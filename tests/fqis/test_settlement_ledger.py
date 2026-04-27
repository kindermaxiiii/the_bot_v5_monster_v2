from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.runtime.hybrid_batch_shadow import (
    run_hybrid_shadow_batch_from_jsonl,
    write_hybrid_shadow_batch_jsonl,
)
from app.fqis.settlement.ledger import (
    MatchResult,
    load_match_results_from_jsonl,
    settlement_report_to_record,
    settle_bet_record,
    settle_hybrid_shadow_batch_from_jsonl,
    write_settlement_report_json,
)


BATCH_INPUT_PATH = Path("tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl")
RESULTS_PATH = Path("tests/fixtures/fqis/match_results_valid.jsonl")


def test_load_match_results_from_jsonl() -> None:
    results = load_match_results_from_jsonl(RESULTS_PATH)

    assert set(results) == {3101, 3102}
    assert results[3101].home_goals == 1
    assert results[3101].away_goals == 0
    assert results[3102].home_goals == 0
    assert results[3102].away_goals == 1


def test_settle_btts_no_win() -> None:
    bet = {
        "event_id": 1,
        "family": "BTTS",
        "side": "NO",
        "team_role": "NONE",
        "line": None,
        "bookmaker_name": "BookA",
        "odds_decimal": 1.75,
        "p_real": 0.62,
    }

    settled = settle_bet_record(
        bet,
        MatchResult(event_id=1, home_goals=1, away_goals=0),
        stake=1.0,
    )

    assert settled.result == "WON"
    assert settled.profit == 0.75
    assert settled.market_key == "BTTS|NO|NONE|NA"


def test_settle_match_total_push() -> None:
    bet = {
        "event_id": 1,
        "family": "MATCH_TOTAL",
        "side": "UNDER",
        "team_role": "NONE",
        "line": 2.0,
        "bookmaker_name": "BookA",
        "odds_decimal": 1.90,
        "p_real": 0.55,
    }

    settled = settle_bet_record(
        bet,
        MatchResult(event_id=1, home_goals=1, away_goals=1),
        stake=1.0,
    )

    assert settled.result == "PUSH"
    assert settled.profit == 0.0


def test_settle_hybrid_shadow_batch_from_jsonl(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=batch_path,
        results_path=RESULTS_PATH,
        stake=1.0,
    )

    assert report.status == "ok"
    assert report.accepted_bet_count >= 1
    assert report.settled_bet_count == report.accepted_bet_count
    assert report.unsettled_bet_count == 0
    assert report.won_count >= 1
    assert report.total_staked == report.settled_bet_count
    assert report.roi is not None
    assert report.has_settled_bets


def test_settlement_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=batch_path,
        results_path=RESULTS_PATH,
        stake=1.0,
    )
    record = settlement_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_settlement_report" in encoded
    assert record["accepted_bet_count"] >= 1
    assert record["settled_bet_count"] == record["accepted_bet_count"]
    assert "settled_bets" in record


def test_write_settlement_report_json(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=batch_path,
        results_path=RESULTS_PATH,
        stake=1.0,
    )

    output_path = tmp_path / "settlement_report.json"
    written_path = write_settlement_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_settlement_report"
    assert record["settled_bet_count"] == record["accepted_bet_count"]


def test_missing_match_result_marks_bet_unsettled(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)
    results_path = tmp_path / "partial_results.jsonl"
    results_path.write_text(
        '{"event_id":3101,"home_score_final":1,"away_score_final":0}\n',
        encoding="utf-8",
    )

    report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=batch_path,
        results_path=results_path,
        stake=1.0,
    )

    assert report.unsettled_bet_count >= 1
    assert any(bet.result == "UNSETTLED" for bet in report.settled_bets)


def test_load_match_results_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_match_results_from_jsonl(tmp_path / "missing.jsonl")


def _write_batch_output(tmp_path: Path) -> Path:
    outcome = run_hybrid_shadow_batch_from_jsonl(BATCH_INPUT_PATH)
    batch_path = tmp_path / "hybrid_shadow_batch.jsonl"

    write_hybrid_shadow_batch_jsonl(outcome, batch_path)

    return batch_path

    