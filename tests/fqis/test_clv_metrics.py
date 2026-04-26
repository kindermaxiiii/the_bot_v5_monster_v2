from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from app.fqis.performance.clv import (
    build_clv_report_from_json,
    clv_report_to_record,
    implied_probability_from_decimal_odds,
    load_closing_odds_from_jsonl,
    write_clv_report_json,
)
from app.fqis.runtime.hybrid_batch_shadow import (
    run_hybrid_shadow_batch_from_jsonl,
    write_hybrid_shadow_batch_jsonl,
)
from app.fqis.settlement.ledger import (
    settle_hybrid_shadow_batch_from_jsonl,
    write_settlement_report_json,
)


BATCH_INPUT_PATH = Path("tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl")
RESULTS_PATH = Path("tests/fixtures/fqis/match_results_valid.jsonl")
CLOSING_PATH = Path("tests/fixtures/fqis/closing_odds_valid.jsonl")


def test_load_closing_odds_from_jsonl() -> None:
    closing = load_closing_odds_from_jsonl(CLOSING_PATH)

    assert len(closing) == 3
    assert closing[(3101, "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5")].closing_odds_decimal == 1.70
    assert closing[(3101, "MATCH_TOTAL|UNDER|NONE|2.5")].closing_odds_decimal == 1.90


def test_implied_probability_from_decimal_odds() -> None:
    assert implied_probability_from_decimal_odds(2.0) == 0.5

    with pytest.raises(ValueError):
        implied_probability_from_decimal_odds(1.0)


def test_build_clv_report_from_json(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
    )

    assert report.status == "ok"
    assert report.bet_count == 3
    assert report.priced_count == 3
    assert report.missing_count == 0
    assert report.beat_count == 2
    assert report.not_beat_count == 1
    assert math.isclose(report.beat_rate, 2 / 3)
    assert report.average_clv_odds_delta is not None
    assert report.average_clv_percent is not None
    assert report.average_clv_implied_probability_delta is not None
    assert report.has_priced_bets


def test_clv_report_by_family_and_market_key(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
    )

    assert report.clv_by_family["TEAM_TOTAL_AWAY"].beat_count == 1
    assert report.clv_by_family["MATCH_TOTAL"].beat_count == 0
    assert report.clv_by_market_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"].beat_rate == 1.0
    assert report.clv_by_market_key["MATCH_TOTAL|UNDER|NONE|2.5"].beat_rate == 0.0


def test_clv_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
    )
    record = clv_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_clv_report" in encoded
    assert record["bet_count"] == 3
    assert record["priced_count"] == 3
    assert "clv_by_family" in record
    assert "clv_by_market_key" in record
    assert "clv_bets" in record


def test_write_clv_report_json(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
    )
    output_path = tmp_path / "clv_report.json"

    written_path = write_clv_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_clv_report"
    assert record["priced_count"] == 3


def test_missing_closing_odds_are_tracked(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)
    partial_closing_path = tmp_path / "partial_closing.jsonl"
    partial_closing_path.write_text(
        '{"event_id":3101,"market_key":"TEAM_TOTAL_AWAY|UNDER|AWAY|1.5","closing_odds_decimal":1.70}\n',
        encoding="utf-8",
    )

    report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=partial_closing_path,
    )

    assert report.bet_count == 3
    assert report.priced_count == 1
    assert report.missing_count == 2
    assert any(bet.clv_status == "missing_closing_odds" for bet in report.clv_bets)


def test_load_closing_odds_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_closing_odds_from_jsonl(tmp_path / "missing.jsonl")


def _write_settlement_report(tmp_path: Path) -> Path:
    batch_outcome = run_hybrid_shadow_batch_from_jsonl(BATCH_INPUT_PATH)
    batch_path = tmp_path / "hybrid_shadow_batch.jsonl"

    write_hybrid_shadow_batch_jsonl(batch_outcome, batch_path)

    settlement_report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=batch_path,
        results_path=RESULTS_PATH,
        stake=1.0,
    )
    settlement_path = tmp_path / "settlement_report.json"

    write_settlement_report_json(settlement_report, settlement_path)

    return settlement_path

    