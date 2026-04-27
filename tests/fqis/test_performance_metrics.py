from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.performance.metrics import (
    build_performance_report_from_json,
    load_settlement_report_records,
    performance_report_to_record,
    write_performance_report_json,
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


def test_build_performance_report_from_json(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_performance_report_from_json(settlement_path)

    assert report.status == "ok"
    assert report.report_count == 1
    assert report.bet_count >= 1
    assert report.settled_bet_count == report.bet_count
    assert report.graded_bet_count >= 1
    assert report.won_count >= 1
    assert report.lost_count >= 0
    assert report.roi is not None
    assert report.hit_rate is not None
    assert report.brier_score is not None
    assert report.has_graded_bets


def test_performance_report_contains_numeric_summaries(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_performance_report_from_json(settlement_path)

    assert report.numeric_summaries["odds_decimal"].count == report.bet_count
    assert report.numeric_summaries["p_real"].count == report.bet_count
    assert report.numeric_summaries["profit"].count == report.bet_count
    assert report.average_odds is not None
    assert report.average_p_real is not None


def test_performance_report_by_family_and_market_key(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_performance_report_from_json(settlement_path)

    assert report.performance_by_family
    assert report.performance_by_market_key
    assert "TEAM_TOTAL_AWAY" in report.performance_by_family
    assert any(
        key.startswith("TEAM_TOTAL_AWAY|UNDER|AWAY|1.5")
        for key in report.performance_by_market_key
    )


def test_performance_report_contains_calibration_buckets(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_performance_report_from_json(settlement_path, bucket_size=0.10)

    assert report.calibration_buckets
    assert sum(bucket.bet_count for bucket in report.calibration_buckets) == report.graded_bet_count
    assert all(bucket.mean_predicted_probability is not None for bucket in report.calibration_buckets)
    assert all(bucket.observed_win_rate is not None for bucket in report.calibration_buckets)
    assert all(bucket.brier_score is not None for bucket in report.calibration_buckets)


def test_performance_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_performance_report_from_json(settlement_path)
    record = performance_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_performance_report" in encoded
    assert record["bet_count"] == report.bet_count
    assert "performance_by_family" in record
    assert "performance_by_market_key" in record
    assert "calibration_buckets" in record


def test_write_performance_report_json(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    report = build_performance_report_from_json(settlement_path)
    output_path = tmp_path / "performance_report.json"

    written_path = write_performance_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_performance_report"
    assert record["bet_count"] == report.bet_count


def test_load_settlement_report_records_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_settlement_report_records(tmp_path / "missing.json")


def test_performance_report_rejects_invalid_bucket_size(tmp_path: Path) -> None:
    settlement_path = _write_settlement_report(tmp_path)

    with pytest.raises(ValueError):
        build_performance_report_from_json(settlement_path, bucket_size=0.0)


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

    