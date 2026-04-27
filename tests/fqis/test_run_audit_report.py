from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.reporting.run_audit import (
    RunAuditThresholds,
    build_run_audit_report,
    run_audit_report_to_record,
    write_run_audit_report_json,
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


def test_build_run_audit_report(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_audit_inputs(tmp_path)

    report = build_run_audit_report(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        run_id="test-run-36",
    )

    assert report.status == "ok"
    assert report.run_id == "test-run-36"
    assert report.health_status in {"PASS", "WARN"}
    assert report.hybrid_batch_report["match_count"] == 2
    assert report.settlement_report["accepted_bet_count"] == 3
    assert report.performance_report["bet_count"] == 3
    assert report.clv_report["priced_count"] == 3
    assert report.headline_metrics["accepted_bet_count"] == 3
    assert report.headline_metrics["clv_beat_rate"] is not None


def test_run_audit_report_contains_expected_flags(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_audit_inputs(tmp_path)

    report = build_run_audit_report(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        thresholds=RunAuditThresholds(max_abs_delta_model_market_mean=0.25),
    )

    codes = {flag.code for flag in report.audit_flags}

    assert "HIGH_MODEL_MARKET_DELTA_MEAN" in codes
    assert "MODEL_ONLY_PROBABILITIES_PRESENT" in codes
    assert report.warn_count >= 1
    assert report.info_count >= 1


def test_run_audit_thresholds_can_relax_delta_warning(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_audit_inputs(tmp_path)

    report = build_run_audit_report(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        thresholds=RunAuditThresholds(max_abs_delta_model_market_mean=1.00),
    )

    codes = {flag.code for flag in report.audit_flags}

    assert "HIGH_MODEL_MARKET_DELTA_MEAN" not in codes


def test_run_audit_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_audit_inputs(tmp_path)

    report = build_run_audit_report(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        run_id="json-test-run",
    )
    record = run_audit_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_run_audit_report" in encoded
    assert record["source"] == "fqis_run_audit_report"
    assert record["run_id"] == "json-test-run"
    assert "headline_metrics" in record
    assert "reports" in record
    assert "hybrid_batch" in record["reports"]
    assert "settlement" in record["reports"]
    assert "performance" in record["reports"]
    assert "clv" in record["reports"]


def test_write_run_audit_report_json(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_audit_inputs(tmp_path)

    report = build_run_audit_report(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
    )

    output_path = tmp_path / "run_audit_report.json"
    written_path = write_run_audit_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_run_audit_report"
    assert "headline_metrics" in record
    assert "audit_flags" in record


def test_run_audit_missing_closing_path_raises(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_audit_inputs(tmp_path)

    with pytest.raises(FileNotFoundError):
        build_run_audit_report(
            hybrid_batch_path=hybrid_batch_path,
            settlement_path=settlement_path,
            closing_path=tmp_path / "missing_closing.jsonl",
        )


def _write_audit_inputs(tmp_path: Path) -> tuple[Path, Path]:
    batch_outcome = run_hybrid_shadow_batch_from_jsonl(BATCH_INPUT_PATH)
    hybrid_batch_path = tmp_path / "hybrid_shadow_batch.jsonl"

    write_hybrid_shadow_batch_jsonl(batch_outcome, hybrid_batch_path)

    settlement_report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=hybrid_batch_path,
        results_path=RESULTS_PATH,
        stake=1.0,
    )
    settlement_path = tmp_path / "settlement_report.json"

    write_settlement_report_json(settlement_report, settlement_path)

    return hybrid_batch_path, settlement_path