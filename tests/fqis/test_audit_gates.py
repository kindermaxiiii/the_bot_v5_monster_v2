from __future__ import annotations

import json
from pathlib import Path

from app.fqis.reporting.audit_bundle import build_audit_bundle
from app.fqis.reporting.audit_gates import (
    AuditHistoryGateThresholds,
    audit_history_gate_report_to_record,
    evaluate_audit_history_from_bundle_root,
    evaluate_audit_history_from_manifest_paths,
    write_audit_history_gate_report_json,
)
from app.fqis.reporting.audit_history import discover_audit_manifest_paths
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


def test_evaluate_audit_history_from_bundle_root_defaults_to_review(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_audit_history_from_bundle_root(bundle_root)

    assert report.status == "ok"
    assert report.run_count == 2
    assert report.decision == "REVIEW"
    assert report.fail_count == 0
    assert report.warn_count >= 1
    assert not report.is_acceptable


def test_audit_history_gates_include_expected_codes(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_audit_history_from_bundle_root(bundle_root)
    gates_by_code = {gate.code: gate for gate in report.gates}

    assert gates_by_code["MIN_RUN_COUNT"].status == "PASS"
    assert gates_by_code["LATEST_ROI"].status == "PASS"
    assert gates_by_code["LATEST_BRIER_SCORE"].status == "PASS"
    assert gates_by_code["LATEST_CLV_BEAT_RATE"].status == "PASS"
    assert gates_by_code["LATEST_MODEL_MARKET_DELTA"].status == "WARN"
    assert gates_by_code["LATEST_MODEL_ONLY_COUNT"].status == "WARN"


def test_audit_history_gates_can_reject_on_strict_clv_threshold(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_audit_history_from_bundle_root(
        bundle_root,
        thresholds=AuditHistoryGateThresholds(
            min_latest_clv_beat_rate_warn=0.95,
            min_latest_clv_beat_rate_fail=0.90,
        ),
    )

    gates_by_code = {gate.code: gate for gate in report.gates}

    assert report.decision == "REJECT"
    assert report.fail_count >= 1
    assert gates_by_code["LATEST_CLV_BEAT_RATE"].status == "FAIL"


def test_audit_history_gates_can_accept_when_thresholds_are_relaxed(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_audit_history_from_bundle_root(
        bundle_root,
        thresholds=AuditHistoryGateThresholds(
            max_total_warn_count_warn=99,
            max_total_warn_count_fail=100,
            max_abs_latest_model_market_delta_warn=1.0,
            max_abs_latest_model_market_delta_fail=2.0,
            max_latest_model_only_count_warn=99,
            max_latest_model_only_count_fail=100,
        ),
    )

    assert report.decision == "ACCEPT"
    assert report.warn_count == 0
    assert report.fail_count == 0
    assert report.is_acceptable


def test_evaluate_audit_history_from_manifest_paths(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)
    manifest_paths = discover_audit_manifest_paths(bundle_root)

    report = evaluate_audit_history_from_manifest_paths(manifest_paths)

    assert report.run_count == 2
    assert report.gate_count >= 8
    assert report.decision in {"ACCEPT", "REVIEW", "REJECT"}


def test_audit_history_gate_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_audit_history_from_bundle_root(bundle_root)
    record = audit_history_gate_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_audit_history_gate_report" in encoded
    assert record["source"] == "fqis_audit_history_gate_report"
    assert record["decision"] == report.decision
    assert "gates" in record
    assert "history_report" in record


def test_write_audit_history_gate_report_json(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_audit_history_from_bundle_root(bundle_root)
    output_path = tmp_path / "audit_history_gate_report.json"

    written_path = write_audit_history_gate_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_audit_history_gate_report"
    assert record["decision"] == report.decision


def _write_two_bundles(tmp_path: Path) -> Path:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)
    bundle_root = tmp_path / "bundles"

    build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=bundle_root,
        run_id="gate-run-a",
    )
    build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=bundle_root,
        run_id="gate-run-b",
    )

    return bundle_root


def _write_bundle_inputs(tmp_path: Path) -> tuple[Path, Path]:
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