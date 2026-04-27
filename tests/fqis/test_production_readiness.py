from __future__ import annotations

import json
from pathlib import Path

from app.fqis.reporting.audit_bundle import build_audit_bundle
from app.fqis.reporting.audit_gates import AuditHistoryGateThresholds
from app.fqis.reporting.audit_history import discover_audit_manifest_paths
from app.fqis.reporting.production_readiness import (
    evaluate_production_readiness_from_bundle_root,
    evaluate_production_readiness_from_manifest_paths,
    production_readiness_report_to_record,
    write_production_readiness_report_json,
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


def test_production_readiness_defaults_to_no_go_review_required(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_production_readiness_from_bundle_root(bundle_root)

    assert report.status == "ok"
    assert report.readiness_status == "NO_GO"
    assert report.readiness_level == "REVIEW_REQUIRED"
    assert report.gate_decision == "REVIEW"
    assert report.run_count == 2
    assert report.blocker_count >= 1
    assert report.warning_count >= 1
    assert report.failure_count == 0
    assert not report.is_go


def test_production_readiness_contains_recommended_actions(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_production_readiness_from_bundle_root(bundle_root)

    assert report.recommended_actions
    assert any("model-market" in action.lower() for action in report.recommended_actions)
    assert any("market-prior" in action.lower() for action in report.recommended_actions)


def test_production_readiness_can_go_with_relaxed_thresholds(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_production_readiness_from_bundle_root(
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

    assert report.readiness_status == "GO"
    assert report.readiness_level == "READY"
    assert report.gate_decision == "ACCEPT"
    assert report.blocker_count == 0
    assert report.is_go


def test_production_readiness_from_manifest_paths(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)
    manifest_paths = discover_audit_manifest_paths(bundle_root)

    report = evaluate_production_readiness_from_manifest_paths(manifest_paths)

    assert report.run_count == 2
    assert report.checklist_count >= 8
    assert report.readiness_status in {"GO", "NO_GO"}


def test_production_readiness_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_production_readiness_from_bundle_root(bundle_root)
    record = production_readiness_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_production_readiness_report" in encoded
    assert record["source"] == "fqis_production_readiness_report"
    assert record["readiness_status"] == report.readiness_status
    assert "checklist" in record
    assert "recommended_actions" in record
    assert "gate_report" in record


def test_write_production_readiness_report_json(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = evaluate_production_readiness_from_bundle_root(bundle_root)
    output_path = tmp_path / "production_readiness_report.json"

    written_path = write_production_readiness_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_production_readiness_report"
    assert record["readiness_status"] == report.readiness_status


def _write_two_bundles(tmp_path: Path) -> Path:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)
    bundle_root = tmp_path / "bundles"

    build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=bundle_root,
        run_id="readiness-run-a",
    )
    build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=bundle_root,
        run_id="readiness-run-b",
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

    