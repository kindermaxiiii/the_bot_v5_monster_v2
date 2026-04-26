from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.reporting.audit_bundle import (
    audit_bundle_manifest_to_record,
    build_audit_bundle,
    write_audit_bundle_manifest_json,
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


def test_build_audit_bundle_creates_expected_files(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)

    manifest = build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=tmp_path / "bundles",
        run_id="test-bundle-37",
    )

    assert manifest.status == "ok"
    assert manifest.run_id == "test-bundle-37"
    assert manifest.file_count == 8
    assert manifest.total_size_bytes > 0
    assert manifest.health_status in {"PASS", "WARN"}
    assert Path(manifest.bundle_dir).exists()

    roles = {file.role for file in manifest.files}

    assert roles == {
        "input_hybrid_shadow_batch",
        "input_settlement_report",
        "input_closing_odds",
        "report_hybrid_shadow_batch",
        "report_performance",
        "report_clv",
        "report_run_audit",
        "manifest",
    }

    for file in manifest.files:
        assert Path(file.path).exists()
        assert file.size_bytes > 0
        assert len(file.sha256) == 64


def test_build_audit_bundle_can_skip_input_copies(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)

    manifest = build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=tmp_path / "bundles",
        run_id="no-input-copy",
        include_input_copies=False,
    )

    roles = {file.role for file in manifest.files}

    assert manifest.file_count == 5
    assert "input_hybrid_shadow_batch" not in roles
    assert "report_run_audit" in roles
    assert "manifest" in roles


def test_audit_bundle_manifest_to_record_is_json_serializable(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)

    manifest = build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=tmp_path / "bundles",
        run_id="json-bundle",
    )

    record = audit_bundle_manifest_to_record(manifest)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_audit_bundle_manifest" in encoded
    assert record["source"] == "fqis_audit_bundle_manifest"
    assert record["run_id"] == "json-bundle"
    assert record["file_count"] == 8
    assert "headline_metrics" in record
    assert "files" in record


def test_write_audit_bundle_manifest_json(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)

    manifest = build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=tmp_path / "bundles",
        run_id="write-manifest",
    )

    output_path = tmp_path / "external_manifest.json"
    written_path = write_audit_bundle_manifest_json(manifest, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_audit_bundle_manifest"
    assert record["run_id"] == "write-manifest"


def test_audit_bundle_contains_report_payloads(tmp_path: Path) -> None:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)

    manifest = build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=tmp_path / "bundles",
        run_id="payload-check",
    )

    bundle_dir = Path(manifest.bundle_dir)
    run_audit_record = json.loads(
        (bundle_dir / "reports" / "run_audit_report.json").read_text(encoding="utf-8")
    )
    performance_record = json.loads(
        (bundle_dir / "reports" / "performance_report.json").read_text(encoding="utf-8")
    )
    clv_record = json.loads(
        (bundle_dir / "reports" / "clv_report.json").read_text(encoding="utf-8")
    )

    assert run_audit_record["source"] == "fqis_run_audit_report"
    assert performance_record["source"] == "fqis_performance_report"
    assert clv_record["source"] == "fqis_clv_report"
    assert run_audit_record["headline_metrics"]["accepted_bet_count"] == 3


def test_audit_bundle_missing_input_raises(tmp_path: Path) -> None:
    _, settlement_path = _write_bundle_inputs(tmp_path)

    with pytest.raises(FileNotFoundError):
        build_audit_bundle(
            hybrid_batch_path=tmp_path / "missing_hybrid.jsonl",
            settlement_path=settlement_path,
            closing_path=CLOSING_PATH,
            output_dir=tmp_path / "bundles",
            run_id="missing-input",
        )


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