from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.reporting.audit_bundle import build_audit_bundle
from app.fqis.reporting.audit_history import (
    audit_history_report_to_record,
    build_audit_history_report_from_bundle_root,
    build_audit_history_report_from_manifest_paths,
    discover_audit_manifest_paths,
    load_audit_bundle_manifest,
    write_audit_history_report_json,
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


def test_discover_audit_manifest_paths(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    paths = discover_audit_manifest_paths(bundle_root)

    assert len(paths) == 2
    assert all(path.name == "manifest.json" for path in paths)


def test_load_audit_bundle_manifest(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)
    manifest_path = discover_audit_manifest_paths(bundle_root)[0]

    record = load_audit_bundle_manifest(manifest_path)

    assert record["source"] == "fqis_audit_bundle_manifest"
    assert record["file_count"] == 7
    assert "headline_metrics" in record


def test_build_audit_history_report_from_bundle_root(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = build_audit_history_report_from_bundle_root(bundle_root)

    assert report.status == "ok"
    assert report.run_count == 2
    assert report.has_runs
    assert report.total_file_count == 14
    assert report.total_size_bytes > 0
    assert report.health_counts["WARN"] == 2
    assert report.total_warn_count >= 2
    assert report.total_info_count >= 2


def test_audit_history_report_contains_metric_summaries(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = build_audit_history_report_from_bundle_root(bundle_root)

    assert report.metric_summaries["roi"].count == 2
    assert report.metric_summaries["roi"].latest is not None
    assert report.metric_summaries["roi"].previous is not None
    assert report.metric_summaries["roi"].change is not None
    assert report.metric_summaries["brier_score"].count == 2
    assert report.metric_summaries["clv_beat_rate"].count == 2


def test_audit_history_report_tracks_flag_codes(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = build_audit_history_report_from_bundle_root(bundle_root)

    assert report.flag_code_counts["HIGH_MODEL_MARKET_DELTA_MEAN"] == 2
    assert report.flag_code_counts["MODEL_ONLY_PROBABILITIES_PRESENT"] == 2


def test_build_audit_history_report_from_manifest_paths_dedupes_paths(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)
    paths = discover_audit_manifest_paths(bundle_root)

    report = build_audit_history_report_from_manifest_paths(
        (paths[0], paths[0], paths[1]),
    )

    assert report.run_count == 2


def test_audit_history_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = build_audit_history_report_from_bundle_root(bundle_root)
    record = audit_history_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_audit_history_report" in encoded
    assert record["source"] == "fqis_audit_history_report"
    assert record["run_count"] == 2
    assert "metric_summaries" in record
    assert "runs" in record


def test_write_audit_history_report_json(tmp_path: Path) -> None:
    bundle_root = _write_two_bundles(tmp_path)

    report = build_audit_history_report_from_bundle_root(bundle_root)
    output_path = tmp_path / "audit_history_report.json"

    written_path = write_audit_history_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_audit_history_report"
    assert record["run_count"] == 2


def test_discover_audit_manifest_paths_rejects_missing_root(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        discover_audit_manifest_paths(tmp_path / "missing")


def _write_two_bundles(tmp_path: Path) -> Path:
    hybrid_batch_path, settlement_path = _write_bundle_inputs(tmp_path)
    bundle_root = tmp_path / "bundles"

    build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=bundle_root,
        run_id="history-run-a",
    )
    build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=CLOSING_PATH,
        output_dir=bundle_root,
        run_id="history-run-b",
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