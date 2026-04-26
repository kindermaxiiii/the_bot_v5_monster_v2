from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.reporting.hybrid_shadow_report import (
    build_hybrid_shadow_batch_report_from_jsonl,
    hybrid_shadow_batch_report_to_record,
    load_hybrid_shadow_batch_records_from_jsonl,
    write_hybrid_shadow_batch_report_json,
)
from app.fqis.runtime.hybrid_batch_shadow import (
    run_hybrid_shadow_batch_from_jsonl,
    write_hybrid_shadow_batch_jsonl,
)


FIXTURE_PATH = Path("tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl")


def test_build_hybrid_shadow_batch_report_from_jsonl(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = build_hybrid_shadow_batch_report_from_jsonl(batch_path)

    assert report.status == "ok"
    assert report.batch_count == 1
    assert report.match_count == 2
    assert report.thesis_count == 3
    assert report.accepted_bet_count >= 1
    assert report.hybrid_probability_count >= 4
    assert report.hybrid_count >= 1
    assert report.model_only_count >= 1
    assert report.has_probabilities


def test_hybrid_shadow_batch_report_contains_numeric_summaries(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = build_hybrid_shadow_batch_report_from_jsonl(batch_path)

    assert report.numeric_summaries["p_model"].count >= 4
    assert report.numeric_summaries["p_hybrid"].count >= 4
    assert report.numeric_summaries["p_hybrid"].mean is not None
    assert report.numeric_summaries["p_hybrid"].minimum is not None
    assert report.numeric_summaries["p_hybrid"].maximum is not None
    assert report.numeric_summaries["p_market_no_vig"].count >= 1
    assert report.numeric_summaries["delta_model_market"].count >= 1


def test_hybrid_shadow_batch_report_counts_sources_and_intents(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = build_hybrid_shadow_batch_report_from_jsonl(batch_path)

    assert report.source_counts["hybrid"] >= 1
    assert report.source_counts["model_only"] >= 1
    assert "BTTS|NO|NONE|NA" in report.intent_counts


def test_hybrid_shadow_batch_report_to_record_is_json_serializable(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = build_hybrid_shadow_batch_report_from_jsonl(batch_path)
    record = hybrid_shadow_batch_report_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_hybrid_shadow_batch_report" in encoded
    assert "numeric_summaries" in record
    assert record["match_count"] == 2
    assert record["hybrid_probability_count"] >= 4


def test_write_hybrid_shadow_batch_report_json(tmp_path: Path) -> None:
    batch_path = _write_batch_output(tmp_path)

    report = build_hybrid_shadow_batch_report_from_jsonl(batch_path)
    output_path = tmp_path / "report.json"

    written_path = write_hybrid_shadow_batch_report_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_hybrid_shadow_batch_report"
    assert record["match_count"] == 2


def test_load_hybrid_shadow_batch_records_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_hybrid_shadow_batch_records_from_jsonl(tmp_path / "missing.jsonl")


def test_load_hybrid_shadow_batch_records_rejects_empty_file(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError):
        load_hybrid_shadow_batch_records_from_jsonl(path)


def _write_batch_output(tmp_path: Path) -> Path:
    outcome = run_hybrid_shadow_batch_from_jsonl(FIXTURE_PATH)
    batch_path = tmp_path / "hybrid_shadow_batch.jsonl"

    write_hybrid_shadow_batch_jsonl(outcome, batch_path)

    return batch_path

    