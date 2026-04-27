
import json

import pytest

from app.fqis.integrations.api_sports.run_ledger import (
    ApiSportsRunLedgerError,
    append_run_ledger_entry,
    build_run_ledger_entry,
    read_run_ledger,
    record_pipeline_manifest,
    summarize_run_ledger,
)


def _manifest_payload(run_id="run-1", status="COMPLETED", ready=True):
    return {
        "run_id": run_id,
        "status": status,
        "ready": ready,
        "run_dir": None,
        "normalized_input": "data/normalized/api_sports/sample.json",
        "payload_sha256": "abc",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps": [
            {"name": "quality_gate", "status": "COMPLETED"},
            {"name": "replay", "status": "COMPLETED"},
        ],
        "errors": [],
    }


def test_build_run_ledger_entry_from_manifest(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    manifest_path.write_text(json.dumps(_manifest_payload()), encoding="utf-8")

    entry = build_run_ledger_entry(manifest_path)

    assert entry.run_id == "run-1"
    assert entry.status == "COMPLETED"
    assert entry.ready is True
    assert entry.steps_total == 2
    assert entry.steps_completed == 2
    assert entry.steps_failed == 0
    assert entry.manifest_sha256
    assert entry.ledger_key == f"{entry.run_id}:{entry.manifest_sha256}"


def test_run_ledger_reads_quality_report_when_present(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    quality_path = tmp_path / "quality_report.json"
    manifest_path.write_text(json.dumps(_manifest_payload()), encoding="utf-8")
    quality_path.write_text(
        json.dumps({"status": "PASS", "ready": True, "issues": []}),
        encoding="utf-8",
    )

    entry = build_run_ledger_entry(manifest_path)

    assert entry.quality_status == "PASS"
    assert entry.quality_ready is True
    assert entry.quality_issues_total == 0


def test_append_run_ledger_entry_is_idempotent(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    ledger_path = tmp_path / "run_ledger.jsonl"
    manifest_path.write_text(json.dumps(_manifest_payload()), encoding="utf-8")

    entry = build_run_ledger_entry(manifest_path)

    assert append_run_ledger_entry(ledger_path, entry) is True
    assert append_run_ledger_entry(ledger_path, entry) is False
    assert len(read_run_ledger(ledger_path)) == 1


def test_record_pipeline_manifest_and_summary(tmp_path):
    manifest_1 = tmp_path / "m1.json"
    manifest_2 = tmp_path / "m2.json"
    ledger_path = tmp_path / "run_ledger.jsonl"

    manifest_1.write_text(json.dumps(_manifest_payload("run-1", "COMPLETED", True)), encoding="utf-8")
    manifest_2.write_text(json.dumps(_manifest_payload("run-2", "FAILED", False)), encoding="utf-8")

    record_pipeline_manifest(manifest_1, ledger_path=ledger_path)
    record_pipeline_manifest(manifest_2, ledger_path=ledger_path)

    summary = summarize_run_ledger(ledger_path)

    assert summary.runs_total == 2
    assert summary.runs_ready == 1
    assert summary.status_counts["COMPLETED"] == 1
    assert summary.status_counts["FAILED"] == 1
    assert summary.latest_run_id == "run-2"
    assert summary.latest_ready is False


def test_missing_manifest_raises(tmp_path):
    with pytest.raises(ApiSportsRunLedgerError):
        build_run_ledger_entry(tmp_path / "missing.json")
