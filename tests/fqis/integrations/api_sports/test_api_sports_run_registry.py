
import json

import pytest

from app.fqis.integrations.api_sports.run_registry import (
    ApiSportsRunRegistry,
    ApiSportsRunRegistryError,
)


def _entry(run_id, status="COMPLETED", ready=True, quality_status="PASS"):
    return {
        "run_id": run_id,
        "status": status,
        "ready": ready,
        "ledger_key": f"{run_id}:sha",
        "manifest_path": f"data/pipeline/api_sports/{run_id}/pipeline_manifest.json",
        "manifest_sha256": f"{run_id}-sha",
        "run_dir": f"data/pipeline/api_sports/{run_id}",
        "normalized_input": "data/normalized/api_sports/sample.json",
        "payload_sha256": "payload-sha",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps_total": 2,
        "steps_completed": 2 if status == "COMPLETED" else 1,
        "steps_failed": 0 if status == "COMPLETED" else 1,
        "errors_total": 0 if status == "COMPLETED" else 1,
        "quality_status": quality_status,
        "quality_ready": quality_status == "PASS",
        "quality_issues_total": 0 if quality_status == "PASS" else 1,
    }


def _write_ledger(path, entries):
    path.write_text(
        "\n".join(json.dumps(entry) for entry in entries) + "\n",
        encoding="utf-8",
    )


def test_registry_selects_latest_run_by_ledger_order(tmp_path):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(ledger, [_entry("run-1"), _entry("run-2")])

    registry = ApiSportsRunRegistry(ledger)

    assert registry.latest().run_id == "run-2"
    assert registry.snapshot().latest_run_id == "run-2"


def test_registry_filters_latest_ready_completed_run(tmp_path):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(
        ledger,
        [
            _entry("run-1", "COMPLETED", True, "PASS"),
            _entry("run-2", "FAILED", False, "BLOCKED"),
            _entry("run-3", "COMPLETED", True, "WARN"),
        ],
    )

    registry = ApiSportsRunRegistry(ledger)

    assert registry.latest(status="COMPLETED", ready=True).run_id == "run-3"
    assert registry.latest(status="FAILED").run_id == "run-2"
    assert registry.latest(quality_status="PASS").run_id == "run-1"


def test_registry_find_run_id_uses_latest_duplicate(tmp_path):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(
        ledger,
        [
            _entry("run-1", "FAILED", False, "BLOCKED"),
            _entry("run-1", "COMPLETED", True, "PASS"),
        ],
    )

    registry = ApiSportsRunRegistry(ledger)

    entry = registry.find_run_id("run-1")
    assert entry is not None
    assert entry.status == "COMPLETED"
    assert entry.ready is True


def test_registry_require_latest_raises_when_no_match(tmp_path):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(ledger, [_entry("run-1", "FAILED", False, "BLOCKED")])

    registry = ApiSportsRunRegistry(ledger)

    with pytest.raises(ApiSportsRunRegistryError):
        registry.require_latest(status="COMPLETED", ready=True)


def test_registry_snapshot_counts_statuses(tmp_path):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(
        ledger,
        [
            _entry("run-1", "COMPLETED", True, "PASS"),
            _entry("run-2", "FAILED", False, "BLOCKED"),
        ],
    )

    snapshot = ApiSportsRunRegistry(ledger).snapshot()

    assert snapshot.entries_total == 2
    assert snapshot.ready_total == 1
    assert snapshot.status_counts["COMPLETED"] == 1
    assert snapshot.status_counts["FAILED"] == 1
    assert snapshot.quality_status_counts["PASS"] == 1
    assert snapshot.quality_status_counts["BLOCKED"] == 1
