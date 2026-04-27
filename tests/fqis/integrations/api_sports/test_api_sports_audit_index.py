
import json

from app.fqis.integrations.api_sports.audit_index import (
    ApiSportsAuditIndexEntry,
    build_api_sports_audit_index,
    select_latest_audit_bundle,
    write_api_sports_audit_index,
)


def _bundle(run_id, *, ready=True, created_at="2026-04-27T10:00:00+00:00", quality_status="PASS"):
    return {
        "status": "BUILT",
        "run_id": run_id,
        "ready": ready,
        "created_at_utc": created_at,
        "output_path": f"bundles/{run_id}_audit_bundle.json",
        "manifest_path": f"pipeline/{run_id}/pipeline_manifest.json",
        "files": [
            {"role": "pipeline_manifest", "path": "manifest.json", "exists": True},
            {"role": "quality_report", "path": "quality.json", "exists": quality_status != "MISSING"},
        ],
        "ledger_entry": {"run_id": run_id},
        "manifest": {"status": "COMPLETED" if ready else "FAILED"},
        "quality_report": None if quality_status == "MISSING" else {"status": quality_status},
        "errors": [] if ready else ["PIPELINE_STATUS_FAILED"],
    }


def test_audit_index_entry_from_file(tmp_path):
    path = tmp_path / "run-1_audit_bundle.json"
    path.write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    entry = ApiSportsAuditIndexEntry.from_file(path)

    assert entry.run_id == "run-1"
    assert entry.status == "BUILT"
    assert entry.ready is True
    assert entry.pipeline_status == "COMPLETED"
    assert entry.quality_status == "PASS"
    assert entry.files_total == 2
    assert entry.files_missing == 0
    assert entry.bundle_sha256


def test_build_audit_index_counts_and_latest_ready(tmp_path):
    (tmp_path / "run-1_audit_bundle.json").write_text(
        json.dumps(_bundle("run-1", ready=False, created_at="2026-04-27T10:00:00+00:00", quality_status="BLOCKED")),
        encoding="utf-8",
    )
    (tmp_path / "run-2_audit_bundle.json").write_text(
        json.dumps(_bundle("run-2", ready=True, created_at="2026-04-27T11:00:00+00:00", quality_status="PASS")),
        encoding="utf-8",
    )

    index = build_api_sports_audit_index(bundle_dir=tmp_path)

    assert index.status == "BUILT"
    assert index.bundles_total == 2
    assert index.ready_total == 1
    assert index.latest_run_id == "run-2"
    assert index.latest_ready_run_id == "run-2"
    assert index.status_counts["BUILT"] == 2
    assert index.quality_status_counts["PASS"] == 1


def test_write_audit_index_creates_output_and_ignores_existing_index(tmp_path):
    (tmp_path / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")
    output_path = tmp_path / "audit_bundle_index.json"

    index = write_api_sports_audit_index(bundle_dir=tmp_path, output_path=output_path)

    assert output_path.exists()
    assert index.bundles_total == 1

    rebuilt = build_api_sports_audit_index(bundle_dir=tmp_path)
    assert rebuilt.bundles_total == 1


def test_select_latest_audit_bundle_filters_ready_and_quality(tmp_path):
    (tmp_path / "run-1_audit_bundle.json").write_text(
        json.dumps(_bundle("run-1", ready=True, created_at="2026-04-27T10:00:00+00:00", quality_status="WARN")),
        encoding="utf-8",
    )
    (tmp_path / "run-2_audit_bundle.json").write_text(
        json.dumps(_bundle("run-2", ready=True, created_at="2026-04-27T11:00:00+00:00", quality_status="PASS")),
        encoding="utf-8",
    )

    entry = select_latest_audit_bundle(bundle_dir=tmp_path, ready=True, quality_status="PASS")

    assert entry is not None
    assert entry.run_id == "run-2"


def test_build_audit_index_records_invalid_bundle_errors(tmp_path):
    (tmp_path / "bad.json").write_text("{bad-json", encoding="utf-8")

    index = build_api_sports_audit_index(bundle_dir=tmp_path)

    assert index.status == "BUILT_WITH_ERRORS"
    assert index.bundles_total == 0
    assert index.errors
