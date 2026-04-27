
import json

import pytest

from app.fqis.integrations.api_sports.audit_bundle import (
    ApiSportsAuditBundleError,
    build_api_sports_audit_bundle,
    write_api_sports_audit_bundle,
)


def _manifest_payload(tmp_path, *, run_id="run-1", status="COMPLETED", ready=True, normalized_input=None):
    return {
        "run_id": run_id,
        "status": status,
        "ready": ready,
        "run_dir": str(tmp_path),
        "normalized_input": normalized_input,
        "payload_sha256": "payload-sha",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps": [
            {"name": "quality_gate", "status": "COMPLETED"},
            {"name": "replay", "status": "COMPLETED"},
        ],
        "errors": [],
    }


def test_audit_bundle_builds_from_manifest_and_quality_report(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    quality_path = tmp_path / "quality_report.json"
    normalized_path = tmp_path / "normalized.json"

    normalized_path.write_text(json.dumps({"fixtures": [], "odds_offers": []}), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(_manifest_payload(tmp_path, normalized_input=str(normalized_path))),
        encoding="utf-8",
    )
    quality_path.write_text(json.dumps({"status": "PASS", "ready": True, "issues": []}), encoding="utf-8")

    bundle = build_api_sports_audit_bundle(manifest_path)

    assert bundle.status == "BUILT"
    assert bundle.run_id == "run-1"
    assert bundle.ready is True
    assert bundle.quality_report["status"] == "PASS"
    assert {item.role for item in bundle.files} == {
        "pipeline_manifest",
        "quality_report",
        "normalized_input",
    }


def test_audit_bundle_marks_missing_quality_report_not_ready(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    manifest_path.write_text(json.dumps(_manifest_payload(tmp_path)), encoding="utf-8")

    bundle = build_api_sports_audit_bundle(manifest_path)

    assert bundle.ready is False
    assert "MISSING_QUALITY_REPORT" in bundle.errors


def test_audit_bundle_tracks_missing_normalized_input(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    missing_normalized = tmp_path / "missing_normalized.json"

    manifest_path.write_text(
        json.dumps(_manifest_payload(tmp_path, normalized_input=str(missing_normalized))),
        encoding="utf-8",
    )
    (tmp_path / "quality_report.json").write_text(
        json.dumps({"status": "PASS", "ready": True, "issues": []}),
        encoding="utf-8",
    )

    bundle = build_api_sports_audit_bundle(manifest_path)

    normalized_file = [item for item in bundle.files if item.role == "normalized_input"][0]
    assert normalized_file.exists is False
    assert any(error.startswith("MISSING_FILE:normalized_input") for error in bundle.errors)


def test_write_audit_bundle_creates_output_file(tmp_path):
    manifest_path = tmp_path / "pipeline_manifest.json"
    output_dir = tmp_path / "bundles"

    manifest_path.write_text(json.dumps(_manifest_payload(tmp_path)), encoding="utf-8")
    (tmp_path / "quality_report.json").write_text(
        json.dumps({"status": "PASS", "ready": True, "issues": []}),
        encoding="utf-8",
    )

    bundle = write_api_sports_audit_bundle(manifest_path, output_dir=output_dir)

    assert bundle.output_path is not None
    output_path = output_dir / "run-1_audit_bundle.json"
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "run-1"


def test_missing_manifest_raises(tmp_path):
    with pytest.raises(ApiSportsAuditBundleError):
        build_api_sports_audit_bundle(tmp_path / "missing.json")
