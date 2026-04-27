
import json

from app.fqis.integrations.api_sports.release_gate import ApiSportsReleaseGateConfig
from app.fqis.integrations.api_sports.release_manifest import (
    build_api_sports_release_manifest,
    load_api_sports_release_manifest,
    write_api_sports_release_manifest,
)


def _ledger_entry(run_id, *, ready=True):
    return {
        "run_id": run_id,
        "status": "COMPLETED" if ready else "FAILED",
        "ready": ready,
        "ledger_key": f"{run_id}:sha",
        "manifest_path": f"{run_id}/pipeline_manifest.json",
        "manifest_sha256": f"{run_id}-sha",
        "steps_total": 2,
        "steps_completed": 2 if ready else 1,
        "steps_failed": 0 if ready else 1,
        "errors_total": 0 if ready else 1,
        "quality_status": "PASS" if ready else "BLOCKED",
        "quality_ready": ready,
        "quality_issues_total": 0 if ready else 1,
    }


def _bundle(run_id, *, ready=True):
    return {
        "status": "BUILT",
        "run_id": run_id,
        "ready": ready,
        "created_at_utc": "2026-04-27T10:00:00+00:00",
        "output_path": f"{run_id}_audit_bundle.json",
        "manifest_path": f"{run_id}/pipeline_manifest.json",
        "files": [{"role": "pipeline_manifest", "path": "manifest.json", "exists": True}],
        "ledger_entry": {"run_id": run_id},
        "manifest": {"status": "COMPLETED" if ready else "FAILED"},
        "quality_report": {"status": "PASS" if ready else "BLOCKED"},
        "errors": [] if ready else ["PIPELINE_STATUS_FAILED"],
    }


def _write_ledger(path, entries):
    path.write_text(
        "\n".join(json.dumps(entry) for entry in entries) + "\n",
        encoding="utf-8",
    )


def test_release_manifest_is_ready_when_release_gate_is_ready(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    manifest = build_api_sports_release_manifest(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        include_git=False,
    )

    assert manifest.status == "READY"
    assert manifest.release_ready is True
    assert manifest.release_id.startswith("api-sports-level2-")
    assert manifest.errors == ()
    assert manifest.git_commit is None
    assert manifest.git_branch is None


def test_release_manifest_blocks_when_release_gate_blocks(tmp_path):
    manifest = build_api_sports_release_manifest(
        ledger_path=tmp_path / "missing_ledger.jsonl",
        bundle_dir=tmp_path / "missing_bundles",
        include_git=False,
    )

    assert manifest.status == "BLOCKED"
    assert manifest.release_ready is False
    assert "RELEASE_GATE_BLOCKED" in manifest.errors


def test_release_manifest_tracks_missing_release_gate_artifact(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    manifest = build_api_sports_release_manifest(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        release_gate_path=tmp_path / "missing_release_gate.json",
        include_git=False,
    )

    assert manifest.status == "BLOCKED"
    assert manifest.release_ready is False
    assert any(error.startswith("MISSING_ARTIFACT:release_gate") for error in manifest.errors)


def test_write_and_load_release_manifest(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    output_path = tmp_path / "release_manifest.json"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    written = write_api_sports_release_manifest(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        output_path=output_path,
        include_git=False,
    )
    loaded = load_api_sports_release_manifest(output_path)

    assert output_path.exists()
    assert written.manifest_path == str(output_path)
    assert loaded.release_id == written.release_id
    assert loaded.status == "READY"
    assert loaded.release_ready is True


def test_release_manifest_can_allow_warning_release(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1"), _ledger_entry("run-2")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    manifest = build_api_sports_release_manifest(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        config=ApiSportsReleaseGateConfig(allow_warnings=True),
        include_git=False,
    )

    assert manifest.status == "READY"
    assert manifest.release_ready is True
    assert manifest.release_gate["status"] == "WARN"
