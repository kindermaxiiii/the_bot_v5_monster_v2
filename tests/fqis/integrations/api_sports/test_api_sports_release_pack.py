
import json

from app.fqis.integrations.api_sports.release_gate import ApiSportsReleaseGateConfig
from app.fqis.integrations.api_sports.release_pack import (
    build_api_sports_release_pack,
    load_api_sports_release_pack,
    write_api_sports_release_pack,
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


def test_release_pack_ready_when_release_manifest_is_ready(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    pack = build_api_sports_release_pack(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        include_git=False,
    )

    assert pack.status == "READY"
    assert pack.release_ready is True
    assert pack.release_id.startswith("api-sports-level2-")
    assert pack.errors == ()


def test_release_pack_blocks_when_release_manifest_blocks(tmp_path):
    pack = build_api_sports_release_pack(
        ledger_path=tmp_path / "missing_ledger.jsonl",
        bundle_dir=tmp_path / "missing_bundles",
        include_git=False,
    )

    assert pack.status == "BLOCKED"
    assert pack.release_ready is False
    assert "RELEASE_MANIFEST_BLOCKED" in pack.errors


def test_write_release_pack_creates_manifest_and_pack(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    manifest_path = tmp_path / "release_manifest.json"
    pack_path = tmp_path / "release_pack.json"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    pack = write_api_sports_release_pack(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        release_manifest_output_path=manifest_path,
        output_path=pack_path,
        include_git=False,
    )

    assert pack.status == "READY"
    assert manifest_path.exists()
    assert pack_path.exists()
    assert any(item.role == "release_manifest" and item.exists for item in pack.files)


def test_load_release_pack_roundtrips(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    manifest_path = tmp_path / "release_manifest.json"
    pack_path = tmp_path / "release_pack.json"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    written = write_api_sports_release_pack(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        release_manifest_output_path=manifest_path,
        output_path=pack_path,
        include_git=False,
    )
    loaded = load_api_sports_release_pack(pack_path)

    assert loaded.release_id == written.release_id
    assert loaded.status == "READY"
    assert loaded.release_ready is True


def test_release_pack_can_allow_warning_release(tmp_path):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1"), _ledger_entry("run-2")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    pack = build_api_sports_release_pack(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        config=ApiSportsReleaseGateConfig(allow_warnings=True),
        include_git=False,
    )

    assert pack.status == "READY"
    assert pack.release_ready is True
    assert pack.release_manifest["release_gate"]["status"] == "WARN"
