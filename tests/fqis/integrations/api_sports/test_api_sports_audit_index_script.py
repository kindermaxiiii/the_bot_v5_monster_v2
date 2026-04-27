
import json

from scripts.fqis_api_sports_audit_index import main


def _bundle(run_id, *, ready=True, created_at="2026-04-27T10:00:00+00:00"):
    return {
        "status": "BUILT",
        "run_id": run_id,
        "ready": ready,
        "created_at_utc": created_at,
        "output_path": f"{run_id}_audit_bundle.json",
        "manifest_path": f"{run_id}/pipeline_manifest.json",
        "files": [{"role": "pipeline_manifest", "path": "manifest.json", "exists": True}],
        "ledger_entry": {"run_id": run_id},
        "manifest": {"status": "COMPLETED" if ready else "FAILED"},
        "quality_report": {"status": "PASS" if ready else "BLOCKED"},
        "errors": [] if ready else ["PIPELINE_STATUS_FAILED"],
    }


def test_audit_index_script_writes_index(tmp_path, capsys):
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()
    output_path = tmp_path / "audit_bundle_index.json"
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    code = main(["--bundle-dir", str(bundle_dir), "--output", str(output_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "BUILT"
    assert payload["bundles_total"] == 1
    assert output_path.exists()


def test_audit_index_script_selects_latest_ready(tmp_path, capsys):
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()
    (bundle_dir / "run-1_audit_bundle.json").write_text(
        json.dumps(_bundle("run-1", ready=False, created_at="2026-04-27T10:00:00+00:00")),
        encoding="utf-8",
    )
    (bundle_dir / "run-2_audit_bundle.json").write_text(
        json.dumps(_bundle("run-2", ready=True, created_at="2026-04-27T11:00:00+00:00")),
        encoding="utf-8",
    )

    code = main(["--bundle-dir", str(bundle_dir), "--latest-ready"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "FOUND"
    assert payload["entry"]["run_id"] == "run-2"


def test_audit_index_script_require_missing_selection_returns_non_zero(tmp_path, capsys):
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1", ready=False)), encoding="utf-8")

    code = main(["--bundle-dir", str(bundle_dir), "--latest-ready", "--require"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == "NOT_FOUND"


def test_audit_index_script_missing_dir_is_safe(tmp_path, capsys):
    output_path = tmp_path / "index.json"

    code = main(["--bundle-dir", str(tmp_path / "missing"), "--output", str(output_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "BUILT_WITH_ERRORS"
    assert output_path.exists()
