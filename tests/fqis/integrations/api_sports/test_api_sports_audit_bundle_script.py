
import json

from scripts.fqis_api_sports_audit_bundle import main


def _manifest_payload(tmp_path, *, run_id="script-run", ready=True):
    return {
        "run_id": run_id,
        "status": "COMPLETED",
        "ready": ready,
        "run_dir": str(tmp_path),
        "normalized_input": None,
        "payload_sha256": "payload-sha",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps": [{"name": "quality_gate", "status": "COMPLETED"}],
        "errors": [],
    }


def _ledger_entry(run_id, manifest_path):
    return {
        "run_id": run_id,
        "status": "COMPLETED",
        "ready": True,
        "ledger_key": f"{run_id}:sha",
        "manifest_path": str(manifest_path),
        "manifest_sha256": f"{run_id}-sha",
        "run_dir": str(manifest_path.parent),
        "normalized_input": None,
        "payload_sha256": "payload-sha",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps_total": 1,
        "steps_completed": 1,
        "steps_failed": 0,
        "errors_total": 0,
        "quality_status": "PASS",
        "quality_ready": True,
        "quality_issues_total": 0,
    }


def test_audit_bundle_script_missing_manifest_returns_safe_failure(tmp_path, capsys):
    code = main(["--manifest", str(tmp_path / "missing.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"


def test_audit_bundle_script_writes_direct_manifest_bundle(tmp_path, capsys):
    manifest_path = tmp_path / "pipeline_manifest.json"
    output_path = tmp_path / "bundle.json"

    manifest_path.write_text(json.dumps(_manifest_payload(tmp_path)), encoding="utf-8")
    (tmp_path / "quality_report.json").write_text(
        json.dumps({"status": "PASS", "ready": True, "issues": []}),
        encoding="utf-8",
    )

    code = main(["--manifest", str(manifest_path), "--output", str(output_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "BUILT"
    assert output_path.exists()


def test_audit_bundle_script_resolves_latest_ready_from_ledger(tmp_path, capsys):
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    manifest_path = run_dir / "pipeline_manifest.json"
    ledger_path = tmp_path / "run_ledger.jsonl"

    manifest_path.write_text(json.dumps(_manifest_payload(run_dir, run_id="run-1")), encoding="utf-8")
    (run_dir / "quality_report.json").write_text(
        json.dumps({"status": "PASS", "ready": True, "issues": []}),
        encoding="utf-8",
    )
    ledger_path.write_text(json.dumps(_ledger_entry("run-1", manifest_path)) + "\n", encoding="utf-8")

    code = main(
        [
            "--ledger",
            str(ledger_path),
            "--latest-ready",
            "--output-dir",
            str(tmp_path / "bundles"),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["run_id"] == "run-1"
    assert payload["ready"] is True
