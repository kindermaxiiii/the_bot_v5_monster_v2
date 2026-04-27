
import json

from scripts.fqis_api_sports_operator_report import main


def _ledger_entry(run_id, *, status="COMPLETED", ready=True):
    return {
        "run_id": run_id,
        "status": status,
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


def test_operator_report_script_missing_inputs_require_ready_returns_non_zero(tmp_path, capsys):
    code = main(
        [
            "--ledger",
            str(tmp_path / "missing_ledger.jsonl"),
            "--bundle-dir",
            str(tmp_path / "missing_bundles"),
            "--require-ready",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == "BLOCKED"
    assert payload["ready"] is False


def test_operator_report_script_writes_output(tmp_path, capsys):
    ledger_path = tmp_path / "run_ledger.jsonl"
    bundle_dir = tmp_path / "bundles"
    output_path = tmp_path / "operator_report.json"
    bundle_dir.mkdir()

    _write_ledger(ledger_path, [_ledger_entry("run-1")])
    (bundle_dir / "run-1_audit_bundle.json").write_text(json.dumps(_bundle("run-1")), encoding="utf-8")

    code = main(
        [
            "--ledger",
            str(ledger_path),
            "--bundle-dir",
            str(bundle_dir),
            "--output",
            str(output_path),
            "--require-ready",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "PASS"
    assert output_path.exists()


def test_operator_report_script_allows_missing_bundle_when_not_required(tmp_path, capsys):
    ledger_path = tmp_path / "run_ledger.jsonl"
    _write_ledger(ledger_path, [_ledger_entry("run-1")])

    code = main(
        [
            "--ledger",
            str(ledger_path),
            "--bundle-dir",
            str(tmp_path / "missing_bundles"),
            "--no-require-audit-bundle",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "WARN"
    assert payload["ready"] is True
