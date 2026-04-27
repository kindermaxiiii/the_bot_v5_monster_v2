
import json

from scripts.fqis_api_sports_run_ledger import main


def _manifest_payload():
    return {
        "run_id": "script-run",
        "status": "COMPLETED",
        "ready": True,
        "run_dir": None,
        "normalized_input": "normalized.json",
        "payload_sha256": "abc",
        "started_at_utc": "2026-04-27T10:00:00+00:00",
        "completed_at_utc": "2026-04-27T10:01:00+00:00",
        "steps": [{"name": "quality_gate", "status": "COMPLETED"}],
        "errors": [],
    }


def test_run_ledger_script_missing_manifest_returns_safe_failure(tmp_path, capsys):
    code = main(["--manifest", str(tmp_path / "missing.json"), "--ledger", str(tmp_path / "ledger.jsonl")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"


def test_run_ledger_script_records_manifest_and_summary(tmp_path, capsys):
    manifest_path = tmp_path / "pipeline_manifest.json"
    ledger_path = tmp_path / "run_ledger.jsonl"
    manifest_path.write_text(json.dumps(_manifest_payload()), encoding="utf-8")

    code = main(["--manifest", str(manifest_path), "--ledger", str(ledger_path), "--summary"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "RECORDED"
    assert payload["summary"]["runs_total"] == 1
    assert ledger_path.exists()


def test_run_ledger_script_summary_without_manifest(tmp_path, capsys):
    code = main(["--ledger", str(tmp_path / "run_ledger.jsonl")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "SUMMARY"
    assert payload["summary"]["runs_total"] == 0
