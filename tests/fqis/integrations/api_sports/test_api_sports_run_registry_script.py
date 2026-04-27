
import json

from scripts.fqis_api_sports_run_registry import main


def _entry(run_id, status="COMPLETED", ready=True, quality_status="PASS"):
    return {
        "run_id": run_id,
        "status": status,
        "ready": ready,
        "ledger_key": f"{run_id}:sha",
        "manifest_path": f"{run_id}/pipeline_manifest.json",
        "manifest_sha256": f"{run_id}-sha",
        "steps_total": 2,
        "steps_completed": 2,
        "steps_failed": 0,
        "errors_total": 0,
        "quality_status": quality_status,
        "quality_ready": quality_status == "PASS",
        "quality_issues_total": 0,
    }


def _write_ledger(path, entries):
    path.write_text(
        "\n".join(json.dumps(entry) for entry in entries) + "\n",
        encoding="utf-8",
    )


def test_run_registry_script_latest_missing_is_safe(tmp_path, capsys):
    code = main(["--ledger", str(tmp_path / "missing.jsonl")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "NOT_FOUND"


def test_run_registry_script_require_missing_returns_non_zero(tmp_path, capsys):
    code = main(["--ledger", str(tmp_path / "missing.jsonl"), "--require"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == "NOT_FOUND"


def test_run_registry_script_resolves_latest_ready_run(tmp_path, capsys):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(
        ledger,
        [
            _entry("run-1", "FAILED", False, "BLOCKED"),
            _entry("run-2", "COMPLETED", True, "PASS"),
        ],
    )

    code = main(["--ledger", str(ledger), "--ready-only"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "FOUND"
    assert payload["entry"]["run_id"] == "run-2"


def test_run_registry_script_lists_with_limit(tmp_path, capsys):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(
        ledger,
        [
            _entry("run-1"),
            _entry("run-2"),
            _entry("run-3"),
        ],
    )

    code = main(["--ledger", str(ledger), "--list", "--limit", "2"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "LIST"
    assert payload["entries_total"] == 2
    assert [entry["run_id"] for entry in payload["entries"]] == ["run-2", "run-3"]


def test_run_registry_script_find_run_id(tmp_path, capsys):
    ledger = tmp_path / "run_ledger.jsonl"
    _write_ledger(ledger, [_entry("target-run")])

    code = main(["--ledger", str(ledger), "--run-id", "target-run"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["status"] == "FOUND"
    assert payload["entry"]["run_id"] == "target-run"
