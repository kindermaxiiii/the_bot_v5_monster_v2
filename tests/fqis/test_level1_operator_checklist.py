from __future__ import annotations

import json
from pathlib import Path

from app.fqis.operations.operator_checklist import (
    build_level1_operator_checklist,
    level1_operator_checklist_to_record,
    write_level1_operator_checklist_json,
)


def test_level1_operator_checklist_demo_is_ready_with_possible_warnings(tmp_path: Path) -> None:
    missing_latest_status = tmp_path / "missing_latest_status.json"

    report = build_level1_operator_checklist(
        profile_name="demo",
        latest_status_path=missing_latest_status,
    )

    assert report.status == "ok"
    assert report.profile_name == "demo"
    assert report.readiness == "READY"
    assert report.is_ready
    assert report.fail_count == 0
    assert report.warn_count >= 1

    codes = {item.code for item in report.items}

    assert "PROFILE_LOAD" in codes
    assert "SHADOW_SCRIPT_PRESENT" in codes
    assert "INPUT_PATH_EXISTS" in codes
    assert "RESULTS_PATH_EXISTS" in codes
    assert "CLOSING_PATH_EXISTS" in codes
    assert "SHADOW_ONLY_POLICY" in codes


def test_level1_operator_checklist_blocks_failed_latest_status(tmp_path: Path) -> None:
    latest_status_path = tmp_path / "latest_failed.json"
    latest_status_path.write_text(
        json.dumps(
            {
                "source": "fqis_shadow_latest_status",
                "event_type": "FAILED",
                "status": "failed",
                "error": {"error_type": "FileNotFoundError"},
            }
        ),
        encoding="utf-8",
    )

    report = build_level1_operator_checklist(
        profile_name="demo",
        latest_status_path=latest_status_path,
    )

    assert report.readiness == "BLOCKED"
    assert not report.is_ready
    assert report.fail_count >= 1

    latest_item = next(item for item in report.items if item.code == "LATEST_STATUS_PRESENT")

    assert latest_item.status == "FAIL"
    assert latest_item.blocking


def test_level1_operator_checklist_blocks_missing_input_from_custom_profile(tmp_path: Path) -> None:
    profile_path = tmp_path / "profiles.json"
    profile_path.write_text(
        json.dumps(
            {
                "profiles": {
                    "broken": {
                        "input_path": "missing/input.jsonl",
                        "results_path": "tests/fixtures/fqis/match_results_valid.jsonl",
                        "closing_path": "tests/fixtures/fqis/closing_odds_valid.jsonl",
                        "output_root": str(tmp_path / "runs"),
                        "audit_bundle_root": str(tmp_path / "history"),
                        "stake": 1.0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    report = build_level1_operator_checklist(
        profile_name="broken",
        profile_path=profile_path,
        latest_status_path=tmp_path / "missing_latest.json",
    )

    assert report.readiness == "BLOCKED"
    assert report.fail_count >= 1

    input_item = next(item for item in report.items if item.code == "INPUT_PATH_EXISTS")

    assert input_item.status == "FAIL"
    assert input_item.blocking


def test_level1_operator_checklist_record_is_json_serializable(tmp_path: Path) -> None:
    report = build_level1_operator_checklist(
        profile_name="demo",
        latest_status_path=tmp_path / "missing_latest.json",
    )

    record = level1_operator_checklist_to_record(report)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_level1_operator_checklist" in encoded
    assert record["source"] == "fqis_level1_operator_checklist"
    assert record["profile_name"] == "demo"
    assert "items" in record


def test_write_level1_operator_checklist_json(tmp_path: Path) -> None:
    report = build_level1_operator_checklist(
        profile_name="demo",
        latest_status_path=tmp_path / "missing_latest.json",
    )

    output_path = tmp_path / "level1_operator_checklist.json"
    written_path = write_level1_operator_checklist_json(report, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["source"] == "fqis_level1_operator_checklist"
    assert record["status"] == "ok"


def test_level1_docs_exist_and_contain_core_commands() -> None:
    runbook = Path("docs/fqis/LEVEL1_OPERATOR_RUNBOOK.md")
    checklist = Path("docs/fqis/LEVEL1_LAUNCH_CHECKLIST.md")

    assert runbook.exists()
    assert checklist.exists()

    runbook_text = runbook.read_text(encoding="utf-8")
    checklist_text = checklist.read_text(encoding="utf-8")

    assert "scripts\\fqis_shadow.py" in runbook_text or "scripts/fqis_shadow.py" in runbook_text
    assert "scripts\\fqis_level1_checklist.py" in runbook_text or "scripts/fqis_level1_checklist.py" in runbook_text
    assert "No real-money staking" in runbook_text
    assert "Run shadow" in checklist_text
    assert "Shadow-only policy" in checklist_text
