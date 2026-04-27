from __future__ import annotations

import json
from pathlib import Path

from app.fqis.monitoring.shadow_monitor import (
    MonitoredShadowRunnerConfig,
    monitored_shadow_runner_outcome_to_record,
    read_shadow_run_events,
    run_monitored_shadow_runner,
    write_monitored_shadow_runner_outcome_json,
)


def test_monitored_shadow_runner_writes_started_and_completed_events(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)
    event_log_path = tmp_path / "ops" / "run_events.jsonl"
    latest_status_path = tmp_path / "ops" / "latest_status.json"

    outcome = run_monitored_shadow_runner(
        MonitoredShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="monitored-ok",
            event_log_path=event_log_path,
            latest_status_path=latest_status_path,
        )
    )

    assert outcome.status == "ok"
    assert outcome.is_success
    assert outcome.runner_outcome is not None
    assert outcome.run_id == "monitored-ok"
    assert outcome.event_log_path == str(event_log_path)
    assert outcome.latest_status_path == str(latest_status_path)

    events = read_shadow_run_events(event_log_path)

    assert [event["event_type"] for event in events] == ["STARTED", "COMPLETED"]
    assert events[-1]["status"] == "ok"
    assert events[-1]["headline"]["accepted_bets"] == 3

    latest = json.loads(latest_status_path.read_text(encoding="utf-8"))

    assert latest["source"] == "fqis_shadow_latest_status"
    assert latest["event_type"] == "COMPLETED"
    assert latest["status"] == "ok"
    assert latest["runner_outcome"]["run_id"] == "monitored-ok"


def test_monitored_shadow_runner_captures_failures(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path, input_path="missing/input.jsonl")
    event_log_path = tmp_path / "ops" / "failed_events.jsonl"
    latest_status_path = tmp_path / "ops" / "latest_failed.json"

    outcome = run_monitored_shadow_runner(
        MonitoredShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="monitored-fail",
            event_log_path=event_log_path,
            latest_status_path=latest_status_path,
        )
    )

    assert outcome.status == "failed"
    assert not outcome.is_success
    assert not outcome.is_go
    assert outcome.runner_outcome is None
    assert outcome.error is not None
    assert outcome.error["error_type"] == "FileNotFoundError"

    events = read_shadow_run_events(event_log_path)

    assert [event["event_type"] for event in events] == ["STARTED", "FAILED"]
    assert events[-1]["status"] == "failed"
    assert events[-1]["error"]["error_type"] == "FileNotFoundError"

    latest = json.loads(latest_status_path.read_text(encoding="utf-8"))

    assert latest["event_type"] == "FAILED"
    assert latest["status"] == "failed"
    assert latest["error"]["error_type"] == "FileNotFoundError"
    assert latest["runner_outcome"] is None


def test_monitored_shadow_runner_uses_default_monitoring_paths(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_monitored_shadow_runner(
        MonitoredShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="default-paths",
        )
    )

    assert outcome.status == "ok"
    assert outcome.event_log_path == str(tmp_path / "runs" / "run_events.jsonl")
    assert outcome.latest_status_path == str(tmp_path / "runs" / "latest_status.json")
    assert Path(outcome.event_log_path).exists()
    assert Path(outcome.latest_status_path).exists()


def test_monitored_shadow_runner_outcome_to_record_is_json_serializable(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_monitored_shadow_runner(
        MonitoredShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="json-monitor",
        )
    )

    record = monitored_shadow_runner_outcome_to_record(outcome)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_monitored_shadow_runner_outcome" in encoded
    assert record["source"] == "fqis_monitored_shadow_runner_outcome"
    assert record["status"] == "ok"
    assert record["run_id"] == "json-monitor"
    assert record["runner_outcome"] is not None


def test_write_monitored_shadow_runner_outcome_json(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_monitored_shadow_runner(
        MonitoredShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="write-monitor",
        )
    )

    output_path = tmp_path / "monitor_outcome.json"
    written_path = write_monitored_shadow_runner_outcome_json(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["source"] == "fqis_monitored_shadow_runner_outcome"
    assert record["status"] == "ok"
    assert record["run_id"] == "write-monitor"


def _write_profile(
    tmp_path: Path,
    *,
    input_path: str = "tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl",
) -> Path:
    profile_path = tmp_path / "profiles.json"
    profile_path.write_text(
        json.dumps(
            {
                "profiles": {
                    "test": {
                        "input_path": input_path,
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

    return profile_path
    