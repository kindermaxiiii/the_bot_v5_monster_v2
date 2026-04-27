from __future__ import annotations

import json
from pathlib import Path

from app.fqis.orchestration.shadow_runner import (
    ShadowRunnerConfig,
    run_shadow_runner,
    shadow_runner_outcome_to_record,
    write_shadow_runner_outcome_json,
)


def test_shadow_runner_creates_outcome_and_latest(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="runner-test-a",
        )
    )

    assert outcome.status == "ok"
    assert outcome.run_id == "runner-test-a"
    assert outcome.readiness_status == "NO_GO"
    assert outcome.readiness_level == "BLOCKED"
    assert not outcome.is_go

    assert Path(outcome.outcome_path).exists()
    assert outcome.latest_path is not None
    assert Path(outcome.latest_path).exists()


def test_shadow_runner_shared_history_reaches_review_required(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    first = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="runner-history-a",
        )
    )
    second = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="runner-history-b",
        )
    )

    assert first.readiness_level == "BLOCKED"
    assert first.shadow_outcome.readiness.run_count == 1

    assert second.readiness_level == "REVIEW_REQUIRED"
    assert second.shadow_outcome.readiness.run_count == 2
    assert second.shadow_outcome.readiness.failure_count == 0


def test_shadow_runner_can_disable_latest(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="no-latest",
            write_latest=False,
        )
    )

    assert outcome.latest_path is None
    assert Path(outcome.outcome_path).exists()


def test_shadow_runner_custom_output_paths(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)
    outcome_output_path = tmp_path / "custom" / "outcome.json"
    latest_output_path = tmp_path / "custom" / "latest.json"

    outcome = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="custom-paths",
            outcome_output_path=outcome_output_path,
            latest_output_path=latest_output_path,
        )
    )

    assert outcome.outcome_path == str(outcome_output_path)
    assert outcome.latest_path == str(latest_output_path)
    assert outcome_output_path.exists()
    assert latest_output_path.exists()


def test_shadow_runner_outcome_to_record_is_json_serializable(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="json-runner",
        )
    )

    record = shadow_runner_outcome_to_record(outcome)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_shadow_runner_outcome" in encoded
    assert record["source"] == "fqis_shadow_runner_outcome"
    assert record["run_id"] == "json-runner"
    assert record["profile"]["name"] == "test"
    assert "shadow_production_outcome" in record


def test_write_shadow_runner_outcome_json(tmp_path: Path) -> None:
    profile_path = _write_profile(tmp_path)

    outcome = run_shadow_runner(
        ShadowRunnerConfig(
            profile_name="test",
            profile_path=profile_path,
            run_id="write-runner",
        )
    )

    output_path = tmp_path / "runner_outcome.json"
    written_path = write_shadow_runner_outcome_json(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_shadow_runner_outcome"
    assert record["run_id"] == "write-runner"


def _write_profile(tmp_path: Path) -> Path:
    profile_path = tmp_path / "profiles.json"
    profile_path.write_text(
        json.dumps(
            {
                "profiles": {
                    "test": {
                        "input_path": "tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl",
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