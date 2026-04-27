
import json
from pathlib import Path

from app.fqis.integrations.api_sports.pipeline import (
    ApiSportsPipelineCommandResult,
    ApiSportsPipelineConfig,
    ApiSportsPipelineRunner,
    ApiSportsPipelineStatus,
    ApiSportsPipelineStepStatus,
)


def _normalized_payload():
    return {
        "fixtures": [{"fixture_key": "api_sports:fixture:1"}],
        "odds_offers": [
            {
                "fixture_key": "api_sports:fixture:1",
                "provider_bookmaker_id": "8",
                "bookmaker_name": "Book",
                "provider_market_key": "api_sports:pre_match:5",
                "mapping_status": "MAPPED",
                "normalization_status": "OK",
                "selection": "OVER",
                "line": 2.5,
                "decimal_odds": 1.91,
            }
        ],
    }


def test_pipeline_dry_run_writes_manifest_without_input(tmp_path):
    runner = ApiSportsPipelineRunner(
        ApiSportsPipelineConfig(
            normalized_input=tmp_path / "missing.json",
            output_dir=tmp_path / "pipeline",
            run_id="dry",
            dry_run=True,
        )
    )

    manifest = runner.run()

    assert manifest.status is ApiSportsPipelineStatus.DRY_RUN
    assert manifest.ready is False
    assert [step.status for step in manifest.steps] == [
        ApiSportsPipelineStepStatus.DRY_RUN,
        ApiSportsPipelineStepStatus.DRY_RUN,
    ]
    assert (tmp_path / "pipeline" / "dry" / "pipeline_manifest.json").exists()


def test_pipeline_missing_input_fails_safely(tmp_path):
    runner = ApiSportsPipelineRunner(
        ApiSportsPipelineConfig(
            normalized_input=tmp_path / "missing.json",
            output_dir=tmp_path / "pipeline",
            run_id="missing",
        )
    )

    manifest = runner.run()

    assert manifest.status is ApiSportsPipelineStatus.FAILED
    assert manifest.ready is False
    assert "INPUT_NOT_FOUND" in manifest.errors[0]


def test_pipeline_success_calls_quality_then_replay(tmp_path):
    input_path = tmp_path / "normalized.json"
    input_path.write_text(json.dumps(_normalized_payload()), encoding="utf-8")
    calls = []

    def fake_runner(command):
        calls.append(tuple(command))
        return ApiSportsPipelineCommandResult(return_code=0, stdout='{"status":"OK"}')

    runner = ApiSportsPipelineRunner(
        ApiSportsPipelineConfig(
            normalized_input=input_path,
            output_dir=tmp_path / "pipeline",
            run_id="ok",
        ),
        command_runner=fake_runner,
    )

    manifest = runner.run()

    assert manifest.status is ApiSportsPipelineStatus.COMPLETED
    assert manifest.ready is True
    assert len(calls) == 2
    assert "fqis_api_sports_quality_gate.py" in calls[0][1]
    assert "fqis_api_sports_replay.py" in calls[1][1]
    assert manifest.payload_sha256 is not None


def test_pipeline_stops_when_quality_gate_fails(tmp_path):
    input_path = tmp_path / "normalized.json"
    input_path.write_text(json.dumps(_normalized_payload()), encoding="utf-8")
    calls = []

    def fake_runner(command):
        calls.append(tuple(command))
        return ApiSportsPipelineCommandResult(return_code=1, stderr="blocked")

    runner = ApiSportsPipelineRunner(
        ApiSportsPipelineConfig(
            normalized_input=input_path,
            output_dir=tmp_path / "pipeline",
            run_id="blocked",
        ),
        command_runner=fake_runner,
    )

    manifest = runner.run()

    assert manifest.status is ApiSportsPipelineStatus.FAILED
    assert len(calls) == 1
    assert manifest.steps[0].status is ApiSportsPipelineStepStatus.FAILED
