from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.orchestration.shadow_production import (
    ShadowProductionConfig,
    run_shadow_production,
    shadow_production_outcome_to_record,
    write_shadow_production_outcome_json,
)


INPUT_PATH = Path("tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl")
RESULTS_PATH = Path("tests/fixtures/fqis/match_results_valid.jsonl")
CLOSING_PATH = Path("tests/fixtures/fqis/closing_odds_valid.jsonl")


def test_run_shadow_production_creates_outputs(tmp_path: Path) -> None:
    outcome = run_shadow_production(
        ShadowProductionConfig(
            input_path=INPUT_PATH,
            results_path=RESULTS_PATH,
            closing_path=CLOSING_PATH,
            output_root=tmp_path / "shadow_runs",
            run_id="shadow-prod-test",
        )
    )

    assert outcome.status == "ok"
    assert outcome.run_id == "shadow-prod-test"
    assert outcome.hybrid_batch.match_count == 2
    assert outcome.hybrid_batch.accepted_bet_count == 3
    assert outcome.settlement.settled_bet_count == 3
    assert outcome.audit_bundle.file_count == 8
    assert outcome.readiness.run_count == 1
    assert outcome.readiness_status == "NO_GO"
    assert outcome.readiness_level in {"REVIEW_REQUIRED", "BLOCKED"}

    assert Path(outcome.output_dir).exists()
    assert Path(outcome.hybrid_batch_path).exists()
    assert Path(outcome.settlement_path).exists()
    assert Path(outcome.bundle_dir).exists()
    assert Path(outcome.readiness_path).exists()


def test_shadow_production_outcome_to_record_is_json_serializable(tmp_path: Path) -> None:
    outcome = run_shadow_production(
        ShadowProductionConfig(
            input_path=INPUT_PATH,
            results_path=RESULTS_PATH,
            closing_path=CLOSING_PATH,
            output_root=tmp_path / "shadow_runs",
            run_id="json-shadow-prod",
        )
    )

    record = shadow_production_outcome_to_record(outcome)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_shadow_production_outcome" in encoded
    assert record["source"] == "fqis_shadow_production_outcome"
    assert record["run_id"] == "json-shadow-prod"
    assert record["headline"]["accepted_bets"] == 3
    assert record["headline"]["settled_bets"] == 3
    assert "audit_bundle_manifest" in record
    assert "readiness_report" in record


def test_write_shadow_production_outcome_json(tmp_path: Path) -> None:
    outcome = run_shadow_production(
        ShadowProductionConfig(
            input_path=INPUT_PATH,
            results_path=RESULTS_PATH,
            closing_path=CLOSING_PATH,
            output_root=tmp_path / "shadow_runs",
            run_id="write-shadow-prod",
        )
    )

    output_path = tmp_path / "shadow_outcome.json"
    written_path = write_shadow_production_outcome_json(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    record = json.loads(output_path.read_text(encoding="utf-8"))

    assert record["status"] == "ok"
    assert record["source"] == "fqis_shadow_production_outcome"
    assert record["run_id"] == "write-shadow-prod"


def test_shadow_production_uses_stake(tmp_path: Path) -> None:
    outcome = run_shadow_production(
        ShadowProductionConfig(
            input_path=INPUT_PATH,
            results_path=RESULTS_PATH,
            closing_path=CLOSING_PATH,
            output_root=tmp_path / "shadow_runs",
            run_id="stake-shadow-prod",
            stake=2.0,
        )
    )

    assert outcome.settlement.total_staked == 6.0
    assert outcome.settlement.total_profit == pytest.approx(5.24)


def test_shadow_production_missing_input_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_shadow_production(
            ShadowProductionConfig(
                input_path=tmp_path / "missing_input.jsonl",
                results_path=RESULTS_PATH,
                closing_path=CLOSING_PATH,
                output_root=tmp_path / "shadow_runs",
                run_id="missing-input",
            )
        )

        