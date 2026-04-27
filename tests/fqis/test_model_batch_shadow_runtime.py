from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.runtime.model_batch_shadow import (
    load_model_shadow_inputs_from_jsonl,
    model_shadow_batch_to_record,
    run_model_shadow_batch,
    run_model_shadow_batch_from_jsonl,
    write_model_shadow_batch_jsonl,
)


FIXTURE_PATH = Path("tests/fixtures/fqis/model_shadow_input_valid.jsonl")


def test_load_model_shadow_inputs_from_jsonl() -> None:
    inputs = load_model_shadow_inputs_from_jsonl(FIXTURE_PATH)

    assert len(inputs) == 2
    assert inputs[0].event_id == 2701
    assert inputs[1].event_id == 2702
    assert len(inputs[0].theses) == 2
    assert len(inputs[0].offers) == 3
    assert len(inputs[1].theses) == 1
    assert len(inputs[1].offers) == 2


def test_run_model_shadow_batch_from_jsonl() -> None:
    outcome = run_model_shadow_batch_from_jsonl(FIXTURE_PATH)

    assert outcome.status == "ok"
    assert outcome.match_count == 2
    assert outcome.thesis_count == 3
    assert outcome.accepted_match_count >= 1
    assert outcome.accepted_bet_count >= 1
    assert outcome.model_probability_count >= 4
    assert 0.0 <= outcome.acceptance_rate <= 1.0


def test_run_model_shadow_batch_accepts_loaded_inputs() -> None:
    inputs = load_model_shadow_inputs_from_jsonl(FIXTURE_PATH)
    outcome = run_model_shadow_batch(inputs, source_path=FIXTURE_PATH)

    assert outcome.source_path == FIXTURE_PATH
    assert outcome.match_count == 2
    assert len(outcome.cycle_outcomes) == 2


def test_model_shadow_batch_to_record_is_json_serializable() -> None:
    outcome = run_model_shadow_batch_from_jsonl(FIXTURE_PATH)

    record = model_shadow_batch_to_record(outcome)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_model_shadow_batch" in encoded
    assert record["match_count"] == 2
    assert len(record["cycles"]) == 2
    assert record["model_probability_count"] >= 4


def test_write_model_shadow_batch_jsonl(tmp_path: Path) -> None:
    outcome = run_model_shadow_batch_from_jsonl(FIXTURE_PATH)

    output_path = tmp_path / "model_shadow_batch.jsonl"
    written_path = write_model_shadow_batch_jsonl(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    record = json.loads(lines[0])

    assert record["status"] == "ok"
    assert record["source"] == "fqis_model_shadow_batch"
    assert record["match_count"] == 2
    assert len(record["cycles"]) == 2


def test_load_model_shadow_inputs_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_model_shadow_inputs_from_jsonl(tmp_path / "missing.jsonl")


def test_load_model_shadow_inputs_rejects_missing_live_match(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text(
        '{"live_offer_rows":[],"theses":[{"thesis_key":"CAGEY_GAME"}]}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_model_shadow_inputs_from_jsonl(path)