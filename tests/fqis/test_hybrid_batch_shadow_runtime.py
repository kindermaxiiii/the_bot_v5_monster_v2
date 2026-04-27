from __future__ import annotations

import json
from pathlib import Path

from app.fqis.runtime.hybrid_batch_shadow import (
    hybrid_shadow_batch_to_record,
    run_hybrid_shadow_batch,
    run_hybrid_shadow_batch_from_jsonl,
    write_hybrid_shadow_batch_jsonl,
)
from app.fqis.runtime.model_batch_shadow import load_model_shadow_inputs_from_jsonl


FIXTURE_PATH = Path("tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl")


def test_run_hybrid_shadow_batch_from_jsonl() -> None:
    outcome = run_hybrid_shadow_batch_from_jsonl(FIXTURE_PATH)

    assert outcome.status == "ok"
    assert outcome.match_count == 2
    assert outcome.thesis_count == 3
    assert outcome.accepted_match_count >= 1
    assert outcome.accepted_bet_count >= 1
    assert outcome.hybrid_probability_count >= 4
    assert outcome.hybrid_count >= 1
    assert outcome.model_only_count >= 1
    assert 0.0 <= outcome.acceptance_rate <= 1.0


def test_run_hybrid_shadow_batch_accepts_loaded_inputs() -> None:
    inputs = load_model_shadow_inputs_from_jsonl(FIXTURE_PATH)

    outcome = run_hybrid_shadow_batch(inputs, source_path=FIXTURE_PATH)

    assert outcome.source_path == FIXTURE_PATH
    assert outcome.match_count == 2
    assert len(outcome.cycle_outcomes) == 2
    assert outcome.cycle_outcomes[0].hybrid_count >= 1


def test_hybrid_shadow_batch_to_record_is_json_serializable() -> None:
    outcome = run_hybrid_shadow_batch_from_jsonl(FIXTURE_PATH)

    record = hybrid_shadow_batch_to_record(outcome)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_hybrid_shadow_batch" in encoded
    assert "p_model" in encoded
    assert "p_market_no_vig" in encoded
    assert "p_hybrid" in encoded
    assert record["match_count"] == 2
    assert len(record["cycles"]) == 2


def test_hybrid_shadow_batch_record_contains_aggregate_diagnostics() -> None:
    outcome = run_hybrid_shadow_batch_from_jsonl(FIXTURE_PATH)

    record = hybrid_shadow_batch_to_record(outcome)

    assert record["source"] == "fqis_hybrid_shadow_batch"
    assert record["hybrid_probability_count"] >= 4
    assert record["hybrid_count"] >= 1
    assert record["model_only_count"] >= 1

    first_cycle = record["cycles"][0]
    first_result = first_cycle["thesis_results"][0]

    assert "hybrid_probability_diagnostics" in first_result
    assert first_result["hybrid_probability_diagnostics"]

    diagnostic = first_result["hybrid_probability_diagnostics"][0]

    assert "intent_key" in diagnostic
    assert "p_model" in diagnostic
    assert "p_market_no_vig" in diagnostic
    assert "p_hybrid" in diagnostic
    assert "source" in diagnostic
    assert "delta_model_market" in diagnostic
    assert "model_weight" in diagnostic
    assert "market_weight" in diagnostic


def test_write_hybrid_shadow_batch_jsonl(tmp_path: Path) -> None:
    outcome = run_hybrid_shadow_batch_from_jsonl(FIXTURE_PATH)

    output_path = tmp_path / "hybrid_shadow_batch.jsonl"
    written_path = write_hybrid_shadow_batch_jsonl(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    record = json.loads(lines[0])

    assert record["status"] == "ok"
    assert record["source"] == "fqis_hybrid_shadow_batch"
    assert record["match_count"] == 2
    assert len(record["cycles"]) == 2
    assert record["hybrid_probability_count"] >= 4


def test_hybrid_shadow_batch_keeps_cycle_level_counts() -> None:
    outcome = run_hybrid_shadow_batch_from_jsonl(FIXTURE_PATH)

    first_cycle = outcome.cycle_outcomes[0]
    second_cycle = outcome.cycle_outcomes[1]

    assert first_cycle.event_id == 3101
    assert second_cycle.event_id == 3102
    assert first_cycle.hybrid_count >= 1
    assert second_cycle.model_only_count >= 1

    