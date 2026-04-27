from __future__ import annotations

import json
from pathlib import Path

from app.fqis.runtime.model_shadow import (
    build_demo_model_shadow_input,
    model_shadow_cycle_to_record,
    run_model_shadow_cycle,
    write_model_shadow_jsonl,
)


def test_build_demo_model_shadow_input() -> None:
    shadow_input = build_demo_model_shadow_input()

    assert shadow_input.event_id == 2601
    assert shadow_input.features.event_id == 2601
    assert len(shadow_input.theses) == 2
    assert len(shadow_input.offers) == 3


def test_run_model_shadow_cycle_uses_model_probabilities() -> None:
    shadow_input = build_demo_model_shadow_input()

    outcome = run_model_shadow_cycle(shadow_input)

    assert outcome.status == "ok"
    assert outcome.event_id == 2601
    assert outcome.thesis_count == 2
    assert outcome.model_probability_count >= 3
    assert all(result.p_real_source == "model" for result in outcome.thesis_results)
    assert all(result.p_real_by_intent_key for result in outcome.thesis_results)


def test_run_model_shadow_cycle_accepts_at_least_one_bet() -> None:
    shadow_input = build_demo_model_shadow_input()

    outcome = run_model_shadow_cycle(shadow_input)

    assert outcome.accepted_bet_count >= 1
    assert len(outcome.accepted_bets) == outcome.accepted_bet_count


def test_model_shadow_cycle_to_record_is_json_serializable() -> None:
    shadow_input = build_demo_model_shadow_input()
    outcome = run_model_shadow_cycle(shadow_input)

    record = model_shadow_cycle_to_record(outcome)

    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_model_shadow" in encoded
    assert record["event_id"] == 2601
    assert record["thesis_count"] == 2
    assert record["model_probability_count"] >= 3


def test_write_model_shadow_jsonl(tmp_path: Path) -> None:
    shadow_input = build_demo_model_shadow_input()
    outcome = run_model_shadow_cycle(shadow_input)

    output_path = tmp_path / "model_shadow.jsonl"
    written_path = write_model_shadow_jsonl(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    record = json.loads(lines[0])

    assert record["status"] == "ok"
    assert record["source"] == "fqis_model_shadow"
    assert record["event_id"] == 2601
    assert record["model_probability_count"] >= 3