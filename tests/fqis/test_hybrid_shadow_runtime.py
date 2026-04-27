from __future__ import annotations

import json
from pathlib import Path

from app.fqis.runtime.hybrid_shadow import (
    build_demo_hybrid_shadow_input,
    hybrid_shadow_cycle_to_record,
    run_hybrid_shadow_cycle,
    write_hybrid_shadow_jsonl,
)


def test_build_demo_hybrid_shadow_input() -> None:
    shadow_input = build_demo_hybrid_shadow_input()

    assert shadow_input.event_id == 3001
    assert shadow_input.features.event_id == 3001
    assert len(shadow_input.theses) == 2
    assert len(shadow_input.offers) == 6


def test_run_hybrid_shadow_cycle_produces_auditable_probabilities() -> None:
    shadow_input = build_demo_hybrid_shadow_input()

    outcome = run_hybrid_shadow_cycle(shadow_input)

    assert outcome.status == "ok"
    assert outcome.event_id == 3001
    assert outcome.thesis_count == 2
    assert outcome.hybrid_probability_count >= 3
    assert outcome.hybrid_count >= 1
    assert all(result.p_real_source == "hybrid" for result in outcome.thesis_results)
    assert all(result.p_real_by_intent_key for result in outcome.thesis_results)


def test_run_hybrid_shadow_cycle_accepts_at_least_one_bet() -> None:
    shadow_input = build_demo_hybrid_shadow_input()

    outcome = run_hybrid_shadow_cycle(shadow_input)

    assert outcome.accepted_bet_count >= 1
    assert len(outcome.accepted_bets) == outcome.accepted_bet_count


def test_hybrid_shadow_cycle_to_record_contains_diagnostics() -> None:
    shadow_input = build_demo_hybrid_shadow_input()
    outcome = run_hybrid_shadow_cycle(shadow_input)

    record = hybrid_shadow_cycle_to_record(outcome)

    assert record["source"] == "fqis_hybrid_shadow"
    assert record["event_id"] == 3001
    assert record["hybrid_probability_count"] >= 3
    assert record["hybrid_count"] >= 1

    first_result = record["thesis_results"][0]

    assert "p_real_by_intent_key" in first_result
    assert "model_p_real_by_intent_key" in first_result
    assert "market_p_real_by_intent_key" in first_result
    assert "hybrid_probability_diagnostics" in first_result

    diagnostic = first_result["hybrid_probability_diagnostics"][0]

    assert "intent_key" in diagnostic
    assert "p_model" in diagnostic
    assert "p_market_no_vig" in diagnostic
    assert "p_hybrid" in diagnostic
    assert "source" in diagnostic
    assert "delta_model_market" in diagnostic
    assert "model_weight" in diagnostic
    assert "market_weight" in diagnostic


def test_hybrid_shadow_cycle_to_record_is_json_serializable() -> None:
    shadow_input = build_demo_hybrid_shadow_input()
    outcome = run_hybrid_shadow_cycle(shadow_input)

    record = hybrid_shadow_cycle_to_record(outcome)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_hybrid_shadow" in encoded
    assert "p_hybrid" in encoded
    assert "p_market_no_vig" in encoded
    assert "p_model" in encoded


def test_write_hybrid_shadow_jsonl(tmp_path: Path) -> None:
    shadow_input = build_demo_hybrid_shadow_input()
    outcome = run_hybrid_shadow_cycle(shadow_input)

    output_path = tmp_path / "hybrid_shadow.jsonl"
    written_path = write_hybrid_shadow_jsonl(outcome, output_path)

    assert written_path == output_path
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1

    record = json.loads(lines[0])

    assert record["status"] == "ok"
    assert record["source"] == "fqis_hybrid_shadow"
    assert record["event_id"] == 3001
    assert record["hybrid_probability_count"] >= 3

    