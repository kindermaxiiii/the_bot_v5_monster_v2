import json
from pathlib import Path

import pytest

from app.fqis.contracts.enums import ThesisKey
from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl


def test_load_shadow_inputs_from_jsonl(tmp_path: Path) -> None:
    input_path = tmp_path / "fqis_shadow_input.jsonl"

    payload = {
        "live_match_row": {
            "event_id": 1301,
            "home_xg_live": 0.95,
            "away_xg_live": 0.18,
            "home_shots_on_target": 4,
            "away_shots_on_target": 1,
            "minute": 58,
            "home_score": 1,
            "away_score": 0,
        },
        "live_offer_rows": [
            {
                "event_id": 1301,
                "bookmaker_id": 1,
                "bookmaker_name": "BookA",
                "family": "TEAM_TOTAL_AWAY",
                "side": "UNDER",
                "period": "FT",
                "team_role": "AWAY",
                "line": 1.5,
                "odds_decimal": 1.92,
                "source_timestamp_utc": "2026-04-26T00:00:00+00:00",
                "freshness_seconds": 8,
            }
        ],
        "p_real_by_thesis": {
            "LOW_AWAY_SCORING_HAZARD": {
                "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62
            }
        },
    }

    input_path.write_text(json.dumps(payload), encoding="utf-8")

    inputs = load_shadow_inputs_from_jsonl(input_path)

    assert len(inputs) == 1
    assert inputs[0].live_match_row["event_id"] == 1301
    assert len(inputs[0].live_offer_rows) == 1
    assert ThesisKey.LOW_AWAY_SCORING_HAZARD in inputs[0].p_real_by_thesis


def test_load_shadow_inputs_from_jsonl_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_shadow_inputs_from_jsonl(tmp_path / "missing.jsonl")


def test_load_shadow_inputs_from_jsonl_rejects_unknown_thesis_key(tmp_path: Path) -> None:
    input_path = tmp_path / "bad.jsonl"

    payload = {
        "live_match_row": {"event_id": 1301},
        "live_offer_rows": [],
        "p_real_by_thesis": {
            "UNKNOWN_THESIS": {}
        },
    }

    input_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        load_shadow_inputs_from_jsonl(input_path)

def test_load_shadow_inputs_from_jsonl_accepts_utf8_bom(tmp_path: Path) -> None:
    input_path = tmp_path / "bom_input.jsonl"

    payload = {
        "live_match_row": {
            "event_id": 1302,
            "home_xg_live": 0.95,
            "away_xg_live": 0.18,
            "home_shots_on_target": 4,
            "away_shots_on_target": 1,
            "minute": 58,
            "home_score": 1,
            "away_score": 0,
        },
        "live_offer_rows": [],
        "p_real_by_thesis": {
            "LOW_AWAY_SCORING_HAZARD": {
                "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.62
            }
        },
    }

    input_path.write_text(json.dumps(payload), encoding="utf-8-sig")

    inputs = load_shadow_inputs_from_jsonl(input_path)

    assert len(inputs) == 1
    assert inputs[0].live_match_row["event_id"] == 1302

    