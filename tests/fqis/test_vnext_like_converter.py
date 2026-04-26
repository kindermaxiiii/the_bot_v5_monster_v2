import json
from pathlib import Path

import pytest

from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl
from app.fqis.runtime.vnext_like_converter import convert_vnext_like_export_to_fqis_input


def test_convert_vnext_like_export_to_fqis_input(tmp_path: Path) -> None:
    source_path = Path("tests/fixtures/fqis/vnext_like_export_valid.jsonl")
    output_path = tmp_path / "fqis_shadow_input.jsonl"

    result = convert_vnext_like_export_to_fqis_input(source_path, output_path)

    assert result.row_count == 1
    assert result.total_offer_count == 2
    assert output_path.exists()

    inputs = load_shadow_inputs_from_jsonl(output_path)

    assert len(inputs) == 1
    assert inputs[0].live_match_row["event_id"] == 1501
    assert len(inputs[0].live_offer_rows) == 2


def test_convert_vnext_like_export_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        convert_vnext_like_export_to_fqis_input(
            tmp_path / "missing.jsonl",
            tmp_path / "out.jsonl",
        )


def test_convert_vnext_like_export_rejects_missing_features(tmp_path: Path) -> None:
    source_path = tmp_path / "bad.jsonl"
    output_path = tmp_path / "out.jsonl"

    payload = {
        "event_id": 1502,
        "features": {
            "home_xg_live": 0.95
        },
        "offers": [],
        "p_real_by_thesis": {},
    }

    source_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        convert_vnext_like_export_to_fqis_input(source_path, output_path)


def test_convert_vnext_like_export_rejects_missing_offer_fields(tmp_path: Path) -> None:
    source_path = tmp_path / "bad_offer.jsonl"
    output_path = tmp_path / "out.jsonl"

    payload = {
        "event_id": 1503,
        "features": {
            "home_xg_live": 0.95,
            "away_xg_live": 0.18,
            "home_shots_on_target": 4,
            "away_shots_on_target": 1,
            "minute": 58,
            "home_score": 1,
            "away_score": 0,
        },
        "offers": [
            {
                "bookmaker_name": "BookA",
                "family": "TEAM_TOTAL_AWAY",
            }
        ],
        "p_real_by_thesis": {},
    }

    source_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        convert_vnext_like_export_to_fqis_input(source_path, output_path)

        