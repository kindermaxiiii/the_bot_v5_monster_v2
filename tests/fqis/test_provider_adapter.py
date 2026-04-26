from pathlib import Path

import pytest

from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl
from app.fqis.runtime.provider_adapter import (
    build_provider_snapshot_from_jsonl,
    build_semilive_rows_from_provider_rows,
    load_provider_source_rows,
)


def test_load_provider_source_rows_from_fixture() -> None:
    rows = load_provider_source_rows(Path("tests/fixtures/fqis/provider_source_valid.jsonl"))

    assert len(rows) == 8
    assert rows[0]["row_type"] == "match"
    assert rows[1]["row_type"] == "offer"


def test_build_semilive_rows_from_provider_rows() -> None:
    rows = load_provider_source_rows(Path("tests/fixtures/fqis/provider_source_valid.jsonl"))

    semilive_rows = build_semilive_rows_from_provider_rows(rows)

    assert len(semilive_rows) == 2

    assert semilive_rows[0]["event_id"] == 2101
    assert semilive_rows[0]["match_label"] == "Alpha FC vs Beta FC"
    assert len(semilive_rows[0]["offers"]) == 2
    assert "LOW_AWAY_SCORING_HAZARD" in semilive_rows[0]["p_real_by_thesis"]

    assert semilive_rows[1]["event_id"] == 2102
    assert semilive_rows[1]["match_label"] == "Gamma FC vs Delta FC"
    assert len(semilive_rows[1]["offers"]) == 1
    assert "LOW_HOME_SCORING_HAZARD" in semilive_rows[1]["p_real_by_thesis"]


def test_build_provider_snapshot_from_jsonl_is_valid_fqis_input(tmp_path: Path) -> None:
    source_path = Path("tests/fixtures/fqis/provider_source_valid.jsonl")
    output_path = tmp_path / "fqis_provider_snapshot.jsonl"

    result = build_provider_snapshot_from_jsonl(source_path, output_path)

    inputs = load_shadow_inputs_from_jsonl(output_path)
    inspection = inspect_shadow_input_file(output_path)

    assert result.record_count == 2
    assert result.total_offer_count == 3
    assert result.event_ids == (2101, 2102)
    assert len(inputs) == 2
    assert inspection["status"] == "ok"
    assert inspection["match_count"] == 2
    assert inspection["total_offer_count"] == 3


def test_provider_adapter_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_provider_source_rows(tmp_path / "missing.jsonl")


def test_provider_adapter_rejects_missing_match_row(tmp_path: Path) -> None:
    source_path = tmp_path / "missing_match.jsonl"

    source_path.write_text(
        '{"row_type":"offer","event_id":2201,"bookmaker_name":"BookA","family":"BTTS","side":"NO","period":"FT","team_role":"NONE","line":null,"odds_decimal":1.80}\n'
        '{"row_type":"probability","event_id":2201,"thesis_key":"LOW_AWAY_SCORING_HAZARD","intent_key":"BTTS|NO|NONE|NA","p_real":0.58}\n',
        encoding="utf-8",
    )

    rows = load_provider_source_rows(source_path)

    with pytest.raises(ValueError):
        build_semilive_rows_from_provider_rows(rows)


def test_provider_adapter_rejects_unknown_row_type(tmp_path: Path) -> None:
    source_path = tmp_path / "unknown_type.jsonl"

    source_path.write_text(
        '{"row_type":"unknown","event_id":2202}\n',
        encoding="utf-8",
    )

    rows = load_provider_source_rows(source_path)

    with pytest.raises(ValueError):
        build_semilive_rows_from_provider_rows(rows)

        