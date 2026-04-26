from pathlib import Path

import pytest

from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl
from app.fqis.runtime.semilive_snapshot import (
    build_semilive_match_snapshot_records,
    build_semilive_snapshot_from_jsonl,
    build_shadow_inputs_from_semilive_rows,
    load_semilive_source_rows,
)


def test_load_semilive_source_rows_from_fixture() -> None:
    rows = load_semilive_source_rows(Path("tests/fixtures/fqis/semilive_source_valid.jsonl"))

    assert len(rows) == 2
    assert rows[0]["event_id"] == 2001
    assert rows[1]["event_id"] == 2002


def test_build_shadow_inputs_from_semilive_rows() -> None:
    rows = load_semilive_source_rows(Path("tests/fixtures/fqis/semilive_source_valid.jsonl"))

    inputs = build_shadow_inputs_from_semilive_rows(rows)

    assert len(inputs) == 2
    assert inputs[0].live_match_row["event_id"] == 2001
    assert len(inputs[0].live_offer_rows) == 2
    assert inputs[1].live_match_row["event_id"] == 2002
    assert len(inputs[1].live_offer_rows) == 2


def test_build_semilive_match_snapshot_records_include_source_audit() -> None:
    rows = load_semilive_source_rows(Path("tests/fixtures/fqis/semilive_source_valid.jsonl"))

    records = build_semilive_match_snapshot_records(rows)

    assert len(records) == 2
    assert records[0]["snapshot_type"] == "fqis_match_level"
    assert records[0]["source_audit"]["source"] == "semilive_fixture"
    assert records[0]["source_audit"]["source_match_label"] == "Alpha FC vs Beta FC"


def test_build_semilive_snapshot_from_jsonl_is_valid_fqis_input(tmp_path: Path) -> None:
    source_path = Path("tests/fixtures/fqis/semilive_source_valid.jsonl")
    output_path = tmp_path / "fqis_semilive_snapshot.jsonl"

    result = build_semilive_snapshot_from_jsonl(source_path, output_path)

    inputs = load_shadow_inputs_from_jsonl(output_path)
    inspection = inspect_shadow_input_file(output_path)

    assert result.record_count == 2
    assert result.total_offer_count == 4
    assert result.event_ids == (2001, 2002)
    assert len(inputs) == 2
    assert inspection["status"] == "ok"
    assert inspection["match_count"] == 2
    assert inspection["total_offer_count"] == 4


def test_semilive_snapshot_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_semilive_source_rows(tmp_path / "missing.jsonl")


def test_semilive_snapshot_rejects_missing_live_field(tmp_path: Path) -> None:
    source_path = tmp_path / "bad.jsonl"

    source_path.write_text(
        '{"event_id":2003,"live_match":{"home_xg_live":0.1},"offers":[],"p_real_by_thesis":{"CAGEY_GAME":{}}}\n',
        encoding="utf-8",
    )

    rows = load_semilive_source_rows(source_path)

    with pytest.raises(ValueError):
        build_shadow_inputs_from_semilive_rows(rows)