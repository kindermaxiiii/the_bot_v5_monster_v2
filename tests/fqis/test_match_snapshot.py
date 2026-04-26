from pathlib import Path

from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl
from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.match_snapshot import (
    build_demo_match_snapshot_records,
    write_match_snapshot_jsonl,
)


def test_match_snapshot_fixture_is_valid_fqis_input() -> None:
    path = Path("tests/fixtures/fqis/match_snapshot_valid.jsonl")

    inputs = load_shadow_inputs_from_jsonl(path)
    report = inspect_shadow_input_file(path)

    assert len(inputs) == 1
    assert inputs[0].live_match_row["event_id"] == 1901
    assert report["status"] == "ok"
    assert report["match_count"] == 1
    assert report["total_offer_count"] == 2


def test_build_demo_match_snapshot_records_are_valid_fqis_input(tmp_path: Path) -> None:
    records = build_demo_match_snapshot_records()
    output_path = tmp_path / "snapshot.jsonl"

    result = write_match_snapshot_jsonl(records, output_path)

    inputs = load_shadow_inputs_from_jsonl(output_path)
    report = inspect_shadow_input_file(output_path)

    assert result.record_count == len(records)
    assert len(inputs) == len(records)
    assert report["status"] == "ok"
    assert report["match_count"] == len(records)
    assert report["total_offer_count"] == result.total_offer_count
    assert result.event_ids == tuple(record["event_id"] for record in records)


def test_demo_match_snapshot_records_include_source_audit() -> None:
    records = build_demo_match_snapshot_records()

    assert len(records) >= 1
    assert records[0]["snapshot_type"] == "fqis_match_level"
    assert records[0]["schema_version"] == 1
    assert records[0]["source_audit"]["source"] == "demo"

    