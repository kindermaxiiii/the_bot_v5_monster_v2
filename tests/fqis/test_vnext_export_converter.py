from pathlib import Path

import pytest

from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl
from app.fqis.runtime.vnext_export_converter import convert_vnext_export_to_fqis_input


def test_convert_vnext_export_to_fqis_input_with_mixed_rows(tmp_path: Path) -> None:
    source_path = Path("tests/fixtures/fqis/vnext_export_mixed.jsonl")
    output_path = tmp_path / "fqis_input.jsonl"

    report = convert_vnext_export_to_fqis_input(source_path, output_path)

    assert report.rows_read == 3
    assert report.rows_converted == 2
    assert report.rows_rejected == 1
    assert report.rejection_reasons["missing_away_xg_live"] == 1

    inputs = load_shadow_inputs_from_jsonl(output_path)

    assert len(inputs) == 2
    assert inputs[0].live_match_row["event_id"] == 1601
    assert inputs[1].live_match_row["event_id"] == 1603


def test_convert_vnext_export_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        convert_vnext_export_to_fqis_input(
            tmp_path / "missing.jsonl",
            tmp_path / "out.jsonl",
        )


def test_convert_vnext_export_writes_empty_file_when_no_rows_convert(tmp_path: Path) -> None:
    source_path = tmp_path / "all_bad.jsonl"
    output_path = tmp_path / "out.jsonl"

    source_path.write_text(
        '{"event_id":1701,"features":{"home_xg_live":0.10},"offers":[],"p_real_by_thesis":{}}\n',
        encoding="utf-8",
    )

    report = convert_vnext_export_to_fqis_input(source_path, output_path)

    assert report.rows_read == 1
    assert report.rows_converted == 0
    assert report.rows_rejected == 1
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8") == ""

    