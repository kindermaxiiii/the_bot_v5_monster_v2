from pathlib import Path

import pytest

from app.fqis.runtime.vnext_export_diagnostics import diagnose_vnext_export_for_fqis


def test_diagnose_vnext_export_for_fqis_on_mixed_fixture() -> None:
    report = diagnose_vnext_export_for_fqis(Path("tests/fixtures/fqis/vnext_export_mixed.jsonl"))

    assert report["status"] == "ok"
    assert report["rows_read"] == 3
    assert report["rows_valid_json"] == 3
    assert report["rows_invalid_json"] == 0

    assert report["top_level_key_counts"]["event_id"] == 3
    assert report["feature_source_counts"]["features"] == 3
    assert report["offer_source_counts"]["offers"] == 3
    assert report["p_real_source_counts"]["p_real_by_thesis"] == 3

    assert report["conversion_readiness_counts"]["convertible"] == 2
    assert report["conversion_readiness_counts"]["missing_away_xg_live"] == 1
    assert report["probably_convertible_rows"] == 2
    assert report["total_offer_row_count"] == 3


def test_diagnose_vnext_export_for_fqis_handles_invalid_json(tmp_path: Path) -> None:
    source_path = tmp_path / "bad.jsonl"

    source_path.write_text(
        '{"event_id":1,"features":{},"offers":[],"p_real_by_thesis":{}}\n'
        '{bad json}\n',
        encoding="utf-8",
    )

    report = diagnose_vnext_export_for_fqis(source_path)

    assert report["rows_read"] == 2
    assert report["rows_valid_json"] == 1
    assert report["rows_invalid_json"] == 1
    assert report["conversion_readiness_counts"]["invalid_json"] == 1


def test_diagnose_vnext_export_for_fqis_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        diagnose_vnext_export_for_fqis(tmp_path / "missing.jsonl")