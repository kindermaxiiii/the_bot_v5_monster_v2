from pathlib import Path

from app.fqis.runtime.input_inspector import inspect_shadow_input_file


def test_inspect_shadow_input_file_returns_summary() -> None:
    report = inspect_shadow_input_file(Path("tests/fixtures/fqis/shadow_input_valid.jsonl"))

    assert report["status"] == "ok"
    assert report["match_count"] == 1
    assert report["total_offer_count"] == 2
    assert report["has_duplicates"] is False
    assert "LOW_AWAY_SCORING_HAZARD" in report["thesis_keys"]