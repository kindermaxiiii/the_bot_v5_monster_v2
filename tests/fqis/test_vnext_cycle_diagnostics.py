from pathlib import Path

import pytest

from app.fqis.runtime.vnext_cycle_diagnostics import diagnose_vnext_cycle_export_for_fqis


def test_diagnose_vnext_cycle_export_for_fqis_on_valid_fixture() -> None:
    report = diagnose_vnext_cycle_export_for_fqis(
        Path("tests/fixtures/fqis/vnext_cycle_export_valid.jsonl")
    )

    assert report["status"] == "ok"
    assert report["cycles_read"] == 1
    assert report["valid_json_cycles"] == 1
    assert report["invalid_json_cycles"] == 0

    assert report["collection_counts"]["fixture_audits"] == 2
    assert report["collection_counts"]["publication_records"] == 1
    assert report["collection_counts"]["payloads"] == 1
    assert report["collection_counts"]["refusal_summaries"] == 1

    assert report["fixture_audit_publish_status_counts"]["PUBLISH"] == 1
    assert report["fixture_audit_publish_status_counts"]["DO_NOT_PUBLISH"] == 1
    assert report["publication_template_counts"]["TEAM_TOTAL_AWAY_UNDER_CORE"] == 1

    assert report["availability_counts"]["publication_records_with_fixture_id"] == 1
    assert report["availability_counts"]["publication_records_with_odds_decimal"] == 1
    assert report["availability_counts"]["publication_records_with_line"] == 1

    assert report["fixture_id_coverage"]["fixture_audit_unique_fixture_ids"] == 2
    assert report["fixture_id_coverage"]["publication_record_unique_fixture_ids"] == 1
    assert report["fixture_id_coverage"]["publication_join_fixture_count"] == 1

    gaps = report["fqis_gap_assessment"]

    assert gaps["has_match_level_ids"] is True
    assert gaps["has_publication_prices"] is True
    assert gaps["has_template_keys"] is True
    assert gaps["has_required_live_features"] is False
    assert gaps["has_p_real_by_thesis"] is False
    assert gaps["can_reconstruct_price_candidates"] is True
    assert gaps["can_build_full_fqis_input"] is False
    assert gaps["recommended_next_step"] == "create_match_level_export_with_features_and_probabilities"


def test_diagnose_vnext_cycle_export_handles_invalid_json(tmp_path: Path) -> None:
    source_path = tmp_path / "bad_cycle.jsonl"

    source_path.write_text(
        '{"cycle_id":1,"fixture_audits":[],"publication_records":[]}\n'
        '{bad json}\n',
        encoding="utf-8",
    )

    report = diagnose_vnext_cycle_export_for_fqis(source_path)

    assert report["cycles_read"] == 2
    assert report["valid_json_cycles"] == 1
    assert report["invalid_json_cycles"] == 1


def test_diagnose_vnext_cycle_export_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        diagnose_vnext_cycle_export_for_fqis(tmp_path / "missing.jsonl")