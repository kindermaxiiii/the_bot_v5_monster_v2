from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


_COLLECTIONS = (
    "fixture_audits",
    "publication_records",
    "payloads",
    "refusal_summaries",
)


def diagnose_vnext_cycle_export_for_fqis(source_path: Path) -> dict[str, Any]:
    if not source_path.exists():
        raise FileNotFoundError(f"vnext cycle export file not found: {source_path}")

    cycles_read = 0
    valid_json_cycles = 0
    invalid_json_cycles = 0
    non_object_cycles = 0

    top_level_key_counts: Counter[str] = Counter()

    fixture_audit_field_counts: Counter[str] = Counter()
    publication_record_field_counts: Counter[str] = Counter()
    payload_field_counts: Counter[str] = Counter()
    refusal_summary_field_counts: Counter[str] = Counter()

    fixture_audit_publish_status_counts: Counter[str] = Counter()
    fixture_audit_template_counts: Counter[str] = Counter()
    publication_template_counts: Counter[str] = Counter()
    publication_status_counts: Counter[str] = Counter()
    publication_disposition_counts: Counter[str] = Counter()

    fixture_ids_in_fixture_audits: set[int] = set()
    fixture_ids_in_publication_records: set[int] = set()
    fixture_ids_in_payloads: set[int] = set()

    totals = {
        "fixture_audits": 0,
        "publication_records": 0,
        "payloads": 0,
        "refusal_summaries": 0,
    }

    availability = Counter()

    for raw_line in source_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        cycles_read += 1

        try:
            cycle = json.loads(line)
        except json.JSONDecodeError:
            invalid_json_cycles += 1
            continue

        valid_json_cycles += 1

        if not isinstance(cycle, dict):
            non_object_cycles += 1
            continue

        top_level_key_counts.update(cycle.keys())

        fixture_audits = _as_list(cycle.get("fixture_audits"))
        publication_records = _as_list(cycle.get("publication_records"))
        payloads = _as_list(cycle.get("payloads"))
        refusal_summaries = _as_list(cycle.get("refusal_summaries"))

        totals["fixture_audits"] += len(fixture_audits)
        totals["publication_records"] += len(publication_records)
        totals["payloads"] += len(payloads)
        totals["refusal_summaries"] += len(refusal_summaries)

        for item in fixture_audits:
            if not isinstance(item, dict):
                continue

            fixture_audit_field_counts.update(item.keys())

            fixture_id = _optional_int(item.get("fixture_id") or item.get("event_id"))
            if fixture_id is not None:
                fixture_ids_in_fixture_audits.add(fixture_id)
                availability["fixture_audits_with_fixture_id"] += 1

            if item.get("match_label"):
                availability["fixture_audits_with_match_label"] += 1

            if item.get("template_key"):
                availability["fixture_audits_with_template_key"] += 1
                fixture_audit_template_counts[str(item["template_key"])] += 1

            if item.get("publish_status"):
                availability["fixture_audits_with_publish_status"] += 1
                fixture_audit_publish_status_counts[str(item["publish_status"])] += 1

            if item.get("final_execution_refusal_reason"):
                availability["fixture_audits_with_refusal_reason"] += 1

        for item in publication_records:
            if not isinstance(item, dict):
                continue

            publication_record_field_counts.update(item.keys())

            fixture_id = _optional_int(item.get("fixture_id") or item.get("event_id"))
            if fixture_id is not None:
                fixture_ids_in_publication_records.add(fixture_id)
                availability["publication_records_with_fixture_id"] += 1

            if item.get("template_key"):
                availability["publication_records_with_template_key"] += 1
                publication_template_counts[str(item["template_key"])] += 1

            if item.get("bookmaker_name"):
                availability["publication_records_with_bookmaker_name"] += 1

            if item.get("odds_decimal") not in (None, ""):
                availability["publication_records_with_odds_decimal"] += 1

            if item.get("line") not in (None, ""):
                availability["publication_records_with_line"] += 1

            if item.get("public_status"):
                availability["publication_records_with_public_status"] += 1
                publication_status_counts[str(item["public_status"])] += 1

            if item.get("disposition"):
                availability["publication_records_with_disposition"] += 1
                publication_disposition_counts[str(item["disposition"])] += 1

        for item in payloads:
            if not isinstance(item, dict):
                continue

            payload_field_counts.update(item.keys())

            fixture_id = _optional_int(item.get("fixture_id") or item.get("event_id"))
            if fixture_id is not None:
                fixture_ids_in_payloads.add(fixture_id)
                availability["payloads_with_fixture_id"] += 1

            if item.get("template_key"):
                availability["payloads_with_template_key"] += 1

        for item in refusal_summaries:
            if isinstance(item, dict):
                refusal_summary_field_counts.update(item.keys())

    all_nested_fixture_ids = (
        fixture_ids_in_fixture_audits
        | fixture_ids_in_publication_records
        | fixture_ids_in_payloads
    )

    publication_join_fixture_count = len(
        fixture_ids_in_fixture_audits & fixture_ids_in_publication_records
    )

    payload_join_fixture_count = len(
        fixture_ids_in_fixture_audits & fixture_ids_in_payloads
    )

    fqis_gap_assessment = _build_gap_assessment(
        totals=totals,
        fixture_audit_field_counts=fixture_audit_field_counts,
        publication_record_field_counts=publication_record_field_counts,
        payload_field_counts=payload_field_counts,
        availability=availability,
    )

    return {
        "status": "ok",
        "source_path": str(source_path),
        "cycles_read": cycles_read,
        "valid_json_cycles": valid_json_cycles,
        "invalid_json_cycles": invalid_json_cycles,
        "non_object_cycles": non_object_cycles,
        "top_level_key_counts": dict(top_level_key_counts),
        "collection_counts": totals,
        "fixture_audit_field_counts": dict(fixture_audit_field_counts),
        "publication_record_field_counts": dict(publication_record_field_counts),
        "payload_field_counts": dict(payload_field_counts),
        "refusal_summary_field_counts": dict(refusal_summary_field_counts),
        "fixture_audit_publish_status_counts": dict(fixture_audit_publish_status_counts),
        "fixture_audit_template_counts": dict(fixture_audit_template_counts),
        "publication_template_counts": dict(publication_template_counts),
        "publication_status_counts": dict(publication_status_counts),
        "publication_disposition_counts": dict(publication_disposition_counts),
        "availability_counts": dict(availability),
        "fixture_id_coverage": {
            "fixture_audit_unique_fixture_ids": len(fixture_ids_in_fixture_audits),
            "publication_record_unique_fixture_ids": len(fixture_ids_in_publication_records),
            "payload_unique_fixture_ids": len(fixture_ids_in_payloads),
            "all_nested_unique_fixture_ids": len(all_nested_fixture_ids),
            "publication_join_fixture_count": publication_join_fixture_count,
            "payload_join_fixture_count": payload_join_fixture_count,
        },
        "fqis_gap_assessment": fqis_gap_assessment,
    }


def _build_gap_assessment(
    *,
    totals: dict[str, int],
    fixture_audit_field_counts: Counter[str],
    publication_record_field_counts: Counter[str],
    payload_field_counts: Counter[str],
    availability: Counter[str],
) -> dict[str, Any]:
    has_match_level_ids = (
        availability.get("fixture_audits_with_fixture_id", 0) > 0
        or availability.get("publication_records_with_fixture_id", 0) > 0
        or availability.get("payloads_with_fixture_id", 0) > 0
    )

    has_publication_prices = (
        availability.get("publication_records_with_odds_decimal", 0) > 0
    )

    has_template_keys = (
        availability.get("fixture_audits_with_template_key", 0) > 0
        or availability.get("publication_records_with_template_key", 0) > 0
        or availability.get("payloads_with_template_key", 0) > 0
    )

    live_feature_fields = {
        "home_xg_live",
        "away_xg_live",
        "home_shots_on_target",
        "away_shots_on_target",
        "minute",
        "home_score",
        "away_score",
    }

    observed_feature_fields = (
        set(fixture_audit_field_counts.keys())
        | set(publication_record_field_counts.keys())
        | set(payload_field_counts.keys())
    )

    has_required_live_features = live_feature_fields.issubset(observed_feature_fields)

    has_p_real_by_thesis = (
        "p_real_by_thesis" in fixture_audit_field_counts
        or "p_real_by_thesis" in publication_record_field_counts
        or "p_real_by_thesis" in payload_field_counts
        or "probabilities_by_thesis" in fixture_audit_field_counts
        or "probabilities_by_thesis" in publication_record_field_counts
        or "probabilities_by_thesis" in payload_field_counts
    )

    can_reconstruct_price_candidates = (
        has_match_level_ids
        and has_publication_prices
        and has_template_keys
        and totals["publication_records"] > 0
    )

    can_build_full_fqis_input = (
        can_reconstruct_price_candidates
        and has_required_live_features
        and has_p_real_by_thesis
    )

    if can_build_full_fqis_input:
        recommended_next_step = "build_cycle_level_converter"
    elif can_reconstruct_price_candidates:
        recommended_next_step = "create_match_level_export_with_features_and_probabilities"
    else:
        recommended_next_step = "create_dedicated_fqis_snapshot_export"

    return {
        "has_match_level_ids": has_match_level_ids,
        "has_publication_prices": has_publication_prices,
        "has_template_keys": has_template_keys,
        "has_required_live_features": has_required_live_features,
        "has_p_real_by_thesis": has_p_real_by_thesis,
        "can_reconstruct_price_candidates": can_reconstruct_price_candidates,
        "can_build_full_fqis_input": can_build_full_fqis_input,
        "recommended_next_step": recommended_next_step,
    }


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None

        