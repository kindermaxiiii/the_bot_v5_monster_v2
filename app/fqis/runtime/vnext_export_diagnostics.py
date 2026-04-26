from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


_REQUIRED_FEATURE_FIELDS = (
    "home_xg_live",
    "away_xg_live",
    "home_shots_on_target",
    "away_shots_on_target",
    "minute",
    "home_score",
    "away_score",
)


def diagnose_vnext_export_for_fqis(source_path: Path) -> dict[str, Any]:
    if not source_path.exists():
        raise FileNotFoundError(f"vnext export file not found: {source_path}")

    rows_read = 0
    rows_valid_json = 0
    rows_invalid_json = 0

    top_level_key_counts: Counter[str] = Counter()
    event_id_source_counts: Counter[str] = Counter()
    feature_source_counts: Counter[str] = Counter()
    feature_field_counts: Counter[str] = Counter()
    offer_source_counts: Counter[str] = Counter()
    offer_field_counts: Counter[str] = Counter()
    p_real_source_counts: Counter[str] = Counter()
    conversion_readiness_counts: Counter[str] = Counter()

    total_offer_row_count = 0

    for raw_line in source_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        rows_read += 1

        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            rows_invalid_json += 1
            conversion_readiness_counts["invalid_json"] += 1
            continue

        rows_valid_json += 1

        if not isinstance(row, dict):
            conversion_readiness_counts["row_not_object"] += 1
            continue

        top_level_key_counts.update(row.keys())

        event_source = _detect_event_id_source(row)
        event_id_source_counts[event_source] += 1

        feature_source, features = _detect_feature_source(row)
        feature_source_counts[feature_source] += 1

        if isinstance(features, dict):
            feature_field_counts.update(features.keys())

        offer_source, offers = _detect_offer_source(row)
        offer_source_counts[offer_source] += 1

        if isinstance(offers, list):
            total_offer_row_count += len(offers)
            for offer in offers:
                if isinstance(offer, dict):
                    offer_field_counts.update(offer.keys())

        p_real_source = _detect_p_real_source(row)
        p_real_source_counts[p_real_source] += 1

        readiness = _diagnose_conversion_readiness(row)
        conversion_readiness_counts[readiness] += 1

    return {
        "status": "ok",
        "source_path": str(source_path),
        "rows_read": rows_read,
        "rows_valid_json": rows_valid_json,
        "rows_invalid_json": rows_invalid_json,
        "top_level_key_counts": dict(top_level_key_counts),
        "event_id_source_counts": dict(event_id_source_counts),
        "feature_source_counts": dict(feature_source_counts),
        "feature_field_counts": dict(feature_field_counts),
        "offer_source_counts": dict(offer_source_counts),
        "offer_field_counts": dict(offer_field_counts),
        "total_offer_row_count": total_offer_row_count,
        "p_real_source_counts": dict(p_real_source_counts),
        "conversion_readiness_counts": dict(conversion_readiness_counts),
        "probably_convertible_rows": conversion_readiness_counts.get("convertible", 0),
    }


def _detect_event_id_source(row: dict[str, Any]) -> str:
    if row.get("event_id") not in (None, ""):
        return "event_id"
    if row.get("fixture_id") not in (None, ""):
        return "fixture_id"
    if row.get("match_id") not in (None, ""):
        return "match_id"

    features = row.get("features")
    if isinstance(features, dict) and features.get("event_id") not in (None, ""):
        return "features.event_id"

    return "missing"


def _detect_feature_source(row: dict[str, Any]) -> tuple[str, Any]:
    if isinstance(row.get("features"), dict):
        return "features", row["features"]
    if isinstance(row.get("live_features"), dict):
        return "live_features", row["live_features"]

    has_top_level_features = any(field in row for field in _REQUIRED_FEATURE_FIELDS)
    if has_top_level_features:
        return "top_level", row

    return "missing", None


def _detect_offer_source(row: dict[str, Any]) -> tuple[str, Any]:
    if isinstance(row.get("offers"), list):
        return "offers", row["offers"]
    if isinstance(row.get("live_offer_rows"), list):
        return "live_offer_rows", row["live_offer_rows"]
    if isinstance(row.get("publication_records"), list):
        return "publication_records", row["publication_records"]

    return "missing", None


def _detect_p_real_source(row: dict[str, Any]) -> str:
    if isinstance(row.get("p_real_by_thesis"), dict):
        return "p_real_by_thesis"
    if isinstance(row.get("probabilities_by_thesis"), dict):
        return "probabilities_by_thesis"

    return "missing"


def _diagnose_conversion_readiness(row: dict[str, Any]) -> str:
    if _detect_event_id_source(row) == "missing":
        return "missing_event_id"

    _, features = _detect_feature_source(row)
    if not isinstance(features, dict):
        return "missing_features"

    for field in _REQUIRED_FEATURE_FIELDS:
        if field not in features:
            return f"missing_{field}"

    _, offers = _detect_offer_source(row)
    if not isinstance(offers, list):
        return "missing_offers"

    if not offers:
        return "empty_offers"

    if not _has_convertible_offer(offers):
        return "no_convertible_offers"

    p_real_source = _detect_p_real_source(row)
    if p_real_source == "missing":
        return "missing_p_real_by_thesis"

    p_real = row.get(p_real_source)
    if not isinstance(p_real, dict) or not p_real:
        return "empty_p_real_by_thesis"

    return "convertible"


def _has_convertible_offer(offers: list[Any]) -> bool:
    for offer in offers:
        if not isinstance(offer, dict):
            continue

        has_family = offer.get("family") not in (None, "") or offer.get("market_family") not in (None, "")
        has_side = offer.get("side") not in (None, "") or offer.get("selection_side") not in (None, "")
        has_odds = (
            offer.get("odds_decimal") not in (None, "")
            or offer.get("odds") not in (None, "")
            or offer.get("price") not in (None, "")
        )

        if has_family and has_side and has_odds:
            return True

    return False