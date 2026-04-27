from __future__ import annotations

import json
from dataclasses import dataclass
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


@dataclass(slots=True, frozen=True)
class VnextLikeConversionResult:
    source_path: Path
    output_path: Path
    row_count: int
    total_offer_count: int


def convert_vnext_like_export_to_fqis_input(
    source_path: Path,
    output_path: Path,
) -> VnextLikeConversionResult:
    if not source_path.exists():
        raise FileNotFoundError(f"vnext-like source file not found: {source_path}")

    converted_rows: list[dict[str, Any]] = []

    for line_number, raw_line in enumerate(source_path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            source_row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on source line {line_number}: {exc}") from exc

        converted_rows.append(_convert_source_row(source_row, line_number=line_number))

    if not converted_rows:
        raise ValueError(f"vnext-like source file is empty: {source_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as handle:
        for row in converted_rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    return VnextLikeConversionResult(
        source_path=source_path,
        output_path=output_path,
        row_count=len(converted_rows),
        total_offer_count=sum(len(row["live_offer_rows"]) for row in converted_rows),
    )


def _convert_source_row(source_row: Any, *, line_number: int) -> dict[str, Any]:
    if not isinstance(source_row, dict):
        raise ValueError(f"source line {line_number}: row must be a JSON object")

    event_id = _resolve_event_id(source_row, line_number=line_number)
    features = _resolve_features(source_row, line_number=line_number)
    offers = _resolve_offers(source_row, line_number=line_number)
    p_real_by_thesis = _resolve_p_real_by_thesis(source_row, line_number=line_number)

    live_match_row = {
        "event_id": event_id,
        "home_xg_live": features["home_xg_live"],
        "away_xg_live": features["away_xg_live"],
        "home_shots_on_target": features["home_shots_on_target"],
        "away_shots_on_target": features["away_shots_on_target"],
        "minute": features["minute"],
        "home_score": features["home_score"],
        "away_score": features["away_score"],
    }

    live_offer_rows = [
        _convert_offer_row(offer, event_id=event_id, line_number=line_number, offer_index=index)
        for index, offer in enumerate(offers, start=1)
    ]

    return {
        "live_match_row": live_match_row,
        "live_offer_rows": live_offer_rows,
        "p_real_by_thesis": p_real_by_thesis,
    }


def _resolve_event_id(source_row: dict[str, Any], *, line_number: int) -> int:
    if "event_id" in source_row:
        return int(source_row["event_id"])

    if "fixture_id" in source_row:
        return int(source_row["fixture_id"])

    features = source_row.get("features")
    if isinstance(features, dict) and "event_id" in features:
        return int(features["event_id"])

    raise ValueError(f"source line {line_number}: missing event_id or fixture_id")


def _resolve_features(source_row: dict[str, Any], *, line_number: int) -> dict[str, Any]:
    raw_features = source_row.get("features", source_row)

    if not isinstance(raw_features, dict):
        raise ValueError(f"source line {line_number}: features must be a JSON object")

    for field in _REQUIRED_FEATURE_FIELDS:
        if field not in raw_features:
            raise ValueError(f"source line {line_number}: missing feature field: {field}")

    return {
        "home_xg_live": float(raw_features["home_xg_live"]),
        "away_xg_live": float(raw_features["away_xg_live"]),
        "home_shots_on_target": int(raw_features["home_shots_on_target"]),
        "away_shots_on_target": int(raw_features["away_shots_on_target"]),
        "minute": int(raw_features["minute"]),
        "home_score": int(raw_features["home_score"]),
        "away_score": int(raw_features["away_score"]),
    }


def _resolve_offers(source_row: dict[str, Any], *, line_number: int) -> list[dict[str, Any]]:
    raw_offers = source_row.get("offers", source_row.get("live_offer_rows"))

    if raw_offers is None:
        raise ValueError(f"source line {line_number}: missing offers or live_offer_rows")

    if not isinstance(raw_offers, list):
        raise ValueError(f"source line {line_number}: offers must be a JSON array")

    return [dict(offer) for offer in raw_offers]


def _resolve_p_real_by_thesis(source_row: dict[str, Any], *, line_number: int) -> dict[str, dict[str, float]]:
    raw = source_row.get("p_real_by_thesis")

    if raw is None:
        raise ValueError(f"source line {line_number}: missing p_real_by_thesis")

    if not isinstance(raw, dict):
        raise ValueError(f"source line {line_number}: p_real_by_thesis must be a JSON object")

    parsed: dict[str, dict[str, float]] = {}

    for thesis_key, intent_probs in raw.items():
        if not isinstance(intent_probs, dict):
            raise ValueError(f"source line {line_number}: probabilities for {thesis_key} must be a JSON object")

        parsed[str(thesis_key)] = {
            str(intent_key): float(probability)
            for intent_key, probability in intent_probs.items()
        }

    return parsed


def _convert_offer_row(
    offer: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
    offer_index: int,
) -> dict[str, Any]:
    required_fields = (
        "bookmaker_name",
        "family",
        "side",
        "period",
        "team_role",
        "line",
        "odds_decimal",
    )

    for field in required_fields:
        if field not in offer:
            raise ValueError(
                f"source line {line_number}: offer #{offer_index} missing required field: {field}"
            )

    return {
        "event_id": int(offer.get("event_id", event_id)),
        "bookmaker_id": offer.get("bookmaker_id"),
        "bookmaker_name": str(offer["bookmaker_name"]),
        "family": str(offer["family"]),
        "side": str(offer["side"]),
        "period": str(offer["period"]),
        "team_role": str(offer["team_role"]),
        "line": offer.get("line"),
        "odds_decimal": float(offer["odds_decimal"]),
        "source_timestamp_utc": offer.get("source_timestamp_utc"),
        "freshness_seconds": offer.get("freshness_seconds"),
    }

    