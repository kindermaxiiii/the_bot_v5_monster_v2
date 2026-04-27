from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True, frozen=True)
class VnextExportConversionReport:
    source_path: Path
    output_path: Path
    rows_read: int
    rows_converted: int
    rows_rejected: int
    rejection_reasons: dict[str, int]


def convert_vnext_export_to_fqis_input(
    source_path: Path,
    output_path: Path,
) -> VnextExportConversionReport:
    if not source_path.exists():
        raise FileNotFoundError(f"vnext export file not found: {source_path}")

    converted_rows: list[dict[str, Any]] = []
    rejection_reasons: Counter[str] = Counter()
    rows_read = 0

    for line_number, raw_line in enumerate(source_path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        rows_read += 1

        try:
            source_row = json.loads(line)
        except json.JSONDecodeError:
            rejection_reasons["invalid_json"] += 1
            continue

        try:
            converted_rows.append(_convert_row(source_row, line_number=line_number))
        except ValueError as exc:
            rejection_reasons[str(exc)] += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as handle:
        for row in converted_rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")

    return VnextExportConversionReport(
        source_path=source_path,
        output_path=output_path,
        rows_read=rows_read,
        rows_converted=len(converted_rows),
        rows_rejected=rows_read - len(converted_rows),
        rejection_reasons=dict(rejection_reasons),
    )


def _convert_row(row: Any, *, line_number: int) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ValueError("row_not_object")

    event_id = _first_present_int(row, ("event_id", "fixture_id", "match_id"), reason="missing_event_id")

    live_match_row = _extract_live_match_row(row, event_id=event_id)
    live_offer_rows = _extract_live_offer_rows(row, event_id=event_id)
    p_real_by_thesis = _extract_p_real_by_thesis(row)

    if not live_offer_rows:
        raise ValueError("missing_offers")

    if not p_real_by_thesis:
        raise ValueError("missing_p_real_by_thesis")

    return {
        "live_match_row": live_match_row,
        "live_offer_rows": live_offer_rows,
        "p_real_by_thesis": p_real_by_thesis,
    }


def _extract_live_match_row(row: dict[str, Any], *, event_id: int) -> dict[str, Any]:
    features = row.get("features")
    if not isinstance(features, dict):
        features = row.get("live_features")
    if not isinstance(features, dict):
        features = row

    required = {
        "home_xg_live": "missing_home_xg_live",
        "away_xg_live": "missing_away_xg_live",
        "home_shots_on_target": "missing_home_shots_on_target",
        "away_shots_on_target": "missing_away_shots_on_target",
        "minute": "missing_minute",
        "home_score": "missing_home_score",
        "away_score": "missing_away_score",
    }

    for field, reason in required.items():
        if field not in features:
            raise ValueError(reason)

    return {
        "event_id": event_id,
        "home_xg_live": float(features["home_xg_live"]),
        "away_xg_live": float(features["away_xg_live"]),
        "home_shots_on_target": int(features["home_shots_on_target"]),
        "away_shots_on_target": int(features["away_shots_on_target"]),
        "minute": int(features["minute"]),
        "home_score": int(features["home_score"]),
        "away_score": int(features["away_score"]),
    }


def _extract_live_offer_rows(row: dict[str, Any], *, event_id: int) -> list[dict[str, Any]]:
    offers = row.get("offers")
    if offers is None:
        offers = row.get("live_offer_rows")
    if offers is None:
        offers = row.get("publication_records")

    if not isinstance(offers, list):
        raise ValueError("missing_offers")

    converted: list[dict[str, Any]] = []

    for offer in offers:
        if not isinstance(offer, dict):
            continue

        try:
            converted.append(_convert_offer(offer, event_id=event_id))
        except ValueError:
            continue

    return converted


def _convert_offer(offer: dict[str, Any], *, event_id: int) -> dict[str, Any]:
    family = _first_present_str(offer, ("family", "market_family"), reason="missing_offer_family")
    side = _first_present_str(offer, ("side", "selection_side"), reason="missing_offer_side")
    period = _first_present_str(offer, ("period",), default="FT")
    team_role = _first_present_str(offer, ("team_role",), default="NONE")
    bookmaker_name = _first_present_str(offer, ("bookmaker_name", "bookmaker"), default="UNKNOWN")
    odds_decimal = _first_present_float(offer, ("odds_decimal", "odds", "price"), reason="missing_offer_odds")

    return {
        "event_id": int(offer.get("event_id", event_id)),
        "bookmaker_id": offer.get("bookmaker_id"),
        "bookmaker_name": bookmaker_name,
        "family": family,
        "side": side,
        "period": period,
        "team_role": team_role,
        "line": offer.get("line"),
        "odds_decimal": odds_decimal,
        "source_timestamp_utc": offer.get("source_timestamp_utc"),
        "freshness_seconds": offer.get("freshness_seconds"),
    }


def _extract_p_real_by_thesis(row: dict[str, Any]) -> dict[str, dict[str, float]]:
    raw = row.get("p_real_by_thesis")
    if raw is None:
        raw = row.get("probabilities_by_thesis")

    if not isinstance(raw, dict):
        raise ValueError("missing_p_real_by_thesis")

    parsed: dict[str, dict[str, float]] = {}

    for thesis_key, intent_probs in raw.items():
        if not isinstance(intent_probs, dict):
            continue

        parsed[str(thesis_key)] = {
            str(intent_key): float(probability)
            for intent_key, probability in intent_probs.items()
        }

    return parsed


def _first_present_int(row: dict[str, Any], keys: tuple[str, ...], *, reason: str) -> int:
    for key in keys:
        if key in row and row[key] is not None and row[key] != "":
            return int(row[key])
    raise ValueError(reason)


def _first_present_float(
    row: dict[str, Any],
    keys: tuple[str, ...],
    *,
    reason: str,
) -> float:
    for key in keys:
        if key in row and row[key] is not None and row[key] != "":
            return float(row[key])
    raise ValueError(reason)


def _first_present_str(
    row: dict[str, Any],
    keys: tuple[str, ...],
    *,
    reason: str | None = None,
    default: str | None = None,
) -> str:
    for key in keys:
        if key in row and row[key] is not None and row[key] != "":
            return str(row[key])

    if default is not None:
        return default

    raise ValueError(reason or f"missing_{keys[0]}")

    