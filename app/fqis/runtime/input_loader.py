from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.runtime.shadow import FqisShadowInput


_REQUIRED_MATCH_FIELDS = (
    "event_id",
    "home_xg_live",
    "away_xg_live",
    "home_shots_on_target",
    "away_shots_on_target",
    "minute",
    "home_score",
    "away_score",
)

_REQUIRED_OFFER_FIELDS = (
    "event_id",
    "bookmaker_name",
    "family",
    "side",
    "period",
    "team_role",
    "line",
    "odds_decimal",
)


def load_shadow_inputs_from_jsonl(path: Path) -> tuple[FqisShadowInput, ...]:
    if not path.exists():
        raise FileNotFoundError(f"shadow input file not found: {path}")

    inputs: list[FqisShadowInput] = []

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        inputs.append(_parse_shadow_input(payload, line_number=line_number))

    if not inputs:
        raise ValueError(f"shadow input file is empty: {path}")

    return tuple(inputs)


def _parse_shadow_input(payload: Any, *, line_number: int) -> FqisShadowInput:
    if not isinstance(payload, Mapping):
        raise ValueError(f"line {line_number}: payload must be a JSON object")

    live_match_row = _require_mapping(payload, "live_match_row", line_number=line_number)
    live_offer_rows_raw = _require_list(payload, "live_offer_rows", line_number=line_number)
    p_real_by_thesis_raw = _require_mapping(payload, "p_real_by_thesis", line_number=line_number)

    _validate_live_match_row(live_match_row, line_number=line_number)

    live_offer_rows = []
    for offer_index, offer in enumerate(live_offer_rows_raw, start=1):
        if not isinstance(offer, Mapping):
            raise ValueError(f"line {line_number}: offer #{offer_index} must be a JSON object")
        offer_dict = dict(offer)
        _validate_offer_row(offer_dict, line_number=line_number, offer_index=offer_index)
        live_offer_rows.append(offer_dict)

    return FqisShadowInput(
        live_match_row=dict(live_match_row),
        live_offer_rows=tuple(live_offer_rows),
        p_real_by_thesis=_parse_p_real_by_thesis(dict(p_real_by_thesis_raw), line_number=line_number),
    )


def _validate_live_match_row(row: Mapping[str, Any], *, line_number: int) -> None:
    _require_fields(row, _REQUIRED_MATCH_FIELDS, line_number=line_number, context="live_match_row")

    _as_int(row["event_id"], line_number=line_number, field="live_match_row.event_id")
    _as_float(row["home_xg_live"], line_number=line_number, field="live_match_row.home_xg_live")
    _as_float(row["away_xg_live"], line_number=line_number, field="live_match_row.away_xg_live")
    _as_int(row["home_shots_on_target"], line_number=line_number, field="live_match_row.home_shots_on_target")
    _as_int(row["away_shots_on_target"], line_number=line_number, field="live_match_row.away_shots_on_target")
    _as_int(row["minute"], line_number=line_number, field="live_match_row.minute")
    _as_int(row["home_score"], line_number=line_number, field="live_match_row.home_score")
    _as_int(row["away_score"], line_number=line_number, field="live_match_row.away_score")


def _validate_offer_row(row: Mapping[str, Any], *, line_number: int, offer_index: int) -> None:
    context = f"live_offer_rows[{offer_index}]"
    _require_fields(row, _REQUIRED_OFFER_FIELDS, line_number=line_number, context=context)

    _as_int(row["event_id"], line_number=line_number, field=f"{context}.event_id")
    _parse_family(row["family"], line_number=line_number, field=f"{context}.family")
    _parse_side(row["side"], line_number=line_number, field=f"{context}.side")
    _parse_period(row["period"], line_number=line_number, field=f"{context}.period")
    _parse_team_role(row["team_role"], line_number=line_number, field=f"{context}.team_role")
    _as_optional_float(row.get("line"), line_number=line_number, field=f"{context}.line")
    odds = _as_float(row["odds_decimal"], line_number=line_number, field=f"{context}.odds_decimal")

    if odds <= 1.0:
        raise ValueError(f"line {line_number}: {context}.odds_decimal must be > 1.0")


def _parse_p_real_by_thesis(
    raw: dict[str, Any],
    *,
    line_number: int,
) -> dict[ThesisKey, dict[str, float]]:
    parsed: dict[ThesisKey, dict[str, float]] = {}

    for thesis_key_raw, intent_probs_raw in raw.items():
        try:
            thesis_key = ThesisKey(str(thesis_key_raw))
        except ValueError as exc:
            raise ValueError(
                f"line {line_number}: unknown thesis key: {thesis_key_raw}"
            ) from exc

        if not isinstance(intent_probs_raw, Mapping):
            raise ValueError(f"line {line_number}: probabilities for {thesis_key.value} must be an object")

        parsed[thesis_key] = {}

        for intent_key, probability_raw in intent_probs_raw.items():
            probability = _as_float(
                probability_raw,
                line_number=line_number,
                field=f"p_real_by_thesis.{thesis_key.value}.{intent_key}",
            )

            if probability < 0.0 or probability > 1.0:
                raise ValueError(
                    f"line {line_number}: probability must be between 0 and 1 for {intent_key}"
                )

            parsed[thesis_key][str(intent_key)] = probability

    return parsed


def _require_mapping(payload: Mapping[str, Any], field: str, *, line_number: int) -> Mapping[str, Any]:
    if field not in payload:
        raise ValueError(f"line {line_number}: missing required field: {field}")

    value = payload[field]
    if not isinstance(value, Mapping):
        raise ValueError(f"line {line_number}: {field} must be a JSON object")

    return value


def _require_list(payload: Mapping[str, Any], field: str, *, line_number: int) -> list[Any]:
    if field not in payload:
        raise ValueError(f"line {line_number}: missing required field: {field}")

    value = payload[field]
    if not isinstance(value, list):
        raise ValueError(f"line {line_number}: {field} must be a JSON array")

    return value


def _require_fields(row: Mapping[str, Any], fields: tuple[str, ...], *, line_number: int, context: str) -> None:
    for field in fields:
        if field not in row:
            raise ValueError(f"line {line_number}: missing required field: {context}.{field}")


def _parse_family(value: Any, *, line_number: int, field: str) -> MarketFamily:
    try:
        return MarketFamily(str(value))
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid {field}: {value}") from exc


def _parse_side(value: Any, *, line_number: int, field: str) -> MarketSide:
    try:
        return MarketSide(str(value))
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid {field}: {value}") from exc


def _parse_period(value: Any, *, line_number: int, field: str) -> Period:
    try:
        return Period(str(value))
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid {field}: {value}") from exc


def _parse_team_role(value: Any, *, line_number: int, field: str) -> TeamRole:
    try:
        return TeamRole(str(value))
    except ValueError as exc:
        raise ValueError(f"line {line_number}: invalid {field}: {value}") from exc


def _as_int(value: Any, *, line_number: int, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"line {line_number}: invalid integer for {field}: {value}") from exc


def _as_float(value: Any, *, line_number: int, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"line {line_number}: invalid float for {field}: {value}") from exc


def _as_optional_float(value: Any, *, line_number: int, field: str) -> float | None:
    if value is None or value == "":
        return None
    return _as_float(value, line_number=line_number, field=field)

    