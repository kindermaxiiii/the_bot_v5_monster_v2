from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from app.fqis.contracts.core import BookOffer
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole
from app.fqis.thesis.features import SimpleMatchFeatures


def adapt_live_match_to_features(row: Mapping[str, Any]) -> SimpleMatchFeatures:
    return SimpleMatchFeatures(
        event_id=_as_int(row["event_id"]),
        home_xg_live=_as_float(row.get("home_xg_live", 0.0)),
        away_xg_live=_as_float(row.get("away_xg_live", 0.0)),
        home_shots_on_target=_as_int(row.get("home_shots_on_target", 0)),
        away_shots_on_target=_as_int(row.get("away_shots_on_target", 0)),
        minute=_as_int(row.get("minute", 0)),
        home_score=_as_int(row.get("home_score", 0)),
        away_score=_as_int(row.get("away_score", 0)),
    )


def adapt_live_offers_to_book_offers(rows: Iterable[Mapping[str, Any]]) -> tuple[BookOffer, ...]:
    offers: list[BookOffer] = []

    for row in rows:
        offers.append(
            BookOffer(
                event_id=_as_int(row["event_id"]),
                bookmaker_id=_as_optional_int(row.get("bookmaker_id")),
                bookmaker_name=str(row.get("bookmaker_name", "UNKNOWN")),
                family=_parse_family(row["family"]),
                side=_parse_side(row["side"]),
                period=_parse_period(row.get("period", "FT")),
                team_role=_parse_team_role(row.get("team_role", "NONE")),
                line=_as_optional_float(row.get("line")),
                odds_decimal=_as_float(row["odds_decimal"]),
                source_timestamp_utc=_as_optional_str(row.get("source_timestamp_utc")),
                freshness_seconds=_as_optional_int(row.get("freshness_seconds")),
            )
        )

    return tuple(offers)


def _parse_family(value: Any) -> MarketFamily:
    return MarketFamily(str(value))


def _parse_side(value: Any) -> MarketSide:
    return MarketSide(str(value))


def _parse_period(value: Any) -> Period:
    return Period(str(value))


def _parse_team_role(value: Any) -> TeamRole:
    return TeamRole(str(value))


def _as_int(value: Any) -> int:
    return int(value)


def _as_float(value: Any) -> float:
    return float(value)


def _as_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _as_optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)

    