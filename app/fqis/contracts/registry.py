from __future__ import annotations

from dataclasses import dataclass

from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole


@dataclass(slots=True, frozen=True)
class MarketDefinition:
    key: str
    family: MarketFamily
    side: MarketSide
    period: Period
    team_role: TeamRole
    line_required: bool


MARKET_REGISTRY: dict[str, MarketDefinition] = {
    "MATCH_TOTAL_OVER_2_5": MarketDefinition(
        key="MATCH_TOTAL_OVER_2_5",
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line_required=True,
    ),
    "MATCH_TOTAL_UNDER_2_5": MarketDefinition(
        key="MATCH_TOTAL_UNDER_2_5",
        family=MarketFamily.MATCH_TOTAL,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line_required=True,
    ),
    "TEAM_TOTAL_HOME_OVER_0_5": MarketDefinition(
        key="TEAM_TOTAL_HOME_OVER_0_5",
        family=MarketFamily.TEAM_TOTAL_HOME,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.HOME,
        line_required=True,
    ),
    "TEAM_TOTAL_HOME_UNDER_1_5": MarketDefinition(
        key="TEAM_TOTAL_HOME_UNDER_1_5",
        family=MarketFamily.TEAM_TOTAL_HOME,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.HOME,
        line_required=True,
    ),
    "TEAM_TOTAL_AWAY_OVER_0_5": MarketDefinition(
        key="TEAM_TOTAL_AWAY_OVER_0_5",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.OVER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line_required=True,
    ),
    "TEAM_TOTAL_AWAY_UNDER_1_5": MarketDefinition(
        key="TEAM_TOTAL_AWAY_UNDER_1_5",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line_required=True,
    ),
    "BTTS_YES": MarketDefinition(
        key="BTTS_YES",
        family=MarketFamily.BTTS,
        side=MarketSide.YES,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line_required=False,
    ),
    "BTTS_NO": MarketDefinition(
        key="BTTS_NO",
        family=MarketFamily.BTTS,
        side=MarketSide.NO,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line_required=False,
    ),
    "RESULT_HOME": MarketDefinition(
        key="RESULT_HOME",
        family=MarketFamily.RESULT,
        side=MarketSide.HOME,
        period=Period.FT,
        team_role=TeamRole.HOME,
        line_required=False,
    ),
    "RESULT_DRAW": MarketDefinition(
        key="RESULT_DRAW",
        family=MarketFamily.RESULT,
        side=MarketSide.DRAW,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line_required=False,
    ),
    "RESULT_AWAY": MarketDefinition(
        key="RESULT_AWAY",
        family=MarketFamily.RESULT,
        side=MarketSide.AWAY,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line_required=False,
    ),
}


def get_market_definition(key: str) -> MarketDefinition:
    try:
        return MARKET_REGISTRY[key]
    except KeyError as exc:
        raise KeyError(f"unknown market key: {key}") from exc
        