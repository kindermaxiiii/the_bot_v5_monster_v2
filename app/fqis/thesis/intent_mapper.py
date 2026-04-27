from __future__ import annotations

from app.fqis.contracts.core import MarketIntent, StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey


def map_thesis_to_market_intents(thesis: StatisticalThesis) -> tuple[MarketIntent, ...]:
    intents: list[MarketIntent] = []

    if thesis.thesis_key == ThesisKey.LOW_AWAY_SCORING_HAZARD:
        intents.extend(
            [
                MarketIntent(
                    event_id=thesis.event_id,
                    thesis_key=thesis.thesis_key,
                    family=MarketFamily.TEAM_TOTAL_AWAY,
                    side=MarketSide.UNDER,
                    period=Period.FT,
                    team_role=TeamRole.AWAY,
                    line=1.5,
                    rationale=("best direct vehicle for low away scoring",),
                ),
                MarketIntent(
                    event_id=thesis.event_id,
                    thesis_key=thesis.thesis_key,
                    family=MarketFamily.BTTS,
                    side=MarketSide.NO,
                    period=Period.FT,
                    team_role=TeamRole.NONE,
                    line=None,
                    rationale=("secondary vehicle for low away scoring",),
                ),
            ]
        )

    if thesis.thesis_key == ThesisKey.LOW_HOME_SCORING_HAZARD:
        intents.extend(
            [
                MarketIntent(
                    event_id=thesis.event_id,
                    thesis_key=thesis.thesis_key,
                    family=MarketFamily.TEAM_TOTAL_HOME,
                    side=MarketSide.UNDER,
                    period=Period.FT,
                    team_role=TeamRole.HOME,
                    line=1.5,
                    rationale=("best direct vehicle for low home scoring",),
                ),
                MarketIntent(
                    event_id=thesis.event_id,
                    thesis_key=thesis.thesis_key,
                    family=MarketFamily.BTTS,
                    side=MarketSide.NO,
                    period=Period.FT,
                    team_role=TeamRole.NONE,
                    line=None,
                    rationale=("secondary vehicle for low home scoring",),
                ),
            ]
        )

    if thesis.thesis_key == ThesisKey.OPEN_GAME:
        intents.extend(
            [
                MarketIntent(
                    event_id=thesis.event_id,
                    thesis_key=thesis.thesis_key,
                    family=MarketFamily.MATCH_TOTAL,
                    side=MarketSide.OVER,
                    period=Period.FT,
                    team_role=TeamRole.NONE,
                    line=2.5,
                    rationale=("open game total vehicle",),
                ),
                MarketIntent(
                    event_id=thesis.event_id,
                    thesis_key=thesis.thesis_key,
                    family=MarketFamily.BTTS,
                    side=MarketSide.YES,
                    period=Period.FT,
                    team_role=TeamRole.NONE,
                    line=None,
                    rationale=("open game dual scoring vehicle",),
                ),
            ]
        )

    if thesis.thesis_key == ThesisKey.CAGEY_GAME:
        intents.append(
            MarketIntent(
                event_id=thesis.event_id,
                thesis_key=thesis.thesis_key,
                family=MarketFamily.MATCH_TOTAL,
                side=MarketSide.UNDER,
                period=Period.FT,
                team_role=TeamRole.NONE,
                line=2.5,
                rationale=("cagey game total vehicle",),
            )
        )

    return tuple(intents)