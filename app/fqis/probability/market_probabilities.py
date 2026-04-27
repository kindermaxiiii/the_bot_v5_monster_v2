from __future__ import annotations

from app.fqis.contracts.core import MarketIntent
from app.fqis.contracts.enums import MarketFamily, MarketSide, TeamRole
from app.fqis.probability.models import MarketProbability, ScoreDistribution


def probability_no_more_goals(distribution: ScoreDistribution) -> MarketProbability:
    probability = sum(
        cell.probability
        for cell in distribution.cells
        if cell.remaining_home_goals == 0 and cell.remaining_away_goals == 0
    )

    return MarketProbability(
        market_key="NO_MORE_GOALS",
        probability=_clamp_probability(probability),
    )


def probability_btts_yes(distribution: ScoreDistribution) -> MarketProbability:
    if distribution.state.home_score > 0 and distribution.state.away_score > 0:
        return MarketProbability(
            market_key="BTTS|YES|NONE|NA",
            probability=1.0,
        )

    probability = sum(
        cell.probability
        for cell in distribution.cells
        if cell.final_home_goals > 0 and cell.final_away_goals > 0
    )

    return MarketProbability(
        market_key="BTTS|YES|NONE|NA",
        probability=_clamp_probability(probability),
    )


def probability_btts_no(distribution: ScoreDistribution) -> MarketProbability:
    if distribution.state.home_score > 0 and distribution.state.away_score > 0:
        return MarketProbability(
            market_key="BTTS|NO|NONE|NA",
            probability=0.0,
        )

    yes = probability_btts_yes(distribution).probability

    return MarketProbability(
        market_key="BTTS|NO|NONE|NA",
        probability=_clamp_probability(1.0 - yes),
    )


def probability_match_total_over(
    distribution: ScoreDistribution,
    *,
    line: float,
) -> MarketProbability:
    probability = sum(
        cell.probability
        for cell in distribution.cells
        if (cell.final_home_goals + cell.final_away_goals) > line
    )

    return MarketProbability(
        market_key=f"MATCH_TOTAL|OVER|NONE|{line}",
        probability=_clamp_probability(probability),
    )


def probability_match_total_under(
    distribution: ScoreDistribution,
    *,
    line: float,
) -> MarketProbability:
    probability = sum(
        cell.probability
        for cell in distribution.cells
        if (cell.final_home_goals + cell.final_away_goals) < line
    )

    return MarketProbability(
        market_key=f"MATCH_TOTAL|UNDER|NONE|{line}",
        probability=_clamp_probability(probability),
    )


def probability_team_total_over(
    distribution: ScoreDistribution,
    *,
    team_role: TeamRole,
    line: float,
) -> MarketProbability:
    probability = sum(
        cell.probability
        for cell in distribution.cells
        if _team_final_goals(cell, team_role) > line
    )

    return MarketProbability(
        market_key=f"{_team_total_family(team_role)}|OVER|{team_role.value}|{line}",
        probability=_clamp_probability(probability),
    )


def probability_team_total_under(
    distribution: ScoreDistribution,
    *,
    team_role: TeamRole,
    line: float,
) -> MarketProbability:
    probability = sum(
        cell.probability
        for cell in distribution.cells
        if _team_final_goals(cell, team_role) < line
    )

    return MarketProbability(
        market_key=f"{_team_total_family(team_role)}|UNDER|{team_role.value}|{line}",
        probability=_clamp_probability(probability),
    )


def probability_1x2(distribution: ScoreDistribution) -> dict[str, MarketProbability]:
    home = sum(
        cell.probability
        for cell in distribution.cells
        if cell.final_home_goals > cell.final_away_goals
    )
    draw = sum(
        cell.probability
        for cell in distribution.cells
        if cell.final_home_goals == cell.final_away_goals
    )
    away = sum(
        cell.probability
        for cell in distribution.cells
        if cell.final_home_goals < cell.final_away_goals
    )

    return {
        "HOME": MarketProbability(
            "MATCH_RESULT|HOME|NONE|NA",
            _clamp_probability(home),
        ),
        "DRAW": MarketProbability(
            "MATCH_RESULT|DRAW|NONE|NA",
            _clamp_probability(draw),
        ),
        "AWAY": MarketProbability(
            "MATCH_RESULT|AWAY|NONE|NA",
            _clamp_probability(away),
        ),
    }


def probability_for_intent(
    distribution: ScoreDistribution,
    intent: MarketIntent,
) -> MarketProbability:
    if intent.family == MarketFamily.BTTS and intent.side == MarketSide.YES:
        return probability_btts_yes(distribution)

    if intent.family == MarketFamily.BTTS and intent.side == MarketSide.NO:
        return probability_btts_no(distribution)

    if intent.family == MarketFamily.MATCH_TOTAL and intent.side == MarketSide.OVER:
        if intent.line is None:
            raise ValueError("MATCH_TOTAL OVER requires a line")

        return probability_match_total_over(
            distribution,
            line=float(intent.line),
        )

    if intent.family == MarketFamily.MATCH_TOTAL and intent.side == MarketSide.UNDER:
        if intent.line is None:
            raise ValueError("MATCH_TOTAL UNDER requires a line")

        return probability_match_total_under(
            distribution,
            line=float(intent.line),
        )

    if intent.family in {MarketFamily.TEAM_TOTAL_HOME, MarketFamily.TEAM_TOTAL_AWAY}:
        if intent.line is None:
            raise ValueError("TEAM_TOTAL requires a line")

        if intent.side == MarketSide.OVER:
            return probability_team_total_over(
                distribution,
                team_role=intent.team_role,
                line=float(intent.line),
            )

        if intent.side == MarketSide.UNDER:
            return probability_team_total_under(
                distribution,
                team_role=intent.team_role,
                line=float(intent.line),
            )

    raise ValueError(
        "unsupported market intent for probability core: "
        f"{intent.family.value}|{intent.side.value}|{intent.team_role.value}|{intent.line}"
    )


def _team_final_goals(cell, team_role: TeamRole) -> int:
    if team_role == TeamRole.HOME:
        return int(cell.final_home_goals)

    if team_role == TeamRole.AWAY:
        return int(cell.final_away_goals)

    raise ValueError("team total probability requires HOME or AWAY team_role")


def _team_total_family(team_role: TeamRole) -> str:
    if team_role == TeamRole.HOME:
        return "TEAM_TOTAL_HOME"

    if team_role == TeamRole.AWAY:
        return "TEAM_TOTAL_AWAY"

    raise ValueError("team total family requires HOME or AWAY team_role")


def _clamp_probability(value: float) -> float:
    value = float(value)

    if abs(value) <= 1e-15:
        return 0.0

    if abs(value - 1.0) <= 1e-15:
        return 1.0

    return max(0.0, min(1.0, value))

    