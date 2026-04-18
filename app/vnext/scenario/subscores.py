from __future__ import annotations

from statistics import mean

from app.vnext.prior.models import HistoricalPriorPack
from app.vnext.scenario.models import HistoricalSubScores, PriorReliabilityBreakdown


_QUALITY_SCORE = {
    "HIGH": 1.0,
    "MEDIUM": 0.75,
    "LOW": 0.5,
    "INCONSISTENT": 0.2,
}


def _clip(value: float, *, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _round(value: float) -> float:
    return round(value, 4)


def _ratio_edge(home_value: float, away_value: float, *, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return _clip((home_value - away_value) / scale)


def _affinity(value: float) -> float:
    return _clip(value, low=0.0, high=1.0)


def build_prior_reliability(pack: HistoricalPriorPack) -> PriorReliabilityBreakdown:
    sample_size_score = mean(
        [
            min(pack.attack_context.sample_size / 8.0, 1.0),
            min(pack.defense_context.sample_size / 8.0, 1.0),
            min(pack.venue_context.sample_size / 8.0, 1.0),
            min(pack.form_context.sample_size / 8.0, 1.0),
            min(pack.strength_context.sample_size / 12.0, 1.0),
            min(pack.style_context.sample_size / 12.0, 1.0),
            min(pack.matchup_context.sample_size / 5.0, 1.0),
            min(pack.competition_context.sample_size / 20.0, 1.0),
        ]
    )
    data_quality_score = mean(
        [
            _QUALITY_SCORE[pack.attack_context.data_quality_flag],
            _QUALITY_SCORE[pack.defense_context.data_quality_flag],
            _QUALITY_SCORE[pack.venue_context.data_quality_flag],
            _QUALITY_SCORE[pack.form_context.data_quality_flag],
            _QUALITY_SCORE[pack.strength_context.data_quality_flag],
            _QUALITY_SCORE[pack.style_context.data_quality_flag],
            _QUALITY_SCORE[pack.matchup_context.data_quality_flag],
            _QUALITY_SCORE[pack.competition_context.data_quality_flag],
        ]
    )
    pack_confidence_score = mean(
        [
            pack.attack_context.confidence_weight,
            pack.defense_context.confidence_weight,
            pack.venue_context.confidence_weight,
            pack.form_context.confidence_weight,
            pack.strength_context.confidence_weight,
            pack.style_context.confidence_weight,
            pack.matchup_context.confidence_weight,
            pack.competition_context.confidence_weight,
        ]
    )
    competition_confidence_score = pack.competition_context.competition.competition_confidence_score
    prior_reliability_score = _clip(
        (
            (sample_size_score * 0.30)
            + (data_quality_score * 0.30)
            + (competition_confidence_score * 0.20)
            + (pack_confidence_score * 0.20)
        ),
        low=0.0,
        high=1.0,
    )
    return PriorReliabilityBreakdown(
        sample_size_score=_round(sample_size_score),
        data_quality_score=_round(data_quality_score),
        competition_confidence_score=_round(competition_confidence_score),
        pack_confidence_score=_round(pack_confidence_score),
        prior_reliability_score=_round(prior_reliability_score),
    )


def build_historical_subscores(pack: HistoricalPriorPack) -> HistoricalSubScores:
    home_attack_pressure = mean(
        [
            pack.attack_context.home.xg_for_per_match,
            pack.attack_context.home.goals_for_per_match,
            pack.attack_context.home.shots_on_for_per_match / 4.0,
            pack.venue_context.home.xg_for_per_match,
        ]
    )
    away_attack_pressure = mean(
        [
            pack.attack_context.away.xg_for_per_match,
            pack.attack_context.away.goals_for_per_match,
            pack.attack_context.away.shots_on_for_per_match / 4.0,
            pack.venue_context.away.xg_for_per_match,
        ]
    )
    home_concession_risk = mean(
        [
            pack.defense_context.home.xg_against_per_match,
            pack.defense_context.home.goals_against_per_match,
            pack.defense_context.home.shots_on_against_per_match / 4.0,
            1.0 - pack.defense_context.home.clean_sheet_rate,
        ]
    )
    away_concession_risk = mean(
        [
            pack.defense_context.away.xg_against_per_match,
            pack.defense_context.away.goals_against_per_match,
            pack.defense_context.away.shots_on_against_per_match / 4.0,
            1.0 - pack.defense_context.away.clean_sheet_rate,
        ]
    )

    home_attack_edge = _clip(
        ((home_attack_pressure + away_concession_risk) - (away_attack_pressure + home_concession_risk)) / 2.0
    )
    away_attack_edge = _clip(-home_attack_edge + _ratio_edge(away_attack_pressure, home_attack_pressure, scale=2.0) * 0.35)
    home_defense_edge = _clip(
        (
            (pack.defense_context.home.clean_sheet_rate - pack.style_context.away.failed_to_score_rate)
            + ((away_concession_risk - home_concession_risk) * 0.7)
            + ((pack.attack_context.away.goals_for_per_match - pack.defense_context.home.goals_against_per_match) * -0.35)
        )
    )
    away_defense_edge = _clip(
        (
            (pack.defense_context.away.clean_sheet_rate - pack.style_context.home.failed_to_score_rate)
            + ((home_concession_risk - away_concession_risk) * 0.7)
            + ((pack.attack_context.home.goals_for_per_match - pack.defense_context.away.goals_against_per_match) * -0.35)
        )
    )
    form_edge = _ratio_edge(pack.form_context.home.form_score, pack.form_context.away.form_score, scale=0.65)
    venue_edge = _clip(
        (
            (pack.venue_context.home.goals_for_per_match - pack.venue_context.away.goals_for_per_match) * 0.35
            + (pack.venue_context.away.goals_against_per_match - pack.venue_context.home.goals_against_per_match) * 0.35
            + (pack.venue_context.home.xg_for_per_match - pack.venue_context.away.xg_for_per_match) * 0.30
        )
    )
    strength_edge = _ratio_edge(
        pack.strength_context.home.global_rating,
        pack.strength_context.away.global_rating,
        scale=30.0,
    )
    balance_score = _affinity(
        1.0
        - (
            mean(
                [
                    abs(home_attack_edge - away_attack_edge),
                    abs(home_defense_edge - away_defense_edge),
                    abs(form_edge),
                    abs(strength_edge),
                    abs(venue_edge),
                ]
            )
            / 0.45
        )
    )
    btts_affinity = _affinity(
        mean(
            [
                pack.style_context.home.btts_rate,
                pack.style_context.away.btts_rate,
                pack.matchup_context.matchup.btts_rate,
                pack.competition_context.competition.btts_rate,
            ]
        )
    )
    over_affinity = _affinity(
        mean(
            [
                pack.style_context.home.over_2_5_rate,
                pack.style_context.away.over_2_5_rate,
                pack.matchup_context.matchup.over_2_5_rate,
                pack.competition_context.competition.over_2_5_rate,
                _affinity(pack.competition_context.competition.avg_goals_per_match / 3.2),
            ]
        )
    )
    under_affinity = _affinity(
        mean(
            [
                pack.style_context.home.under_2_5_rate,
                pack.style_context.away.under_2_5_rate,
                _affinity(1.0 - (pack.competition_context.competition.avg_goals_per_match / 3.4)),
            ]
        )
    )
    clean_sheet_home_affinity = _affinity(
        mean(
            [
                pack.style_context.home.clean_sheet_rate,
                pack.style_context.away.failed_to_score_rate,
                _affinity((home_defense_edge + 1.0) / 2.0),
            ]
        )
    )
    clean_sheet_away_affinity = _affinity(
        mean(
            [
                pack.style_context.away.clean_sheet_rate,
                pack.style_context.home.failed_to_score_rate,
                _affinity((away_defense_edge + 1.0) / 2.0),
            ]
        )
    )
    competition_goal_bias = _clip(
        (
            ((pack.competition_context.competition.avg_goals_per_match - 2.5) / 1.2) * 0.55
            + ((pack.competition_context.competition.over_2_5_rate - 0.5) * 0.9) * 0.45
        )
    )
    matchup_nudge = _clip(
        (
            (pack.matchup_context.matchup.home_team_goals_per_match - pack.matchup_context.matchup.away_team_goals_per_match) * 0.45
            + ((pack.matchup_context.matchup.draw_rate - 0.33) * -0.25)
        ),
        low=-0.35,
        high=0.35,
    )
    return HistoricalSubScores(
        source="historical_subscores.v1",
        home_attack_edge=_round(home_attack_edge),
        away_attack_edge=_round(away_attack_edge),
        home_defense_edge=_round(home_defense_edge),
        away_defense_edge=_round(away_defense_edge),
        form_edge=_round(form_edge),
        venue_edge=_round(venue_edge),
        strength_edge=_round(strength_edge),
        balance_score=_round(balance_score),
        btts_affinity=_round(btts_affinity),
        under_2_5_affinity=_round(under_affinity),
        over_2_5_affinity=_round(over_affinity),
        clean_sheet_home_affinity=_round(clean_sheet_home_affinity),
        clean_sheet_away_affinity=_round(clean_sheet_away_affinity),
        competition_goal_bias=_round(competition_goal_bias),
        matchup_nudge=_round(matchup_nudge),
    )
