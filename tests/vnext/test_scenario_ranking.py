from __future__ import annotations

from app.vnext.scenario.models import HistoricalSubScores
from app.vnext.scenario.ranker import rank_scenarios


def _make_subscores(**overrides: float) -> HistoricalSubScores:
    base = HistoricalSubScores(
        source="test",
        home_attack_edge=0.0,
        away_attack_edge=0.0,
        home_defense_edge=0.0,
        away_defense_edge=0.0,
        form_edge=0.0,
        venue_edge=0.0,
        strength_edge=0.0,
        balance_score=0.5,
        btts_affinity=0.5,
        under_2_5_affinity=0.5,
        over_2_5_affinity=0.5,
        clean_sheet_home_affinity=0.5,
        clean_sheet_away_affinity=0.5,
        competition_goal_bias=0.0,
        matchup_nudge=0.0,
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_matchup_nudge_never_dominates_on_its_own() -> None:
    ranked = rank_scenarios(_make_subscores(matchup_nudge=0.35))
    top = ranked[0]

    assert top.score < 0.55
    assert top.breakdown.structural_support_count == 0


def test_style_affinities_do_not_beat_structure_on_their_own() -> None:
    style_only = rank_scenarios(
        _make_subscores(
            btts_affinity=1.0,
            over_2_5_affinity=1.0,
            under_2_5_affinity=0.0,
        )
    )[0]
    structural = rank_scenarios(
        _make_subscores(
            home_attack_edge=0.55,
            home_defense_edge=0.35,
            form_edge=0.30,
            venue_edge=0.25,
            strength_edge=0.20,
        )
    )[0]

    assert style_only.score < structural.score
    assert style_only.breakdown.structural_support_count < structural.breakdown.structural_support_count


def test_strength_edge_cannot_win_without_creation_and_defense_support() -> None:
    ranked = rank_scenarios(_make_subscores(strength_edge=0.9))
    top = ranked[0]

    assert top.score < 0.6
    assert top.breakdown.structural_support_count <= 1
