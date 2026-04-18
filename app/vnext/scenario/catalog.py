from __future__ import annotations

from app.vnext.scenario.models import ScenarioDefinition


SCENARIO_CATALOG_VERSION = "sprint2_catalog.v1"


SCENARIO_CATALOG: tuple[ScenarioDefinition, ...] = (
    ScenarioDefinition(
        key="HOME_CONTROL",
        label="Home Control",
        structural_weights={
            "home_attack_edge": 0.24,
            "home_defense_edge": 0.20,
            "form_edge": 0.14,
            "venue_edge": 0.12,
            "strength_edge": 0.12,
        },
        style_weights={
            "under_2_5_affinity": 0.06,
            "clean_sheet_home_affinity": 0.08,
        },
        matchup_weight=0.04,
        minimum_structural_supports=3,
        description="Home side structurally stronger and able to control the match.",
    ),
    ScenarioDefinition(
        key="AWAY_CONTROL",
        label="Away Control",
        structural_weights={
            "away_attack_edge": 0.24,
            "away_defense_edge": 0.20,
            "form_edge": -0.14,
            "venue_edge": -0.12,
            "strength_edge": -0.12,
        },
        style_weights={
            "under_2_5_affinity": 0.06,
            "clean_sheet_away_affinity": 0.08,
        },
        matchup_weight=-0.04,
        minimum_structural_supports=3,
        description="Away side structurally stronger and able to control the match.",
    ),
    ScenarioDefinition(
        key="OPEN_BALANCED",
        label="Open Balanced",
        structural_weights={
            "balance_score": 0.12,
            "home_attack_edge": 0.14,
            "away_attack_edge": 0.14,
            "home_defense_edge": -0.08,
            "away_defense_edge": -0.08,
            "competition_goal_bias": 0.08,
        },
        style_weights={
            "btts_affinity": 0.14,
            "over_2_5_affinity": 0.16,
        },
        minimum_structural_supports=3,
        description="Balanced teams in a more open game environment.",
    ),
    ScenarioDefinition(
        key="CAGEY_BALANCED",
        label="Cagey Balanced",
        structural_weights={
            "balance_score": 0.12,
            "home_defense_edge": 0.10,
            "away_defense_edge": 0.10,
            "competition_goal_bias": -0.08,
        },
        style_weights={
            "under_2_5_affinity": 0.18,
            "clean_sheet_home_affinity": 0.08,
            "clean_sheet_away_affinity": 0.08,
        },
        minimum_structural_supports=3,
        description="Balanced teams in a more suppressed game environment.",
    ),
    ScenarioDefinition(
        key="HOME_ATTACKING_BIAS",
        label="Home Attacking Bias",
        structural_weights={
            "home_attack_edge": 0.32,
            "form_edge": 0.12,
            "venue_edge": 0.12,
            "strength_edge": 0.10,
            "competition_goal_bias": 0.08,
        },
        style_weights={
            "over_2_5_affinity": 0.08,
        },
        matchup_weight=0.03,
        minimum_structural_supports=3,
        description="Home side carries the stronger attacking bias.",
    ),
    ScenarioDefinition(
        key="AWAY_ATTACKING_BIAS",
        label="Away Attacking Bias",
        structural_weights={
            "away_attack_edge": 0.32,
            "form_edge": -0.12,
            "venue_edge": -0.12,
            "strength_edge": -0.10,
            "competition_goal_bias": 0.08,
        },
        style_weights={
            "over_2_5_affinity": 0.08,
        },
        matchup_weight=-0.03,
        minimum_structural_supports=3,
        description="Away side carries the stronger attacking bias.",
    ),
    ScenarioDefinition(
        key="DUAL_SCORING_BIAS",
        label="Dual Scoring Bias",
        structural_weights={
            "home_attack_edge": 0.20,
            "away_attack_edge": 0.20,
            "home_defense_edge": -0.10,
            "away_defense_edge": -0.10,
            "balance_score": 0.08,
            "competition_goal_bias": 0.08,
        },
        style_weights={
            "btts_affinity": 0.16,
            "over_2_5_affinity": 0.14,
        },
        matchup_weight=0.04,
        minimum_structural_supports=2,
        description="Both sides have a credible scoring path in an open structure.",
    ),
    ScenarioDefinition(
        key="HOME_DEFENSIVE_HOLD_BIAS",
        label="Home Defensive Hold Bias",
        structural_weights={
            "home_defense_edge": 0.28,
            "form_edge": 0.10,
            "venue_edge": 0.08,
            "strength_edge": 0.10,
            "competition_goal_bias": -0.06,
        },
        style_weights={
            "clean_sheet_home_affinity": 0.18,
            "under_2_5_affinity": 0.12,
        },
        matchup_weight=0.02,
        minimum_structural_supports=2,
        description="Home side has the stronger defensive hold profile.",
    ),
    ScenarioDefinition(
        key="AWAY_DEFENSIVE_HOLD_BIAS",
        label="Away Defensive Hold Bias",
        structural_weights={
            "away_defense_edge": 0.28,
            "form_edge": -0.10,
            "venue_edge": -0.08,
            "strength_edge": -0.10,
            "competition_goal_bias": -0.06,
        },
        style_weights={
            "clean_sheet_away_affinity": 0.18,
            "under_2_5_affinity": 0.12,
        },
        matchup_weight=-0.02,
        minimum_structural_supports=2,
        description="Away side has the stronger defensive hold profile.",
    ),
)
