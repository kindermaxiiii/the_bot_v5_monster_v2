from __future__ import annotations

from app.vnext.scenario.catalog import SCENARIO_CATALOG, SCENARIO_CATALOG_VERSION


def test_scenario_catalog_is_short_and_stable() -> None:
    keys = [definition.key for definition in SCENARIO_CATALOG]
    assert SCENARIO_CATALOG_VERSION == "sprint2_catalog.v1"
    assert keys == [
        "HOME_CONTROL",
        "AWAY_CONTROL",
        "OPEN_BALANCED",
        "CAGEY_BALANCED",
        "HOME_ATTACKING_BIAS",
        "AWAY_ATTACKING_BIAS",
        "DUAL_SCORING_BIAS",
        "HOME_DEFENSIVE_HOLD_BIAS",
        "AWAY_DEFENSIVE_HOLD_BIAS",
    ]
