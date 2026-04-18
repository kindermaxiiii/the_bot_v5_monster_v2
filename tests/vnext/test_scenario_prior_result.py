from __future__ import annotations

from app.vnext.prior.builder import build_historical_prior_pack
from app.vnext.scenario.builder import build_scenario_prior_result
from tests.vnext.factories import build_reference_dataset


def test_scenario_prior_result_is_complete_and_explainable() -> None:
    dataset = build_reference_dataset()
    pack = build_historical_prior_pack(dataset, fixture_id=999)
    result = build_scenario_prior_result(pack)

    assert result.catalog_version == "sprint2_catalog.v1"
    assert result.source_version == "scenario_prior.v1"
    assert len(result.scenarios) == 9
    assert result.top_scenario.key in {
        "HOME_CONTROL",
        "HOME_ATTACKING_BIAS",
        "HOME_DEFENSIVE_HOLD_BIAS",
    }
    assert result.prior_reliability.prior_reliability_score > 0.0
    assert result.top_scenario.breakdown.structural_support_count >= 2
    assert len(result.top_scenario.supporting_subscores) >= 2
    assert result.scenarios[0].score >= result.scenarios[1].score
