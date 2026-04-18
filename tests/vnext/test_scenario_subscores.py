from __future__ import annotations

from app.vnext.prior.builder import build_historical_prior_pack
from app.vnext.scenario.subscores import build_historical_subscores, build_prior_reliability
from tests.vnext.factories import build_reference_dataset


def test_historical_subscores_are_populated_and_structural() -> None:
    dataset = build_reference_dataset()
    pack = build_historical_prior_pack(dataset, fixture_id=999)
    subscores = build_historical_subscores(pack)

    assert subscores.home_attack_edge > 0.0
    assert subscores.strength_edge > 0.0
    assert subscores.form_edge > 0.0
    assert 0.0 <= subscores.balance_score <= 1.0
    assert 0.0 <= subscores.btts_affinity <= 1.0
    assert -1.0 <= subscores.matchup_nudge <= 1.0


def test_prior_reliability_score_drops_with_sparse_history() -> None:
    full_dataset = build_reference_dataset()
    sparse_dataset = build_reference_dataset(sparse_team_ids={2})

    full_pack = build_historical_prior_pack(full_dataset, fixture_id=999)
    sparse_pack = build_historical_prior_pack(sparse_dataset, fixture_id=999)

    full_reliability = build_prior_reliability(full_pack)
    sparse_reliability = build_prior_reliability(sparse_pack)

    assert full_reliability.prior_reliability_score > sparse_reliability.prior_reliability_score
    assert full_reliability.sample_size_score == sparse_reliability.sample_size_score
    assert full_reliability.data_quality_score > sparse_reliability.data_quality_score
