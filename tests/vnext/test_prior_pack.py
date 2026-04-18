from __future__ import annotations

from app.vnext.prior.builder import build_historical_prior_pack
from tests.vnext.factories import build_reference_dataset


def test_historical_prior_pack_is_complete_for_target_fixture() -> None:
    dataset = build_reference_dataset()
    pack = build_historical_prior_pack(dataset, fixture_id=999)

    assert pack.source_version == "vnext_sprint1"
    assert pack.fixture_id == 999
    assert pack.attack_context.home.xg_for_per_match > 0.0
    assert pack.defense_context.away.goals_against_per_match >= 0.0
    assert pack.venue_context.home.venue == "HOME"
    assert pack.form_context.home.form_score >= 0.0
    assert pack.strength_context.home.global_rating > 0.0
    assert pack.style_context.away.btts_rate >= 0.0
    assert pack.matchup_context.matchup.seasons_covered <= 3
    assert pack.competition_context.competition.competition_confidence_score > 0.0


def test_prior_pack_quality_flags_bubble_up_from_sparse_history() -> None:
    dataset = build_reference_dataset(sparse_team_ids={2})
    pack = build_historical_prior_pack(dataset, fixture_id=999)

    assert pack.attack_context.data_quality_flag in {"LOW", "MEDIUM"}
    assert pack.form_context.data_quality_flag in {"LOW", "MEDIUM"}
    assert pack.strength_context.data_quality_flag in {"LOW", "MEDIUM"}
