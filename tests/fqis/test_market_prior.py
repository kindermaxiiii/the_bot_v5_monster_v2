from __future__ import annotations

import math

import pytest

from app.fqis.contracts.core import BookOffer
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole
from app.fqis.probability.market_prior import (
    build_market_prior_by_intent_key,
    build_market_prior_groups,
    compare_model_to_market_prior,
    implied_probability_from_decimal_odds,
    normalize_no_vig_probabilities,
    offer_probability_key,
)


def test_implied_probability_from_decimal_odds() -> None:
    assert implied_probability_from_decimal_odds(2.0) == 0.5
    assert math.isclose(implied_probability_from_decimal_odds(1.80), 0.5555555555555556)


def test_implied_probability_rejects_invalid_odds() -> None:
    with pytest.raises(ValueError):
        implied_probability_from_decimal_odds(1.0)


def test_normalize_no_vig_probabilities() -> None:
    probabilities = normalize_no_vig_probabilities((0.55, 0.55))

    assert probabilities == (0.5, 0.5)
    assert math.isclose(sum(probabilities), 1.0, rel_tol=1e-12)


def test_build_market_prior_groups_for_two_way_market() -> None:
    offers = (
        _btts_offer(MarketSide.YES, 1.91),
        _btts_offer(MarketSide.NO, 1.91),
    )

    groups = build_market_prior_groups(offers)

    assert len(groups) == 1

    group = groups[0]

    assert group.is_complete
    assert group.outcome_count == 2
    assert group.overround > 1.0
    assert len(group.probabilities) == 2
    assert all(
        math.isclose(probability.no_vig_probability, 0.5, rel_tol=1e-12)
        for probability in group.probabilities
    )


def test_build_market_prior_groups_keeps_lines_separate() -> None:
    offers = (
        _match_total_offer(MarketSide.OVER, 2.5, 1.90),
        _match_total_offer(MarketSide.UNDER, 2.5, 1.90),
        _match_total_offer(MarketSide.OVER, 3.5, 1.95),
        _match_total_offer(MarketSide.UNDER, 3.5, 1.85),
    )

    groups = build_market_prior_groups(offers)

    assert len(groups) == 2
    assert all(group.is_complete for group in groups)
    assert {group.outcome_count for group in groups} == {2}


def test_incomplete_market_group_is_rejected() -> None:
    offers = (
        _btts_offer(MarketSide.NO, 1.80),
    )

    groups = build_market_prior_groups(offers)

    assert len(groups) == 1
    assert not groups[0].is_complete
    assert groups[0].probabilities == ()
    assert groups[0].rejection_reason is not None


def test_build_market_prior_by_intent_key_builds_consensus() -> None:
    offers = (
        _btts_offer(MarketSide.YES, 1.91, bookmaker_id=1, bookmaker_name="BookA"),
        _btts_offer(MarketSide.NO, 1.91, bookmaker_id=1, bookmaker_name="BookA"),
        _btts_offer(MarketSide.YES, 2.00, bookmaker_id=2, bookmaker_name="BookB"),
        _btts_offer(MarketSide.NO, 1.80, bookmaker_id=2, bookmaker_name="BookB"),
    )

    prior = build_market_prior_by_intent_key(offers)

    assert "BTTS|YES|NONE|NA" in prior
    assert "BTTS|NO|NONE|NA" in prior
    assert math.isclose(
        prior["BTTS|YES|NONE|NA"] + prior["BTTS|NO|NONE|NA"],
        1.0,
        rel_tol=1e-12,
    )


def test_compare_model_to_market_prior() -> None:
    comparisons = compare_model_to_market_prior(
        {
            "BTTS|NO|NONE|NA": 0.62,
            "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5": 0.80,
        },
        {
            "BTTS|NO|NONE|NA": 0.54,
        },
    )

    by_key = {comparison.intent_key: comparison for comparison in comparisons}

    assert by_key["BTTS|NO|NONE|NA"].has_market_prior
    assert math.isclose(by_key["BTTS|NO|NONE|NA"].delta_model_market, 0.08)
    assert not by_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"].has_market_prior
    assert by_key["TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"].delta_model_market is None


def test_offer_probability_key_matches_pipeline_intent_key_format() -> None:
    offer = BookOffer(
        event_id=2801,
        bookmaker_id=1,
        bookmaker_name="BookA",
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        odds_decimal=1.92,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=8,
    )

    assert offer_probability_key(offer) == "TEAM_TOTAL_AWAY|UNDER|AWAY|1.5"


def _btts_offer(
    side: MarketSide,
    odds_decimal: float,
    *,
    bookmaker_id: int = 1,
    bookmaker_name: str = "BookA",
) -> BookOffer:
    return BookOffer(
        event_id=2801,
        bookmaker_id=bookmaker_id,
        bookmaker_name=bookmaker_name,
        family=MarketFamily.BTTS,
        side=side,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=None,
        odds_decimal=odds_decimal,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=8,
    )


def _match_total_offer(
    side: MarketSide,
    line: float,
    odds_decimal: float,
) -> BookOffer:
    return BookOffer(
        event_id=2801,
        bookmaker_id=1,
        bookmaker_name="BookA",
        family=MarketFamily.MATCH_TOTAL,
        side=side,
        period=Period.FT,
        team_role=TeamRole.NONE,
        line=line,
        odds_decimal=odds_decimal,
        source_timestamp_utc="2026-04-26T00:00:00+00:00",
        freshness_seconds=8,
    )

    