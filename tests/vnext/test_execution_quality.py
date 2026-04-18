from __future__ import annotations

from datetime import datetime

from app.vnext.execution.offer_binding import bind_offers_to_template
from app.vnext.execution.quality import build_execution_quality
from app.vnext.execution.models import MarketOffer
from app.vnext.markets.lines import line_template


def test_retrievability_and_publishability_are_distinct() -> None:
    offer = MarketOffer(
        bookmaker_id=1,
        bookmaker_name="Book",
        market_family="BTTS",
        side="YES",
        line=None,
        team_scope="NONE",
        odds_decimal=2.1,
        normalized_market_label="BTTS",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=300,
        raw_source_ref="offer:2",
    )
    group = bind_offers_to_template(line_template("BTTS_YES_CORE"), (offer,))
    quality = build_execution_quality(group)

    assert quality.retrievability_score > 0.0
    assert quality.publishability_score > 0.0
    assert quality.publishability_score != quality.retrievability_score
    assert quality.freshness_score > 0.0


def test_quality_uses_bound_offer_subset() -> None:
    offer_good = MarketOffer(
        bookmaker_id=1,
        bookmaker_name="Book A",
        market_family="OU_FT",
        side="OVER",
        line=2.5,
        team_scope="NONE",
        odds_decimal=1.9,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=45,
        raw_source_ref="offer:10",
    )
    offer_offline = MarketOffer(
        bookmaker_id=2,
        bookmaker_name="Book B",
        market_family="OU_FT",
        side="OVER",
        line=3.0,
        team_scope="NONE",
        odds_decimal=2.6,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=800,
        raw_source_ref="offer:11",
    )
    group = bind_offers_to_template(line_template("OU_FT_OVER_CORE"), (offer_good, offer_offline))
    quality = build_execution_quality(group)
    assert quality.template_binding_score >= 0.7
