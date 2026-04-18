from __future__ import annotations

from datetime import datetime

from app.vnext.execution.offer_binding import bind_offers_to_template
from app.vnext.execution.models import MarketOffer
from app.vnext.execution.quality import build_execution_quality, execution_blockers
from app.vnext.markets.lines import line_template


def test_execution_blockers_tiers() -> None:
    offer = MarketOffer(
        bookmaker_id=1,
        bookmaker_name="Book",
        market_family="OU_FT",
        side="OVER",
        line=5.5,
        team_scope="NONE",
        odds_decimal=25.0,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=800,
        raw_source_ref="offer:3",
    )
    group = bind_offers_to_template(line_template("OU_FT_OVER_CORE"), (offer,))
    quality = build_execution_quality(group)
    blockers = execution_blockers(quality, group)

    assert any(blocker.tier == "QUALITY" for blocker in blockers)

    empty_group = bind_offers_to_template(line_template("OU_FT_OVER_CORE"), ())
    empty_quality = build_execution_quality(empty_group)
    empty_blockers = execution_blockers(empty_quality, empty_group)
    assert any(blocker.tier == "PRODUCT" for blocker in empty_blockers)
