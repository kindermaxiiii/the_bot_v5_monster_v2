from __future__ import annotations

from datetime import datetime

from app.vnext.execution.offer_binding import bind_offers_to_template
from app.vnext.execution.models import MarketOffer
from app.vnext.markets.lines import line_template


def _offer(*, line: float | None, odds: float, side: str) -> MarketOffer:
    return MarketOffer(
        bookmaker_id=1,
        bookmaker_name="Book",
        market_family="OU_FT",
        side=side,  # type: ignore[arg-type]
        line=line,
        team_scope="NONE",
        odds_decimal=odds,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=30,
        raw_source_ref="offer:1",
    )


def test_binding_exact_vs_relaxed() -> None:
    template = line_template("OU_FT_OVER_CORE")
    exact_group = bind_offers_to_template(template, (_offer(line=2.5, odds=1.9, side="OVER"),))
    assert exact_group.template_binding_status == "EXACT"
    assert exact_group.bound_line == 2.5

    relaxed_group = bind_offers_to_template(template, (_offer(line=3.0, odds=2.4, side="OVER"),))
    assert relaxed_group.template_binding_status == "RELAXED"
    assert relaxed_group.bound_line == 3.0

    no_group = bind_offers_to_template(template, ())
    assert no_group.template_binding_status == "NO_BIND"


def test_selected_offer_uses_bound_line() -> None:
    template = line_template("OU_FT_OVER_CORE")
    offers = (
        _offer(line=2.5, odds=1.9, side="OVER"),
        _offer(line=3.0, odds=2.6, side="OVER"),
    )
    group = bind_offers_to_template(template, offers)

    assert group.template_binding_status == "EXACT"
    assert group.bound_line == 2.5
