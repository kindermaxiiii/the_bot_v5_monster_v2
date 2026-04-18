from __future__ import annotations

from datetime import datetime

from app.vnext.execution.models import MarketOffer
from app.vnext.execution.selector import build_executable_market_selection
from tests.vnext.live_factories import build_reference_posterior_result
from app.vnext.selection.match_selector import build_match_market_selection_result
from app.vnext.markets.lines import line_template
from app.vnext.markets.models import MarketCandidate, MarketSupportBreakdown, MarketTranslationResult
from app.vnext.selection.models import MatchBestCandidate, MatchMarketSelectionResult


def _offer(*, bookmaker_id: int, odds: float, line: float | None = 2.5) -> MarketOffer:
    return MarketOffer(
        bookmaker_id=bookmaker_id,
        bookmaker_name=f"Book {bookmaker_id}",
        market_family="OU_FT",
        side="OVER",
        line=line,
        team_scope="NONE",
        odds_decimal=odds,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=45,
        raw_source_ref=f"offer:{bookmaker_id}",
    )


def test_execution_selector_picks_best_offer() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    offers = (_offer(bookmaker_id=1, odds=1.85), _offer(bookmaker_id=2, odds=1.95))

    result = build_executable_market_selection(selection, offers)

    assert result.execution_candidate is not None
    assert result.offer_chosen is not None
    assert result.no_executable_vehicle_reason is None


def test_selected_offer_respects_bound_line() -> None:
    posterior = build_reference_posterior_result()
    support_breakdown = MarketSupportBreakdown(
        scenario_support_score=0.7,
        attack_support_score=0.7,
        defensive_support_score=0.6,
        directionality_score=0.65,
        live_support_score=0.6,
        reliability_score=posterior.posterior_reliability.posterior_reliability_score,
        conflict_score=0.1,
        supporting_scenarios=("HOME_CONTROL",),
        supporting_signals=("attack_support",),
    )
    candidate = MarketCandidate(
        fixture_id=posterior.prior_result.fixture_id,
        family="OU_FT",
        maturity="APPROVED",
        line_template=line_template("OU_FT_OVER_CORE"),
        exists=True,
        is_blocked=False,
        is_selectable=True,
        support_score=0.72,
        confidence_score=0.68,
        support_breakdown=support_breakdown,
        blockers=(),
    )
    translation = MarketTranslationResult(posterior_result=posterior, candidates=(candidate,))
    selection = MatchMarketSelectionResult(
        translation_result=translation,
        best_candidate=MatchBestCandidate(candidate=candidate, selection_score=0.7),
        no_selection_reason=None,
    )
    offers = (
        _offer(bookmaker_id=1, odds=1.9, line=2.5),
        _offer(bookmaker_id=2, odds=1.88, line=2.5),
        _offer(bookmaker_id=3, odds=2.4, line=3.0),
    )

    result = build_executable_market_selection(selection, offers)

    assert result.execution_candidate is not None
    assert result.offer_chosen is not None
    assert result.execution_candidate.template_binding_status == "EXACT"
    assert result.offer_chosen.line == 2.5


def test_freshness_can_beat_better_odds() -> None:
    posterior = build_reference_posterior_result()
    support_breakdown = MarketSupportBreakdown(
        scenario_support_score=0.7,
        attack_support_score=0.7,
        defensive_support_score=0.6,
        directionality_score=0.65,
        live_support_score=0.6,
        reliability_score=posterior.posterior_reliability.posterior_reliability_score,
        conflict_score=0.1,
        supporting_scenarios=("HOME_CONTROL",),
        supporting_signals=("attack_support",),
    )
    candidate = MarketCandidate(
        fixture_id=posterior.prior_result.fixture_id,
        family="OU_FT",
        maturity="APPROVED",
        line_template=line_template("OU_FT_OVER_CORE"),
        exists=True,
        is_blocked=False,
        is_selectable=True,
        support_score=0.72,
        confidence_score=0.68,
        support_breakdown=support_breakdown,
        blockers=(),
    )
    translation = MarketTranslationResult(posterior_result=posterior, candidates=(candidate,))
    selection = MatchMarketSelectionResult(
        translation_result=translation,
        best_candidate=MatchBestCandidate(candidate=candidate, selection_score=0.7),
        no_selection_reason=None,
    )
    fresh = MarketOffer(
        bookmaker_id=1,
        bookmaker_name="Book Fresh",
        market_family="OU_FT",
        side="OVER",
        line=2.5,
        team_scope="NONE",
        odds_decimal=1.85,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=30,
        raw_source_ref="offer:fresh",
    )
    stale = MarketOffer(
        bookmaker_id=2,
        bookmaker_name="Book Stale",
        market_family="OU_FT",
        side="OVER",
        line=2.5,
        team_scope="NONE",
        odds_decimal=2.2,
        normalized_market_label="OU_FT",
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=700,
        raw_source_ref="offer:stale",
    )

    result = build_executable_market_selection(selection, (fresh, stale))

    assert result.offer_chosen is not None
    assert result.offer_chosen.bookmaker_id == 1


def test_execution_selector_no_offer() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)

    result = build_executable_market_selection(selection, ())

    assert result.execution_candidate is None
    assert result.no_executable_vehicle_reason == "no_offer_found"
