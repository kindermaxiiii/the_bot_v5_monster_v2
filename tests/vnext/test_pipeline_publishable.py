from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from app.vnext.board.arbiter import build_board_snapshot
from app.vnext.board.models import BoardEntry, BoardSnapshot
from app.vnext.execution.models import MarketOffer
from app.vnext.execution.selector import build_executable_market_selection
from app.vnext.governance.models import InternalMatchStatus, PublicMatchStatus
from app.vnext.pipeline.builder import build_publishable_pipeline
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result


def build_forced_board_snapshot(
    selection,
    public_status: PublicMatchStatus = "WATCHLIST",
    internal_status: InternalMatchStatus = "READY",
) -> BoardSnapshot:
    return BoardSnapshot(
        entries=(
            BoardEntry(
                fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                internal_status=internal_status,
                public_status=public_status,
                board_score=0.72,
                rank=1,
                selection_result=selection,
            ),
        ),
        elite_count=1 if public_status == "ELITE" else 0,
        watchlist_count=1 if public_status == "WATCHLIST" else 0,
    )


def build_offer_for_template(template, bookmaker_id: int, odds: float, freshness: int) -> MarketOffer:
    preferred_lines = {
        "over_1_5_or_2_5": (2.5, 1.5),
        "under_2_5_or_3_5": (2.5, 3.5),
        "home_over_0_5_or_1_5": (0.5, 1.5),
        "away_over_0_5_or_1_5": (0.5, 1.5),
        "home_under_1_5_or_2_5": (1.5, 2.5),
        "away_under_1_5_or_2_5": (1.5, 2.5),
    }
    line_candidates = preferred_lines.get(template.suggested_line_family, ())
    line = line_candidates[0] if line_candidates else 0.0
    team_scope = "HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE"
    return MarketOffer(
        bookmaker_id=bookmaker_id,
        bookmaker_name=f"Book {bookmaker_id}",
        market_family=template.family,
        side=template.direction,
        line=line,
        team_scope=team_scope,
        odds_decimal=odds,
        normalized_market_label=template.family,
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=freshness,
        raw_source_ref=f"offer:{bookmaker_id}",
    )


def test_governed_watchlist_without_execution_does_not_publish() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_board_snapshot((selection,))

    pipeline = build_publishable_pipeline(board, ())

    assert pipeline.publish_count == 0
    assert pipeline.results[0].governed_public_status in {"WATCHLIST", "ELITE", "NO_BET"}
    assert pipeline.results[0].publish_status == "DO_NOT_PUBLISH"
    assert "execution_missing_for_match" in pipeline.results[0].execution_refusal_summary


def test_governed_watchlist_with_execution_publishes() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_forced_board_snapshot(selection, public_status="WATCHLIST")
    template = selection.best_candidate.candidate.line_template
    offers = (
        build_offer_for_template(template, bookmaker_id=1, odds=1.9, freshness=30),
        build_offer_for_template(template, bookmaker_id=2, odds=1.88, freshness=45),
        build_offer_for_template(template, bookmaker_id=3, odds=1.86, freshness=60),
    )
    execution = build_executable_market_selection(selection, offers)
    assert execution.execution_candidate is not None
    pipeline = build_publishable_pipeline(board, (execution,))

    result = pipeline.results[0]
    assert result.governed_public_status == "WATCHLIST"
    assert result.publish_status == "PUBLISH"


def test_template_mismatch_blocks_publish() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_forced_board_snapshot(selection, public_status="WATCHLIST")
    template = selection.best_candidate.candidate.line_template
    offers = (
        build_offer_for_template(template, bookmaker_id=1, odds=1.9, freshness=30),
        build_offer_for_template(template, bookmaker_id=2, odds=1.88, freshness=45),
        build_offer_for_template(template, bookmaker_id=3, odds=1.86, freshness=60),
    )
    execution = build_executable_market_selection(selection, offers)
    mismatched = replace(execution, template_key="OTHER_TEMPLATE")
    pipeline = build_publishable_pipeline(board, (mismatched,))

    result = pipeline.results[0]
    assert result.publish_status == "DO_NOT_PUBLISH"
    assert "pipeline_link_mismatch" in result.execution_refusal_summary


def test_execution_candidate_without_offer_blocks_publish() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_forced_board_snapshot(selection, public_status="WATCHLIST")
    template = selection.best_candidate.candidate.line_template
    offers = (
        build_offer_for_template(template, bookmaker_id=1, odds=1.9, freshness=30),
        build_offer_for_template(template, bookmaker_id=2, odds=1.88, freshness=45),
        build_offer_for_template(template, bookmaker_id=3, odds=1.86, freshness=60),
    )
    execution = build_executable_market_selection(selection, offers)
    assert execution.execution_candidate is not None
    candidate_without_offer = replace(execution.execution_candidate, selected_offer=None)
    execution_without_offer = replace(execution, offer_chosen=None, execution_candidate=candidate_without_offer)
    pipeline = build_publishable_pipeline(board, (execution_without_offer,))

    result = pipeline.results[0]
    assert result.publish_status == "DO_NOT_PUBLISH"
    assert "execution_offer_missing" in result.execution_refusal_summary
