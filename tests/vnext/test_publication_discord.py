from __future__ import annotations

from datetime import datetime

from app.vnext.board.models import BoardEntry, BoardSnapshot
from app.vnext.execution.models import MarketOffer
from app.vnext.execution.selector import build_executable_market_selection
from app.vnext.governance.models import InternalMatchStatus
from app.vnext.notifier.discord_vnext import prepare_discord_messages
from app.vnext.pipeline.builder import build_publishable_pipeline
from app.vnext.publication.builder import build_publication_bundles
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result


def build_offer_for_template(template, bookmaker_id: int) -> MarketOffer:
    line = 0.0
    if template.suggested_line_family == "over_1_5_or_2_5":
        line = 2.5
    return MarketOffer(
        bookmaker_id=bookmaker_id,
        bookmaker_name=f"Book {bookmaker_id}",
        market_family=template.family,
        side=template.direction,
        line=line,
        team_scope="HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE",
        odds_decimal=1.9,
        normalized_market_label=template.family,
        offer_timestamp_utc=datetime.utcnow(),
        freshness_seconds=30,
        raw_source_ref=f"offer:{bookmaker_id}",
    )


def build_board_snapshot(selection, public_status: str) -> BoardSnapshot:
    return BoardSnapshot(
        entries=(
            BoardEntry(
                fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                internal_status="READY",
                public_status=public_status,  # type: ignore[arg-type]
                board_score=0.74,
                rank=1,
                selection_result=selection,
            ),
        ),
        elite_count=1 if public_status == "ELITE" else 0,
        watchlist_count=1 if public_status == "WATCHLIST" else 0,
    )


def test_bundle_empty_yields_no_messages() -> None:
    assert prepare_discord_messages(()) == ()


def test_elite_and_watchlist_distinct_rendering() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    offers = (
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=1),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=2),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=3),
    )
    execution = build_executable_market_selection(selection, offers)

    elite_board = build_board_snapshot(selection, "ELITE")
    watchlist_board = build_board_snapshot(selection, "WATCHLIST")

    elite_pipeline = build_publishable_pipeline(elite_board, (execution,))
    watchlist_pipeline = build_publishable_pipeline(watchlist_board, (execution,))

    elite_bundles = build_publication_bundles(elite_pipeline)
    watchlist_bundles = build_publication_bundles(watchlist_pipeline)

    elite_messages = prepare_discord_messages(elite_bundles)
    watchlist_messages = prepare_discord_messages(watchlist_bundles)

    assert elite_messages
    assert watchlist_messages
    assert elite_messages[0].startswith("[ELITE]")
    assert watchlist_messages[0].startswith("[WATCHLIST]")
