from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.vnext.board.models import BoardEntry, BoardSnapshot
from app.vnext.execution.models import MarketOffer
from app.vnext.execution.selector import build_executable_market_selection
from app.vnext.runtime.deduper import Deduper
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result


def build_offer_for_template(template, bookmaker_id: int) -> MarketOffer:
    line = 0.0
    if template.suggested_line_family == "over_1_5_or_2_5":
        line = 2.5
    elif template.suggested_line_family == "under_2_5_or_3_5":
        line = 2.5
    elif template.suggested_line_family == "home_over_0_5_or_1_5":
        line = 0.5
    elif template.suggested_line_family == "away_over_0_5_or_1_5":
        line = 0.5
    elif template.suggested_line_family == "home_under_1_5_or_2_5":
        line = 1.5
    elif template.suggested_line_family == "away_under_1_5_or_2_5":
        line = 1.5
    team_scope = "HOME" if template.direction.startswith("HOME") else "AWAY" if template.direction.startswith("AWAY") else "NONE"
    return MarketOffer(
        bookmaker_id=bookmaker_id,
        bookmaker_name=f"Book {bookmaker_id}",
        market_family=template.family,
        side=template.direction,
        line=line if template.family != "BTTS" and template.family != "RESULT" else None,
        team_scope=team_scope,
        odds_decimal=1.9,
        normalized_market_label=template.family,
        offer_timestamp_utc=datetime.now(timezone.utc),
        freshness_seconds=30,
        raw_source_ref=f"offer:{bookmaker_id}",
    )


def build_publishable_result():
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = BoardSnapshot(
        entries=(
            BoardEntry(
                fixture_id=selection.translation_result.posterior_result.prior_result.fixture_id,
                internal_status="READY",
                public_status="WATCHLIST",
                board_score=0.72,
                rank=1,
                selection_result=selection,
            ),
        ),
        elite_count=0,
        watchlist_count=1,
    )
    template = selection.best_candidate.candidate.line_template
    offers = (
        build_offer_for_template(template, bookmaker_id=1),
        build_offer_for_template(template, bookmaker_id=2),
        build_offer_for_template(template, bookmaker_id=3),
    )
    execution = build_executable_market_selection(selection, offers)
    from app.vnext.pipeline.builder import build_publishable_pipeline

    pipeline = build_publishable_pipeline(board, (execution,))
    return pipeline.results[0]


def test_deduper_blocks_within_cooldown() -> None:
    result = build_publishable_result()
    deduper = Deduper(cooldown_seconds=120)
    now = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    assert deduper.is_duplicate(result, now) is False
    deduper.mark_seen(result, now)
    assert deduper.is_duplicate(result, now + timedelta(seconds=30)) is True
    assert deduper.is_duplicate(result, now + timedelta(seconds=180)) is False
