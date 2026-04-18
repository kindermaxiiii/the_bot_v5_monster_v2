from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime

from app.vnext.board.models import BoardEntry, BoardSnapshot
from app.vnext.execution.models import MarketOffer
from app.vnext.execution.selector import build_executable_market_selection
from app.vnext.governance.models import InternalMatchStatus, PublicMatchStatus
from app.vnext.pipeline.builder import build_publishable_pipeline
from app.vnext.publication.builder import build_publication_bundles
from app.vnext.publication.models import PublicMatchPayload
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


def test_do_not_publish_generates_no_payload() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_forced_board_snapshot(selection, public_status="WATCHLIST")
    offers = (
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=1),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=2),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=3),
    )
    execution = build_executable_market_selection(selection, offers)
    no_publish = replace(execution, offer_chosen=None, execution_candidate=None, no_executable_vehicle_reason="no_offer_found")
    pipeline = build_publishable_pipeline(board, (no_publish,))

    bundles = build_publication_bundles(pipeline)

    assert bundles == ()


def test_public_payload_field_whitelist() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_forced_board_snapshot(selection, public_status="WATCHLIST")
    offers = (
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=1),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=2),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=3),
    )
    execution = build_executable_market_selection(selection, offers)
    pipeline = build_publishable_pipeline(board, (execution,))
    bundles = build_publication_bundles(pipeline)

    assert bundles
    payload = bundles[0].payloads[0]
    payload_dict = asdict(payload)
    expected_fields = set(PublicMatchPayload.__dataclass_fields__.keys())
    assert set(payload_dict.keys()) == expected_fields


def test_public_labels_not_placeholders() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_forced_board_snapshot(selection, public_status="WATCHLIST")
    offers = (
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=1),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=2),
        build_offer_for_template(selection.best_candidate.candidate.line_template, bookmaker_id=3),
    )
    execution = build_executable_market_selection(selection, offers)
    pipeline = build_publishable_pipeline(board, (execution,))
    bundles = build_publication_bundles(pipeline)

    assert bundles
    payload = bundles[0].payloads[0]
    assert not payload.match_label.startswith("Fixture ")
    assert payload.competition_label != "Competition"
