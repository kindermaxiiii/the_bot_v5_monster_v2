from __future__ import annotations

from app.vnext.notifier.contracts import (
    AggregateCountVnextNotifier,
    ExplicitAckVnextNotifier,
    NoopVnextNotifier,
    NotifierAckRecord,
    adapt_vnext_notifier,
    send_with_notifier,
)
from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle


def _build_bundle() -> PublicMessageBundle:
    payload = PublicMatchPayload(
        fixture_id=999,
        public_status="WATCHLIST",
        publish_channel="WATCHLIST",
        match_label="Lions vs Falcons",
        competition_label="Premier Test",
        market_label="TEAM_TOTAL",
        line_label="Team Total Away Under Core",
        bookmaker_label="Book 1",
        odds_label="1.87",
        confidence_band="HIGH",
        public_summary="TEAM_TOTAL Team Total Away Under Core @ Book 1 1.87",
    )
    return PublicMessageBundle(
        publish_channel="WATCHLIST",
        payloads=(payload,),
    )


def test_adapt_vnext_notifier_wraps_legacy_callable() -> None:
    bundles = (_build_bundle(),)

    def legacy_notifier(_bundles) -> int:
        return 1

    adapted = adapt_vnext_notifier(legacy_notifier)

    assert adapted is not None
    assert adapted.send(bundles) == 1

    resolved = send_with_notifier(legacy_notifier, bundles)

    assert resolved.attempted_count == 1
    assert resolved.notified_count == 1
    assert resolved.acked_records == ()
    assert resolved.mode == "aggregate"


def test_send_with_notifier_supports_explicit_ack_notifier_objects() -> None:
    bundles = (_build_bundle(),)
    notifier = ExplicitAckVnextNotifier(
        acked_records=(
            NotifierAckRecord(
                fixture_id=999,
                public_status="WATCHLIST",
                template_key="TEAM_TOTAL_AWAY_UNDER_CORE",
                bookmaker_id=1,
                line=1.5,
                odds_decimal=1.9,
            ),
        ),
    )

    resolved = send_with_notifier(notifier, bundles)

    assert resolved.attempted_count == 1
    assert resolved.notified_count == 1
    assert len(resolved.acked_records) == 1
    assert resolved.mode == "explicit_ack"


def test_send_with_notifier_supports_aggregate_notifier_objects() -> None:
    bundles = (_build_bundle(),)

    resolved = send_with_notifier(AggregateCountVnextNotifier(), bundles)

    assert resolved.attempted_count == 1
    assert resolved.notified_count == 1
    assert resolved.acked_records == ()
    assert resolved.mode == "aggregate"


def test_send_with_notifier_supports_noop_notifier_objects() -> None:
    bundles = (_build_bundle(),)

    resolved = send_with_notifier(NoopVnextNotifier(), bundles)

    assert resolved.attempted_count == 0
    assert resolved.notified_count == 0
    assert resolved.acked_records == ()
    assert resolved.mode == "none"
