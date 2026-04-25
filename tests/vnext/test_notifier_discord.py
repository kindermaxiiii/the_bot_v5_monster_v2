from __future__ import annotations

from app.clients.discord import DiscordSendResult
from app.vnext.notifier.discord_vnext import DiscordVnextNotifier
from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle


def _build_bundle(publish_channel: str, fixture_id: int) -> PublicMessageBundle:
    payload = PublicMatchPayload(
        fixture_id=fixture_id,
        public_status=publish_channel,  # type: ignore[arg-type]
        publish_channel=publish_channel,  # type: ignore[arg-type]
        match_label=f"Match {fixture_id}",
        competition_label="Premier Test",
        market_label="TEAM_TOTAL",
        line_label="Team Total Away Under Core",
        bookmaker_label="Book 1",
        odds_label="1.87",
        confidence_band="HIGH",
        public_summary=f"Summary {fixture_id}",
    )
    return PublicMessageBundle(
        publish_channel=publish_channel,  # type: ignore[arg-type]
        payloads=(payload,),
    )


def test_discord_vnext_notifier_returns_explicit_ack_result() -> None:
    sent_messages: list[tuple[str, str]] = []

    def fake_sender(webhook_url: str, content: str) -> DiscordSendResult:
        sent_messages.append((webhook_url, content))
        return DiscordSendResult(ok=True, status_code=200)

    notifier = DiscordVnextNotifier(
        webhook_url="https://discord.example/webhook",
        sender=fake_sender,
    )

    result = notifier.send((_build_bundle("WATCHLIST", 999),))

    assert result.attempted_count == 1
    assert result.notified_count == 1
    assert result.mode == "explicit_ack"
    assert len(result.acked_records) == 1
    assert result.acked_records[0].fixture_id == 999
    assert result.acked_records[0].public_status == "WATCHLIST"
    assert sent_messages == [
        (
            "https://discord.example/webhook",
            "\n".join(
                [
                    "👀 WATCHLIST LIVE",
                    "🏟️ Premier Test",
                    "⚔️ Match 999",
                    "🎯 Total extérieur - Under 1.5",
                    "💸 Book 1 • 1.87 • HIGH",
                ]
            ),
        )
    ]


def test_discord_vnext_notifier_routes_by_publish_channel() -> None:
    sent_messages: list[tuple[str, str]] = []

    def fake_sender(webhook_url: str, content: str) -> DiscordSendResult:
        sent_messages.append((webhook_url, content))
        return DiscordSendResult(ok=True, status_code=200)

    notifier = DiscordVnextNotifier(
        elite_webhook_url="https://discord.example/elite",
        watchlist_webhook_url="https://discord.example/watchlist",
        sender=fake_sender,
    )

    result = notifier.send(
        (
            _build_bundle("ELITE", 1000),
            _build_bundle("WATCHLIST", 1001),
        )
    )

    assert result.attempted_count == 2
    assert result.notified_count == 2
    assert result.mode == "explicit_ack"
    assert len(result.acked_records) == 2
    assert sent_messages[0][0] == "https://discord.example/elite"
    assert sent_messages[1][0] == "https://discord.example/watchlist"


def test_discord_vnext_notifier_only_counts_sender_attempts() -> None:
    def fake_sender(_webhook_url: str, _content: str) -> DiscordSendResult:
        return DiscordSendResult(ok=False, status_code=500, error="send_failed")

    notifier = DiscordVnextNotifier(
        elite_webhook_url="",
        watchlist_webhook_url="https://discord.example/watchlist",
        sender=fake_sender,
    )

    result = notifier.send(
        (
            _build_bundle("WATCHLIST", 1001),
            _build_bundle("ELITE", 1002),
        )
    )

    assert result.attempted_count == 1
    assert result.notified_count == 0
    assert result.acked_records == ()
    assert result.mode == "explicit_ack"