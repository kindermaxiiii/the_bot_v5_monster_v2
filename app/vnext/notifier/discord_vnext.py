from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from app.clients.discord import DiscordSendResult, send_discord_message
from app.vnext.notifier.contracts import NotifierAckRecord
from app.vnext.notifier.discord_format import (
    build_bundle_webhook_payload,
    build_channel_webhook_payload,
    format_bundle,
    prepare_discord_messages,
)
from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle

logger = logging.getLogger(__name__)

DiscordSender = Callable[[str, str | dict[str, object]], DiscordSendResult]


@dataclass(slots=True, frozen=True)
class DiscordVnextSendResult:
    attempted_count: int
    notified_count: int
    acked_records: tuple[NotifierAckRecord, ...]
    mode: str = "explicit_ack"


def _clean(value: str | None) -> str:
    return (value or "").strip()


def _normalize_channel(value: str | None) -> str:
    return _clean(value).upper()


def _default_sender(webhook_url: str, message: str | dict[str, object]) -> DiscordSendResult:
    return send_discord_message(webhook_url, message)


def prepare_discord_webhook_payloads(
    bundles: tuple[PublicMessageBundle, ...],
) -> tuple[dict[str, object], ...]:
    return tuple(build_bundle_webhook_payload(bundle) for bundle in bundles if bundle.payloads)


def _payload_ack(payload: PublicMatchPayload) -> NotifierAckRecord:
    raw_odds = getattr(payload, "odds_decimal", None)
    if raw_odds is None:
        odds_label = _clean(getattr(payload, "odds_label", ""))
        try:
            raw_odds = float(odds_label.replace(",", "."))
        except ValueError:
            raw_odds = None

    return NotifierAckRecord(
        fixture_id=int(getattr(payload, "fixture_id")),
        public_status=_clean(getattr(payload, "public_status", "")) or _clean(getattr(payload, "publish_channel", "")),
        template_key=getattr(payload, "template_key", None),
        bookmaker_id=getattr(payload, "bookmaker_id", None),
        line=getattr(payload, "line", None),
        odds_decimal=raw_odds,
    )


class DiscordVnextNotifier:
    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        elite_webhook_url: str | None = None,
        watchlist_webhook_url: str | None = None,
        sender: DiscordSender | None = None,
    ) -> None:
        self.webhook_url = _clean(webhook_url)
        self.elite_webhook_url = _clean(elite_webhook_url)
        self.watchlist_webhook_url = _clean(watchlist_webhook_url)
        self.sender = sender or _default_sender
        self._sender_supports_payload_dict = sender is None

    def has_any_webhook(self) -> bool:
        return any(
            (
                self.webhook_url,
                self.elite_webhook_url,
                self.watchlist_webhook_url,
            )
        )

    def _resolve_webhook(self, publish_channel: str) -> str:
        channel = _normalize_channel(publish_channel)
        if channel == "ELITE" and self.elite_webhook_url:
            return self.elite_webhook_url
        if channel != "ELITE" and self.watchlist_webhook_url:
            return self.watchlist_webhook_url
        return self.webhook_url

    def _send_with_text_fallback(
        self,
        bundles: tuple[PublicMessageBundle, ...],
    ) -> DiscordVnextSendResult:
        attempted_count = 0
        notified_count = 0
        acked_records: list[NotifierAckRecord] = []

        for bundle in bundles:
            if not bundle.payloads:
                continue

            publish_channel = _clean(getattr(bundle, "publish_channel", "")) or "WATCHLIST"
            webhook_url = self._resolve_webhook(publish_channel)
            if not webhook_url:
                logger.warning(
                    "vnext_discord_notifier_missing_webhook publish_channel=%s",
                    publish_channel,
                )
                continue

            outbound_message = format_bundle(bundle)
            attempted_count += 1
            send_result = self.sender(webhook_url, outbound_message)

            if not send_result.ok:
                logger.warning(
                    "vnext_discord_notifier_send_failed publish_channel=%s status_code=%s error=%s",
                    publish_channel,
                    send_result.status_code,
                    send_result.error,
                )
                continue

            notified_count += 1
            acked_records.extend(_payload_ack(payload) for payload in bundle.payloads)

        return DiscordVnextSendResult(
            attempted_count=attempted_count,
            notified_count=notified_count,
            acked_records=tuple(acked_records),
            mode="explicit_ack",
        )

    def _send_batched_embeds(
        self,
        bundles: tuple[PublicMessageBundle, ...],
    ) -> DiscordVnextSendResult:
        attempted_count = 0
        notified_count = 0
        acked_records: list[NotifierAckRecord] = []

        bundles_by_channel: dict[str, list[PublicMessageBundle]] = {}
        for bundle in bundles:
            if not bundle.payloads:
                continue
            publish_channel = _clean(getattr(bundle, "publish_channel", "")) or "WATCHLIST"
            channel = _normalize_channel(publish_channel) or "WATCHLIST"
            bundles_by_channel.setdefault(channel, []).append(bundle)

        for channel, channel_bundles in bundles_by_channel.items():
            webhook_url = self._resolve_webhook(channel)
            if not webhook_url:
                logger.warning(
                    "vnext_discord_notifier_missing_webhook publish_channel=%s",
                    channel,
                )
                continue

            payload = build_channel_webhook_payload(channel, tuple(channel_bundles))
            attempted_count += 1
            send_result = self.sender(webhook_url, payload)

            if not send_result.ok:
                logger.warning(
                    "vnext_discord_notifier_send_failed publish_channel=%s status_code=%s error=%s",
                    channel,
                    send_result.status_code,
                    send_result.error,
                )
                continue

            sent_payload_count = sum(len(bundle.payloads) for bundle in channel_bundles)
            notified_count += sent_payload_count
            for bundle in channel_bundles:
                acked_records.extend(_payload_ack(payload_item) for payload_item in bundle.payloads)

        return DiscordVnextSendResult(
            attempted_count=attempted_count,
            notified_count=notified_count,
            acked_records=tuple(acked_records),
            mode="explicit_ack",
        )

    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> DiscordVnextSendResult:
        if self._sender_supports_payload_dict:
            return self._send_batched_embeds(bundles)
        return self._send_with_text_fallback(bundles)