from __future__ import annotations

import logging
from typing import Callable

from app.clients.discord import DiscordSendResult, discord_client
from app.config import settings
from app.vnext.notifier.contracts import NotifierSendResult, VnextNotifier
from app.vnext.notifier.discord_format import format_bundle
from app.vnext.publication.models import PublicMessageBundle


logger = logging.getLogger(__name__)


def prepare_discord_messages(bundles: tuple[PublicMessageBundle, ...]) -> tuple[str, ...]:
    if not bundles:
        return ()
    return tuple(format_bundle(bundle) for bundle in bundles)


class DiscordVnextNotifier(VnextNotifier):
    def __init__(
        self,
        *,
        webhook_url: str | None = None,
        elite_webhook_url: str | None = None,
        watchlist_webhook_url: str | None = None,
        sender: Callable[[str, str], DiscordSendResult] | None = None,
    ) -> None:
        self.webhook_url = str(webhook_url or "").strip()
        self.elite_webhook_url = (
            str(settings.discord_webhook_real or "").strip()
            if elite_webhook_url is None
            else str(elite_webhook_url).strip()
        )
        self.watchlist_webhook_url = (
            str(settings.discord_webhook_doc or "").strip()
            if watchlist_webhook_url is None
            else str(watchlist_webhook_url).strip()
        )
        self.sender = sender or discord_client.send_message

    def has_any_webhook(self) -> bool:
        return bool(self.webhook_url or self.elite_webhook_url or self.watchlist_webhook_url)

    def _resolve_webhook_url(self, bundle: PublicMessageBundle) -> str:
        if self.webhook_url:
            return self.webhook_url
        if bundle.publish_channel == "ELITE":
            return self.elite_webhook_url
        if bundle.publish_channel == "WATCHLIST":
            return self.watchlist_webhook_url
        return ""

    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> NotifierSendResult:
        attempted_count = 0
        notified_count = 0
        messages = prepare_discord_messages(bundles)

        for bundle, message in zip(bundles, messages):
            webhook_url = self._resolve_webhook_url(bundle)
            if not webhook_url:
                logger.warning(
                    "vnext_discord_notifier_missing_webhook publish_channel=%s",
                    bundle.publish_channel,
                )
                continue

            attempted_count += 1
            try:
                result = self.sender(webhook_url, message)
            except Exception as exc:
                logger.warning(
                    "vnext_discord_notifier_send_failed publish_channel=%s error=%s",
                    bundle.publish_channel,
                    exc,
                )
                continue

            if result.ok:
                notified_count += 1

        return NotifierSendResult(
            attempted_count=attempted_count,
            notified_count=notified_count,
            acked_records=(),
            mode="aggregate",
        )
