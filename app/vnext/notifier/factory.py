from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.vnext.notifier.contracts import VnextNotifier
from app.vnext.notifier.discord_vnext import DiscordVnextNotifier


NotifierKind = Literal["none", "discord"]


@dataclass(slots=True, frozen=True)
class VnextNotifierBinding:
    notifier: VnextNotifier | None
    enable_send: bool
    resolved_kind: NotifierKind
    warning: str | None = None


def build_vnext_notifier(
    kind: NotifierKind,
    *,
    discord_webhook_url: str = "",
    discord_elite_webhook_url: str | None = None,
    discord_watchlist_webhook_url: str | None = None,
) -> VnextNotifierBinding:
    if kind == "none":
        return VnextNotifierBinding(
            notifier=None,
            enable_send=False,
            resolved_kind="none",
            warning=None,
        )

    if kind != "discord":
        raise ValueError("unsupported_vnext_notifier_kind")

    notifier = DiscordVnextNotifier(
        webhook_url=discord_webhook_url or None,
        elite_webhook_url=discord_elite_webhook_url,
        watchlist_webhook_url=discord_watchlist_webhook_url,
    )
    if not notifier.has_any_webhook():
        return VnextNotifierBinding(
            notifier=None,
            enable_send=False,
            resolved_kind="none",
            warning="discord_webhook_missing",
        )

    return VnextNotifierBinding(
        notifier=notifier,
        enable_send=True,
        resolved_kind="discord",
        warning=None,
    )
