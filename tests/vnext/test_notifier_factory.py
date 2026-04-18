from __future__ import annotations

from app.vnext.notifier.discord_vnext import DiscordVnextNotifier
from app.vnext.notifier.factory import build_vnext_notifier


def test_build_vnext_notifier_none_disables_send() -> None:
    binding = build_vnext_notifier("none")

    assert binding.notifier is None
    assert binding.enable_send is False
    assert binding.resolved_kind == "none"
    assert binding.warning is None


def test_build_vnext_notifier_discord_returns_concrete_notifier() -> None:
    binding = build_vnext_notifier(
        "discord",
        discord_webhook_url="https://discord.example/webhook",
    )

    assert isinstance(binding.notifier, DiscordVnextNotifier)
    assert binding.enable_send is True
    assert binding.resolved_kind == "discord"
    assert binding.warning is None


def test_build_vnext_notifier_discord_without_webhook_falls_back_cleanly() -> None:
    binding = build_vnext_notifier(
        "discord",
        discord_webhook_url="",
        discord_elite_webhook_url="",
        discord_watchlist_webhook_url="",
    )

    assert binding.notifier is None
    assert binding.enable_send is False
    assert binding.resolved_kind == "none"
    assert binding.warning == "discord_webhook_missing"
