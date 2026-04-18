from __future__ import annotations

import app.vnext.notifier as notifier


def test_notifier_package_reexports_expected_symbols() -> None:
    expected = {
        "AggregateCountVnextNotifier",
        "DiscordVnextNotifier",
        "ExplicitAckVnextNotifier",
        "NoopVnextNotifier",
        "NotifierAckRecord",
        "NotifierMode",
        "NotifierSendResult",
        "VnextNotifier",
        "VnextNotifierBinding",
        "adapt_vnext_notifier",
        "build_vnext_notifier",
        "prepare_discord_messages",
        "send_with_notifier",
    }

    assert expected.issubset(set(notifier.__all__))
    for name in expected:
        assert hasattr(notifier, name)
