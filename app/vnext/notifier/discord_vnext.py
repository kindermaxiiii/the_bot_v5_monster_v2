from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from app.clients.discord import DiscordSendResult, send_discord_message
from app.vnext.notifier.contracts import NotifierAckRecord
from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle

logger = logging.getLogger(__name__)

DiscordSender = Callable[[str, str], DiscordSendResult]


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


def _default_sender(webhook_url: str, content: str) -> DiscordSendResult:
    return send_discord_message(webhook_url, content)


def _title_for_channel(publish_channel: str) -> str:
    channel = _normalize_channel(publish_channel)
    if channel == "ELITE":
        return "🔥 ELITE LIVE"
    return "👀 WATCHLIST LIVE"


def _competition_line(payload: PublicMatchPayload) -> str:
    competition = _clean(getattr(payload, "competition_label", ""))
    return f"🏟️ {competition}" if competition else "🏟️ Live"


def _match_line(payload: PublicMatchPayload) -> str:
    match_label = _clean(getattr(payload, "match_label", ""))
    return f"⚔️ {match_label}" if match_label else "⚔️ Match"


def _replace_many(value: str, replacements: dict[str, str]) -> str:
    text = value
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def _humanize_internal_label(value: str) -> str:
    text = _clean(value)
    if not text:
        return ""

    direct_map = {
        "TEAM_TOTAL_HOME_UNDER_CORE": "Total domicile - Under 1.5",
        "TEAM_TOTAL_AWAY_UNDER_CORE": "Total extérieur - Under 1.5",
        "TEAM_TOTAL_HOME_OVER_CORE": "Total domicile - Over 0.5",
        "TEAM_TOTAL_AWAY_OVER_CORE": "Total extérieur - Over 0.5",
        "OU_FT_UNDER_CORE": "Total match - Under 2.5",
        "OU_FT_OVER_CORE": "Total match - Over 2.5",
        "BTTS_NO_CORE": "Les deux équipes marquent - Non",
        "BTTS_YES_CORE": "Les deux équipes marquent - Oui",
        "RESULT_HOME_CORE": "Victoire domicile",
        "RESULT_AWAY_CORE": "Victoire extérieur",
        "RESULT_DRAW_CORE": "Match nul",
        "Team Total Home Under Core": "Total domicile - Under 1.5",
        "Team Total Away Under Core": "Total extérieur - Under 1.5",
        "Team Total Home Over Core": "Total domicile - Over 0.5",
        "Team Total Away Over Core": "Total extérieur - Over 0.5",
        "Under FT Core": "Total match - Under 2.5",
        "Over FT Core": "Total match - Over 2.5",
        "BTTS No Core": "Les deux équipes marquent - Non",
        "BTTS Yes Core": "Les deux équipes marquent - Oui",
    }
    if text in direct_map:
        return direct_map[text]

    text = text.replace("_", " ")
    text = _replace_many(
        text,
        {
            "TEAM TOTAL": "Total équipe",
            "HOME": "domicile",
            "AWAY": "extérieur",
            "UNDER": "Under",
            "OVER": "Over",
            "BTTS": "Les deux équipes marquent",
            "RESULT": "Résultat",
            " FT ": " match ",
            " CORE": "",
        },
    )
    text = " ".join(text.split())
    return text[:1].upper() + text[1:] if text else text


def _market_line(payload: PublicMatchPayload) -> str:
    line_label = _clean(getattr(payload, "line_label", ""))
    market_label = _clean(getattr(payload, "market_label", ""))
    public_summary = _clean(getattr(payload, "public_summary", ""))

    preferred = line_label or market_label
    humanized = _humanize_internal_label(preferred)
    if humanized:
        return f"🎯 {humanized}"

    if public_summary:
        return f"🎯 {public_summary}"

    return "🎯 Lecture live"


def _book_line(payload: PublicMatchPayload) -> str:
    bookmaker = _clean(getattr(payload, "bookmaker_label", "")) or "Book"
    odds = _clean(getattr(payload, "odds_label", "")) or "-"
    confidence = _clean(getattr(payload, "confidence_band", "")) or "-"
    return f"💸 {bookmaker} • {odds} • {confidence}"


def _render_payload_block(payload: PublicMatchPayload) -> str:
    lines = [
        _competition_line(payload),
        _match_line(payload),
        _market_line(payload),
        _book_line(payload),
    ]
    return "\n".join(line for line in lines if line.strip())


def _render_bundle(bundle: PublicMessageBundle) -> str:
    header = _title_for_channel(getattr(bundle, "publish_channel", "WATCHLIST"))
    payload_blocks = [_render_payload_block(payload) for payload in bundle.payloads]
    payload_blocks = [block for block in payload_blocks if block.strip()]
    if not payload_blocks:
        return header
    return "\n\n".join([header, *payload_blocks])


def prepare_discord_messages(bundles: tuple[PublicMessageBundle, ...]) -> tuple[str, ...]:
    return tuple(_render_bundle(bundle) for bundle in bundles if bundle.payloads)


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

    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> DiscordVnextSendResult:
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

            content = _render_bundle(bundle)
            attempted_count += 1
            send_result = self.sender(webhook_url, content)

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