from __future__ import annotations

from datetime import datetime
from typing import Any

from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle


ELITE_COLOR = 0xDC2626
WATCHLIST_COLOR = 0x2563EB

CHANNEL_HEADERS = {
    "ELITE": "🔥 ELITE LIVE",
    "WATCHLIST": "👀 WATCHLIST LIVE",
}


def _clean(text: str | None, default: str = "") -> str:
    value = str(text or "").strip()
    return value if value else default


def _normalize_channel(value: str | None) -> str:
    channel = _clean(value).upper()
    return channel if channel else "WATCHLIST"


def _optional_attr(payload: PublicMatchPayload, name: str) -> str:
    return _clean(getattr(payload, name, ""), "")


def _optional_url(payload: PublicMatchPayload, *names: str) -> str:
    for name in names:
        value = _clean(getattr(payload, name, ""), "")
        if value:
            return value
    return ""


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


def _compact_selection(payload: PublicMatchPayload) -> str:
    preferred = _clean(payload.line_label) or _clean(payload.market_label) or "Sélection"
    humanized = _humanize_internal_label(preferred)
    return humanized or preferred


def _embed_color(publish_channel: str, payload: PublicMatchPayload) -> int:
    channel = _normalize_channel(publish_channel)
    confidence = _clean(payload.confidence_band).upper()

    if channel == "ELITE":
        if confidence == "HIGH":
            return 0xDC2626
        if confidence == "MEDIUM":
            return 0xF59E0B
        return 0xB91C1C

    if confidence == "HIGH":
        return 0x2563EB
    if confidence == "MEDIUM":
        return 0x1D4ED8
    return WATCHLIST_COLOR


def _field(name: str, value: str, *, inline: bool = True) -> dict[str, object]:
    return {
        "name": name,
        "value": value,
        "inline": inline,
    }


def _timestamp_value(payload: PublicMatchPayload) -> str | None:
    for attr_name in ("timestamp_utc", "event_timestamp_utc", "offer_timestamp_utc"):
        raw = getattr(payload, attr_name, None)
        if raw is None:
            continue
        if isinstance(raw, datetime):
            return raw.isoformat()
        text = _clean(str(raw), "")
        if text:
            return text
    return None


def _live_status_text(payload: PublicMatchPayload) -> str:
    time_label = (
        _optional_attr(payload, "match_time_label")
        or _optional_attr(payload, "live_time_label")
        or _optional_attr(payload, "kickoff_label")
    )
    score_label = _optional_attr(payload, "score_label")

    parts: list[str] = []
    if time_label:
        parts.append(time_label)
    if score_label:
        parts.append(score_label)

    return " • ".join(parts)


def _build_description(payload: PublicMatchPayload) -> str:
    competition = _clean(payload.competition_label, "Compétition")
    selection = _compact_selection(payload)
    live_status = _live_status_text(payload)

    lines = [f"🏆 {competition}"]
    if live_status:
        lines.append(f"⏱️ {live_status}")
    lines.append("")
    lines.append(f"🎯 **{selection}**")
    return "\n".join(lines).strip()


def _build_embed(publish_channel: str, payload: PublicMatchPayload) -> dict[str, Any]:
    match_label = _clean(payload.match_label, "Match")
    bookmaker = _clean(payload.bookmaker_label, "Book")
    odds = _clean(payload.odds_label, "-")
    confidence = _clean(payload.confidence_band, "-")

    thumbnail_url = _optional_url(
        payload,
        "competition_logo_url",
        "league_logo_url",
        "home_logo_url",
        "away_logo_url",
    )
    image_url = _optional_url(payload, "match_image_url", "banner_url")

    embed: dict[str, Any] = {
        "title": match_label,
        "description": _build_description(payload),
        "color": _embed_color(publish_channel, payload),
        "fields": [
            _field("Book", bookmaker, inline=True),
            _field("Cote", odds, inline=True),
            _field("Confiance", confidence, inline=True),
        ],
    }

    timestamp_value = _timestamp_value(payload)
    if timestamp_value:
        embed["timestamp"] = timestamp_value
    if thumbnail_url:
        embed["thumbnail"] = {"url": thumbnail_url}
    if image_url:
        embed["image"] = {"url": image_url}

    return embed


def format_payload_text(payload: PublicMatchPayload) -> str:
    competition = _clean(payload.competition_label, "Competition")
    match_label = _clean(payload.match_label, "Match")
    selection = _compact_selection(payload)
    bookmaker = _clean(payload.bookmaker_label, "Book")
    odds = _clean(payload.odds_label, "-")
    confidence = _clean(payload.confidence_band, "-")

    lines = [
        f"🏟️ {competition}",
        f"⚔️ {match_label}",
        f"🎯 {selection}",
        f"💸 {bookmaker} • {odds} • {confidence}",
    ]

    return "\n".join(lines)


def format_bundle(bundle: PublicMessageBundle) -> str:
    header = CHANNEL_HEADERS.get(_normalize_channel(bundle.publish_channel), f"📣 {bundle.publish_channel}")
    if not bundle.payloads:
        return header

    blocks = [header]
    for index, payload in enumerate(bundle.payloads):
        if index > 0:
            blocks.append("")
        blocks.append(format_payload_text(payload))

    return "\n".join(blocks).strip()


def prepare_discord_messages(bundles: tuple[PublicMessageBundle, ...]) -> tuple[str, ...]:
    return tuple(format_bundle(bundle) for bundle in bundles if bundle.payloads)


def build_bundle_webhook_payload(bundle: PublicMessageBundle) -> dict[str, object]:
    channel = _normalize_channel(bundle.publish_channel)
    header = CHANNEL_HEADERS.get(channel, f"📣 {bundle.publish_channel}")

    if not bundle.payloads:
        return {
            "username": "THE BOT V5",
            "content": header,
            "allowed_mentions": {"parse": []},
        }

    embeds = [_build_embed(channel, payload) for payload in bundle.payloads[:10]]
    return {
        "username": "THE BOT V5",
        "content": header,
        "embeds": embeds,
        "allowed_mentions": {"parse": []},
    }


def build_channel_webhook_payload(
    publish_channel: str,
    bundles: tuple[PublicMessageBundle, ...],
) -> dict[str, object]:
    channel = _normalize_channel(publish_channel)
    header = CHANNEL_HEADERS.get(channel, f"📣 {publish_channel}")

    payloads: list[PublicMatchPayload] = []
    for bundle in bundles:
        payloads.extend(bundle.payloads)

    embeds = [_build_embed(channel, payload) for payload in payloads[:10]]

    if not embeds:
        return {
            "username": "THE BOT V5",
            "content": header,
            "allowed_mentions": {"parse": []},
        }

    content = header
    if len(payloads) > 1:
        content = f"{header} • {len(payloads)} signaux"

    overflow = len(payloads) - len(embeds)
    if overflow > 0:
        content = f"{content} • +{overflow} non affiché(s)"

    return {
        "username": "THE BOT V5",
        "content": content,
        "embeds": embeds,
        "allowed_mentions": {"parse": []},
    }