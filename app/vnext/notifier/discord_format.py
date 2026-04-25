from __future__ import annotations

from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle


_CHANNEL_HEADERS = {
    "ELITE": "🔥 ELITE LIVE",
    "WATCHLIST": "👀 WATCHLIST LIVE",
}


def _clean(text: str | None, default: str = "-") -> str:
    value = str(text or "").strip()
    return value if value else default


def _compact_selection(payload: PublicMatchPayload) -> str:
    raw = _clean(payload.line_label, "Sélection")
    key = raw.lower()

    # Team totals
    if "team total home under" in key:
        return "Total domicile - Under 1.5"
    if "team total away under" in key:
        return "Total extérieur - Under 1.5"
    if "team total home over" in key:
        return "Total domicile - Over 0.5"
    if "team total away over" in key:
        return "Total extérieur - Over 0.5"

    # OU full time
    if "under" in key and ("full time" in key or "ft" in key):
        return "Under 2.5 FT"
    if "over" in key and ("full time" in key or "ft" in key):
        return "Over 2.5 FT"

    # BTTS
    if "btts no" in key or "both teams to score no" in key:
        return "BTTS - Non"
    if "btts yes" in key or "both teams to score yes" in key:
        return "BTTS - Oui"

    # Result
    if "result home" in key:
        return "1X2 - Domicile"
    if "result away" in key:
        return "1X2 - Extérieur"

    return raw


def _format_payload(payload: PublicMatchPayload) -> str:
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
    header = _CHANNEL_HEADERS.get(bundle.publish_channel, f"📣 {bundle.publish_channel}")
    if not bundle.payloads:
        return header

    blocks = [header]
    for index, payload in enumerate(bundle.payloads):
        if index > 0:
            blocks.append("")
        blocks.append(_format_payload(payload))

    return "\n".join(blocks).strip()