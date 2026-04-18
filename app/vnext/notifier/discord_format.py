from __future__ import annotations

from app.vnext.publication.models import PublicMessageBundle


def format_bundle(bundle: PublicMessageBundle) -> str:
    header = f"[{bundle.publish_channel}]"
    lines = [header]
    for payload in bundle.payloads:
        lines.append(
            f"{payload.match_label} | {payload.market_label} {payload.line_label} | "
            f"{payload.bookmaker_label} {payload.odds_label} | {payload.confidence_band}"
        )
    return "\n".join(lines)
