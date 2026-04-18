from __future__ import annotations

from app.vnext.pipeline.models import PipelineSnapshot, PublishableMatchResult
from app.vnext.publication.formatter import build_public_payload
from app.vnext.publication.models import PublicMessageBundle


def _build_bundles(payloads: tuple) -> tuple[PublicMessageBundle, ...]:
    if not payloads:
        return ()

    bundles_by_channel: dict[str, list] = {}
    for payload in payloads:
        bundles_by_channel.setdefault(payload.publish_channel, []).append(payload)

    bundles = []
    for channel, channel_payloads in bundles_by_channel.items():
        bundles.append(
            PublicMessageBundle(
                publish_channel=channel,  # type: ignore[arg-type]
                payloads=tuple(channel_payloads),
            )
        )

    return tuple(bundles)


def build_publication_bundles(snapshot: PipelineSnapshot) -> tuple[PublicMessageBundle, ...]:
    payloads = []
    for result in snapshot.results:
        payload = build_public_payload(result)
        if payload is not None:
            payloads.append(payload)
    return _build_bundles(tuple(payloads))


def build_publication_bundles_from_results(
    results: tuple[PublishableMatchResult, ...],
) -> tuple[PublicMessageBundle, ...]:
    payloads = []
    for result in results:
        payload = build_public_payload(result)
        if payload is not None:
            payloads.append(payload)
    return _build_bundles(tuple(payloads))
