from __future__ import annotations

from app.vnext.pipeline.models import PipelineSnapshot, PublishableMatchResult
from app.vnext.publication.formatter import build_public_payload
from app.vnext.publication.models import PublicMatchPayload, PublicMessageBundle


def _bundle_for_payload(payload: PublicMatchPayload) -> PublicMessageBundle:
    return PublicMessageBundle(
        publish_channel=payload.publish_channel,
        payloads=(payload,),
    )


def _build_bundles(payloads: tuple[PublicMatchPayload, ...]) -> tuple[PublicMessageBundle, ...]:
    if not payloads:
        return ()

    return tuple(_bundle_for_payload(payload) for payload in payloads)


def build_publication_bundles(snapshot: PipelineSnapshot) -> tuple[PublicMessageBundle, ...]:
    payloads: list[PublicMatchPayload] = []
    for result in snapshot.results:
        payload = build_public_payload(result)
        if payload is not None:
            payloads.append(payload)
    return _build_bundles(tuple(payloads))


def build_publication_bundles_from_results(
    results: tuple[PublishableMatchResult, ...],
) -> tuple[PublicMessageBundle, ...]:
    payloads: list[PublicMatchPayload] = []
    for result in results:
        payload = build_public_payload(result)
        if payload is not None:
            payloads.append(payload)
    return _build_bundles(tuple(payloads))