from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DiscordWebhookPayload = dict[str, Any]


@dataclass(slots=True, frozen=True)
class DiscordSendResult:
    ok: bool
    status_code: int
    error: str = ""
    response_body: str = ""


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _strip_nones(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            if item is None:
                continue
            cleaned_item = _strip_nones(item)
            if cleaned_item is None:
                continue
            cleaned[key] = cleaned_item
        return cleaned

    if isinstance(value, list):
        cleaned_list = []
        for item in value:
            cleaned_item = _strip_nones(item)
            if cleaned_item is None:
                continue
            cleaned_list.append(cleaned_item)
        return cleaned_list

    return value


def _coerce_payload(message: str | DiscordWebhookPayload) -> DiscordWebhookPayload:
    if isinstance(message, dict):
        payload = dict(message)
    else:
        payload = {"content": _normalize_text(message)}

    payload.setdefault("allowed_mentions", {"parse": []})

    content = payload.get("content")
    if content is not None:
        payload["content"] = _normalize_text(content)

    embeds = payload.get("embeds")
    if embeds is None:
        payload["embeds"] = []
    elif not isinstance(embeds, list):
        payload["embeds"] = [embeds]

    payload = _strip_nones(payload)

    has_content = bool(_normalize_text(payload.get("content")))
    has_embeds = bool(payload.get("embeds"))

    if not has_content and not has_embeds:
        payload["content"] = ""

    return payload


def _validate_payload(payload: DiscordWebhookPayload) -> str | None:
    has_content = bool(_normalize_text(payload.get("content")))
    has_embeds = bool(payload.get("embeds"))

    if not has_content and not has_embeds:
        return "empty_message"

    embeds = payload.get("embeds", [])
    if isinstance(embeds, list) and len(embeds) > 10:
        return "too_many_embeds"

    return None


def _send_with_requests(
    webhook_url: str,
    payload: DiscordWebhookPayload,
    *,
    timeout_seconds: float,
) -> DiscordSendResult:
    try:
        response = requests.post(  # type: ignore[union-attr]
            webhook_url,
            json=payload,
            headers={
                "User-Agent": "the_bot_v5_monster_v2/1.0",
            },
            timeout=timeout_seconds,
        )
        response_body = response.text or ""
        status_code = int(response.status_code)
        return DiscordSendResult(
            ok=status_code in (200, 204),
            status_code=status_code,
            error="" if status_code in (200, 204) else (response_body or response.reason or "http_error"),
            response_body=response_body,
        )
    except Exception as exc:  # pragma: no cover
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error=str(exc),
            response_body="",
        )


def _send_with_urllib(
    webhook_url: str,
    payload: DiscordWebhookPayload,
    *,
    timeout_seconds: float,
) -> DiscordSendResult:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request = Request(
        url=webhook_url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "the_bot_v5_monster_v2/1.0",
        },
    )

    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            response_body = response.read().decode("utf-8", errors="replace")
            status_code = int(getattr(response, "status", 200))
            return DiscordSendResult(
                ok=status_code in (200, 204),
                status_code=status_code,
                error="",
                response_body=response_body,
            )
    except HTTPError as exc:
        response_body = ""
        try:
            response_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            response_body = ""
        return DiscordSendResult(
            ok=False,
            status_code=int(exc.code),
            error=response_body or str(exc),
            response_body=response_body,
        )
    except URLError as exc:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error=str(exc.reason),
            response_body="",
        )
    except Exception as exc:  # pragma: no cover
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error=str(exc),
            response_body="",
        )


def send_discord_message(
    webhook_url: str,
    message: str | DiscordWebhookPayload,
    *,
    timeout_seconds: float = 10.0,
) -> DiscordSendResult:
    url = _normalize_text(webhook_url)
    if not url:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error="missing_webhook_url",
        )

    payload = _coerce_payload(message)
    validation_error = _validate_payload(payload)
    if validation_error is not None:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error=validation_error,
        )

    if requests is not None:
        return _send_with_requests(
            url,
            payload,
            timeout_seconds=timeout_seconds,
        )

    return _send_with_urllib(
        url,
        payload,
        timeout_seconds=timeout_seconds,
    )

    