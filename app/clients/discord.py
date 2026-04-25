from __future__ import annotations

import json
from dataclasses import dataclass

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(slots=True, frozen=True)
class DiscordSendResult:
    ok: bool
    status_code: int
    error: str = ""
    response_body: str = ""


def _normalize_text(value: str | None) -> str:
    return str(value or "").strip()


def _build_payload(content: str) -> dict[str, object]:
    return {
        "content": content,
        "allowed_mentions": {"parse": []},
    }


def _send_with_requests(
    webhook_url: str,
    content: str,
    *,
    timeout_seconds: float,
) -> DiscordSendResult:
    payload = _build_payload(content)
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
    except Exception as exc:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error=str(exc),
            response_body="",
        )


def _send_with_urllib(
    webhook_url: str,
    content: str,
    *,
    timeout_seconds: float,
) -> DiscordSendResult:
    payload = _build_payload(content)
    body = json.dumps(payload).encode("utf-8")

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
    except Exception as exc:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error=str(exc),
            response_body="",
        )


def send_discord_message(
    webhook_url: str,
    content: str,
    *,
    timeout_seconds: float = 10.0,
) -> DiscordSendResult:
    url = _normalize_text(webhook_url)
    message = _normalize_text(content)

    if not url:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error="missing_webhook_url",
        )

    if not message:
        return DiscordSendResult(
            ok=False,
            status_code=0,
            error="empty_message",
        )

    if requests is not None:
        return _send_with_requests(
            url,
            message,
            timeout_seconds=timeout_seconds,
        )

    return _send_with_urllib(
        url,
        message,
        timeout_seconds=timeout_seconds,
    )