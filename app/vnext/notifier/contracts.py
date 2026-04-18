from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Protocol, runtime_checkable

from app.vnext.publication.models import PublicMessageBundle


NotifierMode = Literal["none", "aggregate", "explicit_ack"]


@dataclass(slots=True, frozen=True)
class NotifierAckRecord:
    fixture_id: int
    public_status: str
    template_key: str | None
    bookmaker_id: int | None
    line: float | None
    odds_decimal: float | None


@dataclass(slots=True, frozen=True)
class NotifierSendResult:
    attempted_count: int
    notified_count: int
    acked_records: tuple[NotifierAckRecord, ...] = ()
    mode: NotifierMode = "explicit_ack"


@dataclass(slots=True, frozen=True)
class ResolvedNotifierSend:
    attempted_count: int
    notified_count: int
    acked_records: tuple[NotifierAckRecord, ...] = ()
    mode: NotifierMode = "none"


@runtime_checkable
class VnextNotifier(Protocol):
    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> int | NotifierSendResult:
        ...


LegacyNotifier = Callable[[tuple[PublicMessageBundle, ...]], int | NotifierSendResult]
NotifierInput = VnextNotifier | LegacyNotifier


@dataclass(slots=True, frozen=True)
class CallableVnextNotifier:
    sender: LegacyNotifier

    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> int | NotifierSendResult:
        return self.sender(bundles)


@dataclass(slots=True, frozen=True)
class NoopVnextNotifier:
    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> NotifierSendResult:
        return NotifierSendResult(
            attempted_count=0,
            notified_count=0,
            acked_records=(),
            mode="none",
        )


@dataclass(slots=True, frozen=True)
class AggregateCountVnextNotifier:
    notified_count: int | None = None

    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> int:
        return len(bundles) if self.notified_count is None else self.notified_count


@dataclass(slots=True, frozen=True)
class ExplicitAckVnextNotifier:
    acked_records: tuple[NotifierAckRecord, ...]
    attempted_count: int | None = None
    notified_count: int | None = None

    def send(self, bundles: tuple[PublicMessageBundle, ...]) -> NotifierSendResult:
        attempted_count = len(bundles) if self.attempted_count is None else self.attempted_count
        notified_count = len(self.acked_records) if self.notified_count is None else self.notified_count
        return NotifierSendResult(
            attempted_count=attempted_count,
            notified_count=notified_count,
            acked_records=self.acked_records,
            mode="explicit_ack",
        )


def adapt_vnext_notifier(notifier: NotifierInput | None) -> VnextNotifier | None:
    if notifier is None:
        return None
    if isinstance(notifier, VnextNotifier):
        return notifier
    if callable(notifier):
        return CallableVnextNotifier(notifier)
    raise TypeError("notifier_contract_invalid")


def resolve_notifier_send(
    notifier_result: int | NotifierSendResult,
    *,
    default_attempt_count: int,
) -> ResolvedNotifierSend:
    if isinstance(notifier_result, int):
        return ResolvedNotifierSend(
            attempted_count=default_attempt_count,
            notified_count=notifier_result,
            acked_records=(),
            mode="aggregate",
        )
    return ResolvedNotifierSend(
        attempted_count=notifier_result.attempted_count,
        notified_count=notifier_result.notified_count,
        acked_records=notifier_result.acked_records,
        mode=notifier_result.mode,
    )


def send_with_notifier(
    notifier: NotifierInput,
    bundles: tuple[PublicMessageBundle, ...],
) -> ResolvedNotifierSend:
    adapted = adapt_vnext_notifier(notifier)
    if adapted is None:
        raise TypeError("notifier_contract_invalid")
    return resolve_notifier_send(
        adapted.send(bundles),
        default_attempt_count=len(bundles),
    )
