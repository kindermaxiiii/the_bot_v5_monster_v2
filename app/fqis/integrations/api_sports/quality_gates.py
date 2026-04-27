from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class ApiSportsQualityStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCKED = "BLOCKED"


class ApiSportsQualitySeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    BLOCKER = "BLOCKER"


@dataclass(frozen=True)
class ApiSportsQualityGateConfig:
    min_fixtures: int = 1
    min_offers: int = 1
    min_mapped_offer_ratio: float = 0.10
    max_review_offer_ratio: float = 0.50
    max_rejected_offer_ratio: float = 0.20
    max_invalid_odds_ratio: float = 0.00
    max_missing_fixture_key_ratio: float = 0.00
    max_missing_market_key_ratio: float = 0.00
    max_duplicate_offer_key_ratio: float = 0.00

    @classmethod
    def from_env(cls) -> "ApiSportsQualityGateConfig":
        return cls(
            min_fixtures=_env_int("APISPORTS_QUALITY_MIN_FIXTURES", 1),
            min_offers=_env_int("APISPORTS_QUALITY_MIN_OFFERS", 1),
            min_mapped_offer_ratio=_env_float("APISPORTS_QUALITY_MIN_MAPPED_OFFER_RATIO", 0.10),
            max_review_offer_ratio=_env_float("APISPORTS_QUALITY_MAX_REVIEW_OFFER_RATIO", 0.50),
            max_rejected_offer_ratio=_env_float("APISPORTS_QUALITY_MAX_REJECTED_OFFER_RATIO", 0.20),
            max_invalid_odds_ratio=_env_float("APISPORTS_QUALITY_MAX_INVALID_ODDS_RATIO", 0.00),
            max_missing_fixture_key_ratio=_env_float("APISPORTS_QUALITY_MAX_MISSING_FIXTURE_KEY_RATIO", 0.00),
            max_missing_market_key_ratio=_env_float("APISPORTS_QUALITY_MAX_MISSING_MARKET_KEY_RATIO", 0.00),
            max_duplicate_offer_key_ratio=_env_float("APISPORTS_QUALITY_MAX_DUPLICATE_OFFER_KEY_RATIO", 0.00),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "min_fixtures": self.min_fixtures,
            "min_offers": self.min_offers,
            "min_mapped_offer_ratio": self.min_mapped_offer_ratio,
            "max_review_offer_ratio": self.max_review_offer_ratio,
            "max_rejected_offer_ratio": self.max_rejected_offer_ratio,
            "max_invalid_odds_ratio": self.max_invalid_odds_ratio,
            "max_missing_fixture_key_ratio": self.max_missing_fixture_key_ratio,
            "max_missing_market_key_ratio": self.max_missing_market_key_ratio,
            "max_duplicate_offer_key_ratio": self.max_duplicate_offer_key_ratio,
        }


@dataclass(frozen=True)
class ApiSportsQualityIssue:
    code: str
    severity: ApiSportsQualitySeverity
    message: str
    count: int = 0
    threshold: float | int | None = None
    observed: float | int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "message": self.message,
            "count": self.count,
            "threshold": self.threshold,
            "observed": self.observed,
        }


@dataclass(frozen=True)
class ApiSportsQualityReport:
    status: ApiSportsQualityStatus
    ready: bool
    source_path: str | None
    payload_sha256: str | None
    counts: Mapping[str, int | float]
    config: ApiSportsQualityGateConfig
    issues: tuple[ApiSportsQualityIssue, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "ready": self.ready,
            "source_path": self.source_path,
            "payload_sha256": self.payload_sha256,
            "counts": dict(self.counts),
            "config": self.config.to_dict(),
            "issues": [issue.to_dict() for issue in self.issues],
        }


class ApiSportsQualityGateError(RuntimeError):
    """Raised when a quality gate input cannot be evaluated."""


def evaluate_snapshot_quality_file(
    path: str | Path,
    *,
    config: ApiSportsQualityGateConfig | None = None,
) -> ApiSportsQualityReport:
    input_path = Path(path)
    if not input_path.exists():
        raise ApiSportsQualityGateError(f"Input path does not exist: {input_path}")

    raw_bytes = input_path.read_bytes()
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ApiSportsQualityGateError(f"Input path is not valid JSON: {input_path}") from exc

    if not isinstance(payload, Mapping):
        raise ApiSportsQualityGateError("Input payload must be a JSON object.")

    return evaluate_snapshot_quality(
        payload,
        config=config,
        source_path=input_path,
        payload_sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )


def evaluate_snapshot_quality(
    payload: Mapping[str, Any],
    *,
    config: ApiSportsQualityGateConfig | None = None,
    source_path: str | Path | None = None,
    payload_sha256: str | None = None,
) -> ApiSportsQualityReport:
    gate_config = config or ApiSportsQualityGateConfig.from_env()
    normalized = _unwrap_payload(payload)

    fixtures = _as_records(
        _first_sequence(
            normalized,
            (
                "fixtures",
                "fixture_records",
                "normalized_fixtures",
                "fqis_fixtures",
            ),
        )
    )
    offers = _as_records(
        _first_sequence(
            normalized,
            (
                "odds_offers",
                "offers",
                "normalized_odds_offers",
                "fqis_odds_offers",
                "odds",
            ),
        )
    )

    counts = _quality_counts(fixtures, offers)
    issues = _quality_issues(counts, gate_config)

    status = _status_from_issues(issues)
    return ApiSportsQualityReport(
        status=status,
        ready=status is not ApiSportsQualityStatus.BLOCKED,
        source_path=str(source_path) if source_path is not None else None,
        payload_sha256=payload_sha256,
        counts=counts,
        config=gate_config,
        issues=tuple(issues),
    )


def assert_snapshot_ready(
    payload: Mapping[str, Any],
    *,
    config: ApiSportsQualityGateConfig | None = None,
) -> ApiSportsQualityReport:
    report = evaluate_snapshot_quality(payload, config=config)
    if report.status is ApiSportsQualityStatus.BLOCKED:
        issue_codes = ", ".join(issue.code for issue in report.issues)
        raise ApiSportsQualityGateError(f"Snapshot blocked by quality gates: {issue_codes}")
    return report


def _quality_counts(
    fixtures: Sequence[Mapping[str, Any]],
    offers: Sequence[Mapping[str, Any]],
) -> dict[str, int | float]:
    offers_total = len(offers)

    mapped = 0
    review = 0
    rejected = 0
    ready = 0
    invalid_odds = 0
    missing_fixture_key = 0
    missing_market_key = 0
    missing_bookmaker = 0

    duplicate_keys = _duplicate_offer_key_count(offers)

    for offer in offers:
        mapping_status = _upper(offer.get("mapping_status"))
        normalization_status = _upper(offer.get("normalization_status"))

        if mapping_status == "MAPPED":
            mapped += 1
        if mapping_status in {"REVIEW", "UNKNOWN"} or normalization_status == "REVIEW":
            review += 1
        if mapping_status in {"IGNORED", "REJECTED"} or normalization_status == "REJECTED":
            rejected += 1

        if _invalid_decimal_odds(offer.get("decimal_odds")):
            invalid_odds += 1

        if _missing(offer.get("fixture_key")):
            missing_fixture_key += 1
        if _missing(offer.get("provider_market_key")):
            missing_market_key += 1
        if _missing(offer.get("provider_bookmaker_id")) and _missing(offer.get("bookmaker_name")):
            missing_bookmaker += 1

        if (
            mapping_status == "MAPPED"
            and normalization_status in {"OK", "READY", "PASS", "VALID"}
            and not _invalid_decimal_odds(offer.get("decimal_odds"))
            and not _missing(offer.get("fixture_key"))
            and not _missing(offer.get("provider_market_key"))
        ):
            ready += 1

    return {
        "fixtures_total": len(fixtures),
        "offers_total": offers_total,
        "offers_mapped": mapped,
        "offers_review": review,
        "offers_rejected": rejected,
        "offers_ready": ready,
        "offers_invalid_odds": invalid_odds,
        "offers_missing_fixture_key": missing_fixture_key,
        "offers_missing_market_key": missing_market_key,
        "offers_missing_bookmaker": missing_bookmaker,
        "duplicate_offer_keys": duplicate_keys,
        "mapped_offer_ratio": _ratio(mapped, offers_total),
        "review_offer_ratio": _ratio(review, offers_total),
        "rejected_offer_ratio": _ratio(rejected, offers_total),
        "invalid_odds_ratio": _ratio(invalid_odds, offers_total),
        "missing_fixture_key_ratio": _ratio(missing_fixture_key, offers_total),
        "missing_market_key_ratio": _ratio(missing_market_key, offers_total),
        "duplicate_offer_key_ratio": _ratio(duplicate_keys, offers_total),
    }


def _quality_issues(
    counts: Mapping[str, int | float],
    config: ApiSportsQualityGateConfig,
) -> list[ApiSportsQualityIssue]:
    issues: list[ApiSportsQualityIssue] = []

    fixtures_total = int(counts["fixtures_total"])
    offers_total = int(counts["offers_total"])

    if fixtures_total < config.min_fixtures:
        issues.append(
            ApiSportsQualityIssue(
                code="MIN_FIXTURES_NOT_MET",
                severity=ApiSportsQualitySeverity.BLOCKER,
                message="Snapshot has fewer fixtures than required.",
                count=fixtures_total,
                threshold=config.min_fixtures,
                observed=fixtures_total,
            )
        )

    if offers_total < config.min_offers:
        issues.append(
            ApiSportsQualityIssue(
                code="MIN_OFFERS_NOT_MET",
                severity=ApiSportsQualitySeverity.BLOCKER,
                message="Snapshot has fewer odds offers than required.",
                count=offers_total,
                threshold=config.min_offers,
                observed=offers_total,
            )
        )

    _append_ratio_issue(
        issues,
        code="MAPPED_OFFER_RATIO_TOO_LOW",
        severity=ApiSportsQualitySeverity.BLOCKER,
        message="Mapped offer ratio is below readiness threshold.",
        observed=float(counts["mapped_offer_ratio"]),
        threshold=config.min_mapped_offer_ratio,
        count=int(counts["offers_mapped"]),
        comparator="lt",
    )
    _append_ratio_issue(
        issues,
        code="REJECTED_OFFER_RATIO_TOO_HIGH",
        severity=ApiSportsQualitySeverity.BLOCKER,
        message="Rejected offer ratio is above readiness threshold.",
        observed=float(counts["rejected_offer_ratio"]),
        threshold=config.max_rejected_offer_ratio,
        count=int(counts["offers_rejected"]),
        comparator="gt",
    )
    _append_ratio_issue(
        issues,
        code="INVALID_ODDS_RATIO_TOO_HIGH",
        severity=ApiSportsQualitySeverity.BLOCKER,
        message="Invalid decimal odds ratio is above readiness threshold.",
        observed=float(counts["invalid_odds_ratio"]),
        threshold=config.max_invalid_odds_ratio,
        count=int(counts["offers_invalid_odds"]),
        comparator="gt",
    )
    _append_ratio_issue(
        issues,
        code="MISSING_FIXTURE_KEY_RATIO_TOO_HIGH",
        severity=ApiSportsQualitySeverity.BLOCKER,
        message="Missing fixture key ratio is above readiness threshold.",
        observed=float(counts["missing_fixture_key_ratio"]),
        threshold=config.max_missing_fixture_key_ratio,
        count=int(counts["offers_missing_fixture_key"]),
        comparator="gt",
    )
    _append_ratio_issue(
        issues,
        code="MISSING_MARKET_KEY_RATIO_TOO_HIGH",
        severity=ApiSportsQualitySeverity.BLOCKER,
        message="Missing provider market key ratio is above readiness threshold.",
        observed=float(counts["missing_market_key_ratio"]),
        threshold=config.max_missing_market_key_ratio,
        count=int(counts["offers_missing_market_key"]),
        comparator="gt",
    )
    _append_ratio_issue(
        issues,
        code="DUPLICATE_OFFER_KEY_RATIO_TOO_HIGH",
        severity=ApiSportsQualitySeverity.BLOCKER,
        message="Duplicate offer key ratio is above readiness threshold.",
        observed=float(counts["duplicate_offer_key_ratio"]),
        threshold=config.max_duplicate_offer_key_ratio,
        count=int(counts["duplicate_offer_keys"]),
        comparator="gt",
    )
    _append_ratio_issue(
        issues,
        code="REVIEW_OFFER_RATIO_HIGH",
        severity=ApiSportsQualitySeverity.WARNING,
        message="Review offer ratio is above observation threshold.",
        observed=float(counts["review_offer_ratio"]),
        threshold=config.max_review_offer_ratio,
        count=int(counts["offers_review"]),
        comparator="gt",
    )

    return issues


def _append_ratio_issue(
    issues: list[ApiSportsQualityIssue],
    *,
    code: str,
    severity: ApiSportsQualitySeverity,
    message: str,
    observed: float,
    threshold: float,
    count: int,
    comparator: str,
) -> None:
    if comparator == "gt" and observed <= threshold:
        return
    if comparator == "lt" and observed >= threshold:
        return

    issues.append(
        ApiSportsQualityIssue(
            code=code,
            severity=severity,
            message=message,
            count=count,
            threshold=threshold,
            observed=round(observed, 6),
        )
    )


def _status_from_issues(issues: Sequence[ApiSportsQualityIssue]) -> ApiSportsQualityStatus:
    if any(issue.severity is ApiSportsQualitySeverity.BLOCKER for issue in issues):
        return ApiSportsQualityStatus.BLOCKED
    if any(issue.severity is ApiSportsQualitySeverity.WARNING for issue in issues):
        return ApiSportsQualityStatus.WARN
    return ApiSportsQualityStatus.PASS


def _unwrap_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("normalized", "batch", "payload"):
        nested = payload.get(key)
        if isinstance(nested, Mapping):
            return nested
    return payload


def _first_sequence(payload: Mapping[str, Any], keys: Sequence[str]) -> Sequence[Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return value
    return []


def _as_records(items: Sequence[Any]) -> list[Mapping[str, Any]]:
    return [item for item in items if isinstance(item, Mapping)]


def _duplicate_offer_key_count(offers: Sequence[Mapping[str, Any]]) -> int:
    keys: list[tuple[object, ...]] = []
    for offer in offers:
        key = (
            offer.get("fixture_key"),
            offer.get("provider_bookmaker_id") or offer.get("bookmaker_name"),
            offer.get("provider_market_key") or offer.get("provider_market_id"),
            offer.get("selection"),
            offer.get("line"),
        )
        if all(not _missing(part) for part in key[:4]):
            keys.append(key)

    counts = Counter(keys)
    return sum(count - 1 for count in counts.values() if count > 1)


def _invalid_decimal_odds(value: Any) -> bool:
    try:
        odds = float(value)
    except (TypeError, ValueError):
        return True
    return odds <= 1.0 or odds > 1000.0


def _missing(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _upper(value: Any) -> str:
    if value is None:
        return ""
    enum_value = getattr(value, "value", None)
    if enum_value is not None:
        value = enum_value
    return str(value).strip().upper()


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsQualityGateError(f"{name} must be an integer.") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ApiSportsQualityGateError(f"{name} must be a float.") from exc