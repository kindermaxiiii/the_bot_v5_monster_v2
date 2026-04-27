from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_REPLAY_AUDIT_DIR = Path("data/audit/api_sports_replay")

_VALID_STATUS = "VALID"
_REVIEW_STATUS = "REVIEW"
_REJECTED_STATUS = "REJECTED"


class ApiSportsReplayError(RuntimeError):
    """Raised when a normalized snapshot cannot be replayed safely."""


@dataclass(frozen=True)
class ApiSportsReplayCounts:
    fixtures_total: int
    offers_total: int
    offers_valid: int
    offers_review: int
    offers_rejected: int
    offers_unknown: int
    markets_total: int
    bookmakers_total: int
    fixtures_with_offers: int
    raw_bytes: int


@dataclass(frozen=True)
class ApiSportsReplayManifest:
    status: str
    mode: str
    provider: str
    replay_id: str
    input_path: str
    output_path: str | None
    input_sha256: str
    generated_at_utc: str
    source: str | None
    snapshot_id: str | None
    run_id: str | None
    counts: ApiSportsReplayCounts
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["counts"] = asdict(self.counts)
        data["warnings"] = list(self.warnings)
        return data


def replay_normalized_snapshot(
    input_path: str | Path,
    *,
    output_dir: str | Path = DEFAULT_REPLAY_AUDIT_DIR,
    write_manifest: bool = True,
) -> ApiSportsReplayManifest:
    path = Path(input_path)
    if not path.exists():
        raise ApiSportsReplayError(f"Input path does not exist: {path}")
    if not path.is_file():
        raise ApiSportsReplayError(f"Input path is not a file: {path}")

    raw_bytes = path.read_bytes()
    payload = _load_json_bytes(raw_bytes, path=path)
    if not isinstance(payload, Mapping):
        raise ApiSportsReplayError(f"Normalized snapshot must be a JSON object: {path}")

    fixtures = _extract_records(
        payload,
        keys=(
            "fixtures",
            "normalized_fixtures",
            "fixture_records",
            "fqis_fixtures",
        ),
    )
    offers = _extract_records(
        payload,
        keys=(
            "odds_offers",
            "offers",
            "normalized_odds_offers",
            "offer_records",
            "fqis_odds_offers",
        ),
    )

    metadata = _extract_metadata(payload)
    provider = _optional_str(metadata.get("provider")) or _infer_provider(fixtures, offers)
    source = _optional_str(metadata.get("source")) or _infer_source(payload, offers)
    snapshot_id = _optional_str(metadata.get("snapshot_id") or metadata.get("id"))
    run_id = _optional_str(metadata.get("run_id"))

    digest = hashlib.sha256(raw_bytes).hexdigest()
    replay_id = f"api_sports_replay_{digest[:16]}"

    warnings = _build_warnings(fixtures=fixtures, offers=offers)
    counts = _build_counts(fixtures=fixtures, offers=offers, raw_bytes=len(raw_bytes))

    output_path: Path | None = None
    if write_manifest:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        output_path = output_root / f"{replay_id}.json"

    manifest = ApiSportsReplayManifest(
        status="COMPLETED",
        mode="shadow_only_snapshot_replay",
        provider=provider or "api_sports_api_football",
        replay_id=replay_id,
        input_path=str(path),
        output_path=str(output_path) if output_path is not None else None,
        input_sha256=digest,
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        source=source,
        snapshot_id=snapshot_id,
        run_id=run_id,
        counts=counts,
        warnings=tuple(warnings),
    )

    if output_path is not None:
        output_path.write_text(
            json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return manifest


def _load_json_bytes(raw_bytes: bytes, *, path: Path) -> Any:
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ApiSportsReplayError(f"Normalized snapshot is not valid UTF-8: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ApiSportsReplayError(f"Normalized snapshot is not valid JSON: {path}: {exc}") from exc


def _extract_metadata(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("metadata", "manifest", "snapshot_manifest", "audit"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            return value
    return payload


def _extract_records(payload: Mapping[str, Any], *, keys: Sequence[str]) -> list[Mapping[str, Any]]:
    for key in keys:
        value = payload.get(key)
        records = _coerce_record_list(value)
        if records:
            return records

    records_value = payload.get("records")
    if isinstance(records_value, Sequence) and not isinstance(records_value, (str, bytes)):
        records: list[Mapping[str, Any]] = []
        for item in records_value:
            if not isinstance(item, Mapping):
                continue
            kind = str(item.get("kind") or item.get("type") or "").lower()
            key_hint = " ".join(keys).lower()
            if "fixture" in key_hint and "fixture" in kind:
                records.append(item)
            elif ("offer" in key_hint or "odds" in key_hint) and (
                "offer" in kind or "odds" in kind
            ):
                records.append(item)
        if records:
            return records

    return []


def _coerce_record_list(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [item for item in value if isinstance(item, Mapping)]

    if isinstance(value, Mapping):
        nested = value.get("records") or value.get("items") or value.get("response")
        if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes)):
            return [item for item in nested if isinstance(item, Mapping)]

    return []


def _build_counts(
    *,
    fixtures: Sequence[Mapping[str, Any]],
    offers: Sequence[Mapping[str, Any]],
    raw_bytes: int,
) -> ApiSportsReplayCounts:
    status_counts = Counter(_normalized_offer_status(offer) for offer in offers)

    market_keys = {
        value
        for value in (_optional_str(offer.get("provider_market_key")) for offer in offers)
        if value
    }
    bookmaker_keys = {
        value
        for value in (
            _optional_str(offer.get("provider_bookmaker_id") or offer.get("bookmaker_name"))
            for offer in offers
        )
        if value
    }
    fixture_offer_keys = {
        value
        for value in (
            _optional_str(
                offer.get("fixture_key")
                or offer.get("provider_fixture_id")
                or offer.get("fixture_id")
            )
            for offer in offers
        )
        if value
    }

    return ApiSportsReplayCounts(
        fixtures_total=len(fixtures),
        offers_total=len(offers),
        offers_valid=status_counts[_VALID_STATUS],
        offers_review=status_counts[_REVIEW_STATUS],
        offers_rejected=status_counts[_REJECTED_STATUS],
        offers_unknown=sum(
            count
            for status, count in status_counts.items()
            if status not in {_VALID_STATUS, _REVIEW_STATUS, _REJECTED_STATUS}
        ),
        markets_total=len(market_keys),
        bookmakers_total=len(bookmaker_keys),
        fixtures_with_offers=len(fixture_offer_keys),
        raw_bytes=raw_bytes,
    )


def _build_warnings(
    *,
    fixtures: Sequence[Mapping[str, Any]],
    offers: Sequence[Mapping[str, Any]],
) -> list[str]:
    warnings: list[str] = []

    if not fixtures:
        warnings.append("no_fixtures_in_normalized_snapshot")
    if not offers:
        warnings.append("no_odds_offers_in_normalized_snapshot")

    for offer in offers:
        offer_warnings = offer.get("warnings")
        if isinstance(offer_warnings, Sequence) and not isinstance(offer_warnings, (str, bytes)):
            warnings.extend(str(item) for item in offer_warnings if item)

    return sorted(set(warnings))


def _normalized_offer_status(offer: Mapping[str, Any]) -> str:
    raw = (
        offer.get("normalization_status")
        or offer.get("status")
        or offer.get("mapping_status")
        or ""
    )
    return str(raw).strip().upper() or "UNKNOWN"


def _infer_provider(
    fixtures: Iterable[Mapping[str, Any]],
    offers: Iterable[Mapping[str, Any]],
) -> str | None:
    for item in [*fixtures, *offers]:
        value = _optional_str(item.get("provider"))
        if value:
            return value
    return None


def _infer_source(payload: Mapping[str, Any], offers: Sequence[Mapping[str, Any]]) -> str | None:
    direct = _optional_str(payload.get("source"))
    if direct:
        return direct

    for offer in offers:
        value = _optional_str(offer.get("source"))
        if value:
            return value
    return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
