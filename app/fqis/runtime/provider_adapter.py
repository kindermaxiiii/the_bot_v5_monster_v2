from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.fqis.runtime.match_snapshot import (
    FqisMatchSnapshotBuildResult,
    write_match_snapshot_jsonl,
)
from app.fqis.runtime.semilive_snapshot import build_semilive_match_snapshot_records


_REQUIRED_LIVE_MATCH_FIELDS = (
    "home_xg_live",
    "away_xg_live",
    "home_shots_on_target",
    "away_shots_on_target",
    "minute",
    "home_score",
    "away_score",
)


@dataclass(slots=True, frozen=True)
class ProviderSnapshotBuildResult:
    output_path: Path
    record_count: int
    total_offer_count: int
    event_ids: tuple[int, ...]


def load_provider_source_rows(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        raise FileNotFoundError(f"provider source file not found: {path}")

    rows: list[dict[str, Any]] = []

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        if not isinstance(row, dict):
            raise ValueError(f"line {line_number}: provider row must be a JSON object")

        rows.append(row)

    if not rows:
        raise ValueError(f"provider source file is empty: {path}")

    return tuple(rows)


def build_semilive_rows_from_provider_rows(
    rows: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    grouped: dict[int, dict[str, Any]] = {}

    for line_number, row in enumerate(rows, start=1):
        row_type = str(row.get("row_type") or row.get("type") or "").lower().strip()

        if row_type not in {"match", "offer", "probability"}:
            raise ValueError(f"line {line_number}: unsupported provider row_type: {row_type}")

        event_id = _resolve_event_id(row, line_number=line_number)
        bucket = grouped.setdefault(
            event_id,
            {
                "source": "provider_adapter",
                "event_id": event_id,
                "match_label": row.get("match_label"),
                "offers": [],
                "p_real_by_thesis": {},
            },
        )

        if row.get("match_label") and not bucket.get("match_label"):
            bucket["match_label"] = row["match_label"]

        if row_type == "match":
            bucket["live_match"] = _normalize_match_row(row, event_id=event_id, line_number=line_number)

        elif row_type == "offer":
            bucket["offers"].append(_normalize_offer_row(row, event_id=event_id, line_number=line_number))

        elif row_type == "probability":
            _merge_probability_row(bucket["p_real_by_thesis"], row, line_number=line_number)

    semilive_rows: list[dict[str, Any]] = []

    for event_id in sorted(grouped):
        bucket = grouped[event_id]

        if "live_match" not in bucket:
            raise ValueError(f"event_id {event_id}: missing match row")

        if not bucket["offers"]:
            raise ValueError(f"event_id {event_id}: missing offers")

        if not bucket["p_real_by_thesis"]:
            raise ValueError(f"event_id {event_id}: missing probabilities")

        semilive_rows.append(
    {
        "source": bucket["source"],
        "event_id": event_id,
        "match_label": bucket.get("match_label"),
        "live_match": bucket["live_match"],
        "offers": list(bucket["offers"]),
        "p_real_by_thesis": bucket["p_real_by_thesis"],
    }
)

    return tuple(semilive_rows)


def build_provider_snapshot_from_jsonl(
    source_path: Path,
    output_path: Path,
) -> ProviderSnapshotBuildResult:
    provider_rows = load_provider_source_rows(source_path)
    semilive_rows = build_semilive_rows_from_provider_rows(provider_rows)
    records = build_semilive_match_snapshot_records(semilive_rows)
    result = write_match_snapshot_jsonl(records, output_path)

    return _to_provider_result(result)


def _normalize_match_row(
    row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> dict[str, Any]:
    raw = row.get("live_match") or row.get("features") or row

    if not isinstance(raw, dict):
        raise ValueError(f"line {line_number}: match payload must be a JSON object")

    for field in _REQUIRED_LIVE_MATCH_FIELDS:
        if field not in raw:
            raise ValueError(f"line {line_number}: missing match field: {field}")

    return {
        "event_id": event_id,
        "home_xg_live": float(raw["home_xg_live"]),
        "away_xg_live": float(raw["away_xg_live"]),
        "home_shots_on_target": int(raw["home_shots_on_target"]),
        "away_shots_on_target": int(raw["away_shots_on_target"]),
        "minute": int(raw["minute"]),
        "home_score": int(raw["home_score"]),
        "away_score": int(raw["away_score"]),
    }


def _normalize_offer_row(
    row: dict[str, Any],
    *,
    event_id: int,
    line_number: int,
) -> dict[str, Any]:
    raw = row.get("offer") if isinstance(row.get("offer"), dict) else row

    bookmaker_name = _first_present_str(raw, ("bookmaker_name", "bookmaker", "bookmaker_label"), default="UNKNOWN")
    family = _first_present_str(raw, ("family", "market_family"), reason=f"line {line_number}: missing offer family")
    side = _first_present_str(raw, ("side", "selection_side"), reason=f"line {line_number}: missing offer side")
    period = _first_present_str(raw, ("period",), default="FT")
    team_role = _first_present_str(raw, ("team_role",), default="NONE")
    odds_decimal = _first_present_float(
        raw,
        ("odds_decimal", "odds", "price"),
        reason=f"line {line_number}: missing offer odds",
    )

    return {
        "event_id": int(raw.get("event_id", event_id)),
        "bookmaker_id": raw.get("bookmaker_id"),
        "bookmaker_name": bookmaker_name,
        "family": family,
        "side": side,
        "period": period,
        "team_role": team_role,
        "line": raw.get("line"),
        "odds_decimal": odds_decimal,
        "source_timestamp_utc": raw.get("source_timestamp_utc"),
        "freshness_seconds": raw.get("freshness_seconds"),
    }


def _merge_probability_row(
    target: dict[str, dict[str, float]],
    row: dict[str, Any],
    *,
    line_number: int,
) -> None:
    nested = row.get("p_real_by_thesis") or row.get("probabilities_by_thesis")

    if isinstance(nested, dict):
        for thesis_key, intent_probs in nested.items():
            if not isinstance(intent_probs, dict):
                raise ValueError(f"line {line_number}: probabilities for {thesis_key} must be a JSON object")

            thesis_bucket = target.setdefault(str(thesis_key), {})
            for intent_key, probability in intent_probs.items():
                thesis_bucket[str(intent_key)] = float(probability)

        return

    thesis_key = row.get("thesis_key")
    intent_key = row.get("intent_key")
    probability = row.get("p_real", row.get("probability"))

    if thesis_key in (None, ""):
        raise ValueError(f"line {line_number}: missing thesis_key")

    if intent_key in (None, ""):
        raise ValueError(f"line {line_number}: missing intent_key")

    if probability in (None, ""):
        raise ValueError(f"line {line_number}: missing p_real")

    target.setdefault(str(thesis_key), {})[str(intent_key)] = float(probability)


def _resolve_event_id(row: dict[str, Any], *, line_number: int) -> int:
    for key in ("event_id", "fixture_id", "match_id"):
        if row.get(key) not in (None, ""):
            return int(row[key])

    for nested_key in ("live_match", "features", "offer"):
        nested = row.get(nested_key)
        if isinstance(nested, dict):
            for key in ("event_id", "fixture_id", "match_id"):
                if nested.get(key) not in (None, ""):
                    return int(nested[key])

    raise ValueError(f"line {line_number}: missing event_id")


def _first_present_str(
    row: dict[str, Any],
    keys: tuple[str, ...],
    *,
    reason: str | None = None,
    default: str | None = None,
) -> str:
    for key in keys:
        if row.get(key) not in (None, ""):
            return str(row[key])

    if default is not None:
        return default

    raise ValueError(reason or f"missing {keys[0]}")


def _first_present_float(
    row: dict[str, Any],
    keys: tuple[str, ...],
    *,
    reason: str,
) -> float:
    for key in keys:
        if row.get(key) not in (None, ""):
            return float(row[key])

    raise ValueError(reason)


def _to_provider_result(result: FqisMatchSnapshotBuildResult) -> ProviderSnapshotBuildResult:
    return ProviderSnapshotBuildResult(
        output_path=result.output_path,
        record_count=result.record_count,
        total_offer_count=result.total_offer_count,
        event_ids=result.event_ids,
    )
    