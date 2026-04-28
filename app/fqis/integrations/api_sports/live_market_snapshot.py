
from __future__ import annotations

import json
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LIVE_MARKET_SNAPSHOT_MODE = "LIVE_MARKET_SNAPSHOT"
OBSERVATION_ONLY_STATUS = "OBSERVATION_ONLY"


@dataclass(frozen=True)
class ApiSportsLiveMarketSnapshotConfig:
    max_rows: int = 100
    min_bookmakers: int = 1

    @classmethod
    def from_env(cls) -> "ApiSportsLiveMarketSnapshotConfig":
        return cls(
            max_rows=_env_int("APISPORTS_LIVE_MARKET_SNAPSHOT_MAX_ROWS", 100),
            min_bookmakers=_env_int("APISPORTS_LIVE_MARKET_SNAPSHOT_MIN_BOOKMAKERS", 1),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "max_rows": self.max_rows,
            "min_bookmakers": self.min_bookmakers,
        }


@dataclass(frozen=True)
class ApiSportsLiveMarketOffer:
    match: str
    market: str
    selection: str
    bookmaker: str
    odds: float
    kickoff_utc: str | None = None
    fixture_id: int | None = None
    source: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ApiSportsLiveMarketOffer":
        match = _required_text(payload.get("match"), "match")
        market = _required_text(payload.get("market"), "market")
        selection = _required_text(payload.get("selection"), "selection")
        bookmaker = _required_text(payload.get("bookmaker"), "bookmaker")
        odds = _required_float(payload.get("odds"), "odds")

        if odds <= 1.0:
            raise ApiSportsLiveMarketSnapshotError("Offer odds must be greater than 1.0.")

        return cls(
            match=match,
            market=market,
            selection=selection,
            bookmaker=bookmaker,
            odds=odds,
            kickoff_utc=_optional_text(payload.get("kickoff_utc")),
            fixture_id=_optional_int(payload.get("fixture_id")),
            source=_optional_text(payload.get("source")),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "match": self.match,
            "market": self.market,
            "selection": self.selection,
            "bookmaker": self.bookmaker,
            "odds": self.odds,
            "kickoff_utc": self.kickoff_utc,
            "fixture_id": self.fixture_id,
            "source": self.source,
        }


@dataclass(frozen=True)
class ApiSportsLiveMarketSnapshotRow:
    match: str
    market: str
    selection: str
    kickoff_utc: str | None
    fixture_id: int | None
    best_bookmaker: str
    best_odds: float
    average_odds: float
    min_odds: float
    max_odds: float
    spread_pct: float
    implied_probability_best: float
    implied_probability_average: float
    bookmakers_count: int
    offers_count: int
    bookmaker_list: tuple[str, ...]
    status: str = OBSERVATION_ONLY_STATUS

    def to_dict(self) -> dict[str, object]:
        return {
            "match": self.match,
            "market": self.market,
            "selection": self.selection,
            "kickoff_utc": self.kickoff_utc,
            "fixture_id": self.fixture_id,
            "best_bookmaker": self.best_bookmaker,
            "best_odds": self.best_odds,
            "average_odds": self.average_odds,
            "min_odds": self.min_odds,
            "max_odds": self.max_odds,
            "spread_pct": self.spread_pct,
            "implied_probability_best": self.implied_probability_best,
            "implied_probability_average": self.implied_probability_average,
            "bookmakers_count": self.bookmakers_count,
            "offers_count": self.offers_count,
            "bookmaker_list": list(self.bookmaker_list),
            "status": self.status,
        }


@dataclass(frozen=True)
class ApiSportsRejectedLiveMarketOffer:
    raw: Mapping[str, Any]
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "raw": dict(self.raw),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ApiSportsLiveMarketSnapshot:
    status: str
    mode: str
    real_staking_enabled: bool
    generated_at_utc: str
    source_path: str | None
    config: ApiSportsLiveMarketSnapshotConfig
    rows: tuple[ApiSportsLiveMarketSnapshotRow, ...]
    rejected: tuple[ApiSportsRejectedLiveMarketOffer, ...]
    warnings: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.status == "READY" and not self.errors

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "ready": self.ready,
            "mode": self.mode,
            "real_staking_enabled": self.real_staking_enabled,
            "generated_at_utc": self.generated_at_utc,
            "source_path": self.source_path,
            "config": self.config.to_dict(),
            "summary": {
                "rows_total": len(self.rows),
                "rejected_total": len(self.rejected),
                "errors_total": len(self.errors),
                "warnings_total": len(self.warnings),
            },
            "rows": [row.to_dict() for row in self.rows],
            "rejected": [item.to_dict() for item in self.rejected],
            "warnings": list(self.warnings),
            "errors": list(self.errors),
        }


class ApiSportsLiveMarketSnapshotError(RuntimeError):
    pass


def build_api_sports_live_market_snapshot(
    *,
    source_path: str | Path | None = None,
    candidates: Sequence[Mapping[str, Any]] | None = None,
    config: ApiSportsLiveMarketSnapshotConfig | None = None,
) -> ApiSportsLiveMarketSnapshot:
    snapshot_config = config or ApiSportsLiveMarketSnapshotConfig.from_env()
    raw_items: list[Mapping[str, Any]] = []

    if source_path is not None:
        raw_items.extend(_load_candidate_source(Path(source_path)))

    if candidates is not None:
        raw_items.extend(item for item in candidates if isinstance(item, Mapping))

    offers: list[ApiSportsLiveMarketOffer] = []
    rejected: list[ApiSportsRejectedLiveMarketOffer] = []

    for raw in raw_items:
        try:
            offers.append(ApiSportsLiveMarketOffer.from_mapping(raw))
        except ApiSportsLiveMarketSnapshotError as exc:
            rejected.append(ApiSportsRejectedLiveMarketOffer(raw=raw, reason=str(exc)))

    rows = _aggregate_offers(offers, snapshot_config)

    return ApiSportsLiveMarketSnapshot(
        status="READY",
        mode=LIVE_MARKET_SNAPSHOT_MODE,
        real_staking_enabled=False,
        generated_at_utc=_utc_now(),
        source_path=str(source_path) if source_path is not None else None,
        config=snapshot_config,
        rows=tuple(rows),
        rejected=tuple(rejected),
        warnings=("OBSERVATION_ONLY", "NO_REAL_STAKING", "NO_MODEL_EDGE_VALIDATION"),
        errors=(),
    )


def write_api_sports_live_market_snapshot(
    *,
    source_path: str | Path,
    output_path: str | Path,
    markdown_path: str | Path | None = None,
    config: ApiSportsLiveMarketSnapshotConfig | None = None,
) -> ApiSportsLiveMarketSnapshot:
    result = build_api_sports_live_market_snapshot(
        source_path=source_path,
        config=config,
    )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)

    if markdown_path is not None:
        md_target = Path(markdown_path)
        md_target.parent.mkdir(parents=True, exist_ok=True)
        md_tmp = md_target.with_suffix(md_target.suffix + ".tmp")
        md_tmp.write_text(render_api_sports_live_market_snapshot_markdown(result), encoding="utf-8")
        md_tmp.replace(md_target)

    return result


def render_api_sports_live_market_snapshot_markdown(result: ApiSportsLiveMarketSnapshot) -> str:
    lines: list[str] = [
        "# FQIS API-Sports Live Market Snapshot",
        "",
        "## Summary",
        "",
        f"- Status: **{result.status}**",
        f"- Mode: **{result.mode}**",
        f"- Real staking enabled: **{str(result.real_staking_enabled).lower()}**",
        f"- Rows: **{len(result.rows)}**",
        f"- Rejected offers: **{len(result.rejected)}**",
        f"- Generated at UTC: `{result.generated_at_utc}`",
        "",
        "> OBSERVATION ONLY. This is a live market view, not a betting signal.",
        "",
        "## Best Odds View",
        "",
    ]

    if not result.rows:
        lines.extend(["No market rows.", ""])
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "| # | Match | Market | Selection | Best Odds | Best Bookmaker | Avg Odds | Spread | Books | Best Implied | Status |",
            "|---:|---|---|---|---:|---|---:|---:|---:|---:|---|",
        ]
    )

    for idx, row in enumerate(result.rows, start=1):
        lines.append(
            "| {idx} | {match} | {market} | {selection} | {best_odds:.2f} | {best_bookmaker} | "
            "{average_odds:.2f} | {spread_pct:+.2f}% | {bookmakers_count} | {implied:.2%} | {status} |".format(
                idx=idx,
                match=_md(row.match),
                market=_md(row.market),
                selection=_md(row.selection),
                best_odds=row.best_odds,
                best_bookmaker=_md(row.best_bookmaker),
                average_odds=row.average_odds,
                spread_pct=row.spread_pct,
                bookmakers_count=row.bookmakers_count,
                implied=row.implied_probability_best,
                status=row.status,
            )
        )

    lines.extend(
        [
            "",
            "## Operator Notes",
            "",
            "- Compare best odds against model prices only after independent model probabilities are available.",
            "- Do not place real stakes from this snapshot.",
            "- Use bookmaker count and spread to detect thin or unstable markets.",
            "",
        ]
    )

    return "\n".join(lines)


def _aggregate_offers(
    offers: Sequence[ApiSportsLiveMarketOffer],
    config: ApiSportsLiveMarketSnapshotConfig,
) -> list[ApiSportsLiveMarketSnapshotRow]:
    grouped: dict[tuple[int | None, str, str, str], list[ApiSportsLiveMarketOffer]] = defaultdict(list)

    for offer in offers:
        key = (offer.fixture_id, offer.match, offer.market, offer.selection)
        grouped[key].append(offer)

    rows: list[ApiSportsLiveMarketSnapshotRow] = []

    for (_, match, market, selection), group in grouped.items():
        bookmakers = tuple(sorted({item.bookmaker for item in group}))
        if len(bookmakers) < config.min_bookmakers:
            continue

        best = max(group, key=lambda item: item.odds)
        odds_values = [item.odds for item in group]
        min_odds = min(odds_values)
        max_odds = max(odds_values)
        average_odds = sum(odds_values) / len(odds_values)
        spread_pct = ((max_odds / min_odds) - 1.0) * 100.0 if min_odds > 0 else 0.0

        rows.append(
            ApiSportsLiveMarketSnapshotRow(
                match=match,
                market=market,
                selection=selection,
                kickoff_utc=best.kickoff_utc,
                fixture_id=best.fixture_id,
                best_bookmaker=best.bookmaker,
                best_odds=round(best.odds, 4),
                average_odds=round(average_odds, 4),
                min_odds=round(min_odds, 4),
                max_odds=round(max_odds, 4),
                spread_pct=round(spread_pct, 4),
                implied_probability_best=round(1.0 / best.odds, 6),
                implied_probability_average=round(1.0 / average_odds, 6),
                bookmakers_count=len(bookmakers),
                offers_count=len(group),
                bookmaker_list=bookmakers,
            )
        )

    rows.sort(key=_row_sort_key)
    return rows[: config.max_rows]


def _row_sort_key(row: ApiSportsLiveMarketSnapshotRow) -> tuple[object, ...]:
    market_rank = {
        "1X2": 0,
        "Total Goals": 1,
        "Both Teams To Score": 2,
    }.get(row.market, 99)

    selection_rank = {
        "Home": 0,
        "Draw": 1,
        "Away": 2,
        "Over 2.5": 3,
        "Under 2.5": 4,
        "Yes": 5,
        "No": 6,
    }.get(row.selection, 99)

    return (row.match, market_rank, selection_rank, row.selection, -row.best_odds)


def _load_candidate_source(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        raise ApiSportsLiveMarketSnapshotError(f"Live market snapshot source does not exist: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ApiSportsLiveMarketSnapshotError(f"Live market snapshot source is invalid JSON: {path}") from exc

    return _extract_candidate_mappings(payload)


def _extract_candidate_mappings(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        for key in ("candidates", "paper_candidates", "items", "offers"):
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return [item for item in value if isinstance(item, Mapping)]

        if {"match", "market", "selection", "odds"} <= set(payload):
            return [payload]

        return []

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return [item for item in payload if isinstance(item, Mapping)]

    return []


def _required_text(value: Any, field: str) -> str:
    result = _optional_text(value)
    if result is None:
        raise ApiSportsLiveMarketSnapshotError(f"Offer field is required: {field}")
    return result


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _required_float(value: Any, field: str) -> float:
    if value is None:
        raise ApiSportsLiveMarketSnapshotError(f"Offer field is required: {field}")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ApiSportsLiveMarketSnapshotError(f"Offer field must be numeric: {field}") from exc


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsLiveMarketSnapshotError(f"{name} must be an integer.") from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _md(value: object) -> str:
    return str(value).replace("|", "\\|")
