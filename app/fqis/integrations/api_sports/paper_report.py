
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.paper_preview import (
    PAPER_MODE,
    default_paper_preview_path,
)


PAPER_REPORT_MODE = "PAPER_REPORT"


@dataclass(frozen=True)
class ApiSportsPaperReportConfig:
    title: str = "FQIS API-Sports Paper Report"
    include_watchlist: bool = True
    include_rejected: bool = True

    @classmethod
    def from_env(cls) -> "ApiSportsPaperReportConfig":
        return cls(
            title=os.getenv("APISPORTS_PAPER_REPORT_TITLE", "FQIS API-Sports Paper Report"),
            include_watchlist=_env_bool("APISPORTS_PAPER_REPORT_INCLUDE_WATCHLIST", True),
            include_rejected=_env_bool("APISPORTS_PAPER_REPORT_INCLUDE_REJECTED", True),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "title": self.title,
            "include_watchlist": self.include_watchlist,
            "include_rejected": self.include_rejected,
        }


@dataclass(frozen=True)
class ApiSportsPaperReport:
    status: str
    mode: str
    generated_at_utc: str
    preview_path: str | None
    report_path: str | None
    real_staking_enabled: bool
    max_stake_units: float
    bets_total: int
    watchlist_total: int
    rejected_total: int
    errors: tuple[str, ...]
    markdown: str

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "mode": self.mode,
            "generated_at_utc": self.generated_at_utc,
            "preview_path": self.preview_path,
            "report_path": self.report_path,
            "real_staking_enabled": self.real_staking_enabled,
            "max_stake_units": self.max_stake_units,
            "bets_total": self.bets_total,
            "watchlist_total": self.watchlist_total,
            "rejected_total": self.rejected_total,
            "errors": list(self.errors),
            "markdown": self.markdown,
        }


class ApiSportsPaperReportError(RuntimeError):
    pass


def build_api_sports_paper_report(
    *,
    preview_path: str | Path | None = None,
    preview: Mapping[str, Any] | None = None,
    config: ApiSportsPaperReportConfig | None = None,
) -> ApiSportsPaperReport:
    report_config = config or ApiSportsPaperReportConfig.from_env()

    source_path: str | None = None
    if preview is not None:
        preview_payload = dict(preview)
        source_path = str(preview_path) if preview_path is not None else None
    else:
        target = Path(preview_path) if preview_path is not None else default_paper_preview_path()
        preview_payload = _load_json_object(target)
        source_path = str(target)

    errors = _report_errors(preview_payload)

    bets = _records(preview_payload.get("bets"))
    watchlist = _records(preview_payload.get("watchlist"))
    rejected = _records(preview_payload.get("rejected"))

    markdown = _render_markdown(
        preview_payload,
        config=report_config,
        errors=errors,
    )

    return ApiSportsPaperReport(
        status="READY" if not errors else "BLOCKED",
        mode=PAPER_REPORT_MODE,
        generated_at_utc=_utc_now(),
        preview_path=source_path,
        report_path=None,
        real_staking_enabled=_bool(preview_payload.get("real_staking_enabled")),
        max_stake_units=_float_or_zero(preview_payload.get("max_stake_units")),
        bets_total=len(bets),
        watchlist_total=len(watchlist),
        rejected_total=len(rejected),
        errors=tuple(errors),
        markdown=markdown,
    )


def write_api_sports_paper_report(
    *,
    preview_path: str | Path | None = None,
    preview: Mapping[str, Any] | None = None,
    output_path: str | Path | None = None,
    config: ApiSportsPaperReportConfig | None = None,
) -> ApiSportsPaperReport:
    report = build_api_sports_paper_report(
        preview_path=preview_path,
        preview=preview,
        config=config,
    )

    target = Path(output_path) if output_path is not None else default_paper_report_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(report.markdown, encoding="utf-8")

    return ApiSportsPaperReport(
        status=report.status,
        mode=report.mode,
        generated_at_utc=report.generated_at_utc,
        preview_path=report.preview_path,
        report_path=str(target),
        real_staking_enabled=report.real_staking_enabled,
        max_stake_units=report.max_stake_units,
        bets_total=report.bets_total,
        watchlist_total=report.watchlist_total,
        rejected_total=report.rejected_total,
        errors=report.errors,
        markdown=report.markdown,
    )


def default_paper_report_path() -> Path:
    return Path(os.getenv("APISPORTS_PAPER_REPORT_PATH", "data/pipeline/api_sports/paper_report.md"))


def _report_errors(preview_payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    if _optional_str(preview_payload.get("status")) != "READY":
        errors.append("PAPER_PREVIEW_NOT_READY")

    if _optional_str(preview_payload.get("mode")) != PAPER_MODE:
        errors.append("PAPER_PREVIEW_MODE_INVALID")

    if _bool(preview_payload.get("real_staking_enabled")):
        errors.append("REAL_STAKING_ENABLED_FOR_PAPER_REPORT")

    preview_errors = _sequence(preview_payload.get("errors"))
    for item in preview_errors:
        errors.append(f"PAPER_PREVIEW_ERROR:{item}")

    return errors


def _render_markdown(
    preview_payload: Mapping[str, Any],
    *,
    config: ApiSportsPaperReportConfig,
    errors: Sequence[str],
) -> str:
    bets = _records(preview_payload.get("bets"))
    watchlist = _records(preview_payload.get("watchlist"))
    rejected = _records(preview_payload.get("rejected"))

    lines: list[str] = [
        f"# {config.title}",
        "",
        "## Summary",
        "",
        f"- Report status: **{'READY' if not errors else 'BLOCKED'}**",
        f"- Preview status: **{_optional_str(preview_payload.get('status')) or 'UNKNOWN'}**",
        f"- Mode: **{_optional_str(preview_payload.get('mode')) or 'UNKNOWN'}**",
        f"- Real staking enabled: **{str(_bool(preview_payload.get('real_staking_enabled'))).lower()}**",
        f"- Max stake units: **{_fmt_stake(_float_or_zero(preview_payload.get('max_stake_units')))}**",
        f"- Bets: **{len(bets)}**",
        f"- Watchlist: **{len(watchlist)}**",
        f"- Rejected: **{len(rejected)}**",
        f"- Generated at UTC: `{_optional_str(preview_payload.get('generated_at_utc')) or 'UNKNOWN'}`",
        "",
        "> PAPER ONLY. No real money. This report is for review, coherence checks, and operator validation only.",
        "",
    ]

    if errors:
        lines.extend(
            [
                "## Errors",
                "",
                *_bullet_lines(errors),
                "",
            ]
        )

    lines.extend(
        [
            "## Paper Bets",
            "",
            *_pick_table(bets),
            "",
            *_reason_block(bets),
            "",
        ]
    )

    if config.include_watchlist:
        lines.extend(
            [
                "## Watchlist",
                "",
                *_pick_table(watchlist),
                "",
                *_reason_block(watchlist),
                "",
            ]
        )

    if config.include_rejected:
        lines.extend(
            [
                "## Rejected",
                "",
                *_pick_table(rejected),
                "",
                *_reason_block(rejected),
                "",
            ]
        )

    lines.extend(
        [
            "## Operator Notes",
            "",
            "- Validate match names, markets, selections, odds, model probability, and edge manually.",
            "- Do not place real stakes from this report.",
            "- Use this report to judge the shape and readability of the paper betting layer.",
            "",
        ]
    )

    return "\n".join(lines).rstrip() + "\n"


def _pick_table(items: Sequence[Mapping[str, Any]]) -> list[str]:
    if not items:
        return ["No entries."]

    lines = [
        "| # | Match | Market | Selection | Odds | Model Prob | Fair Odds | Edge | Stake | Decision |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---|",
    ]

    for index, item in enumerate(items, start=1):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(index),
                    _cell(_optional_str(item.get("match")) or "UNKNOWN"),
                    _cell(_optional_str(item.get("market")) or "UNKNOWN"),
                    _cell(_optional_str(item.get("selection")) or "UNKNOWN"),
                    _fmt_float(item.get("odds"), digits=2),
                    _fmt_probability(item.get("model_probability")),
                    _fmt_float(item.get("fair_odds"), digits=4),
                    _fmt_edge(item.get("edge_pct")),
                    _fmt_stake(_float_or_zero(item.get("stake_units"))),
                    _cell(_optional_str(item.get("decision")) or "UNKNOWN"),
                ]
            )
            + " |"
        )

    return lines


def _reason_block(items: Sequence[Mapping[str, Any]]) -> list[str]:
    if not items:
        return []

    lines = ["### Reasons and warnings", ""]

    for index, item in enumerate(items, start=1):
        match = _optional_str(item.get("match")) or f"entry-{index}"
        reason = _optional_str(item.get("reason")) or "No reason provided."
        warnings = ", ".join(str(warning) for warning in _sequence(item.get("warnings")))
        warning_text = warnings if warnings else "none"
        lines.append(f"- **{_cell(match)}** ? {reason} Warnings: `{warning_text}`")

    return lines


def _bullet_lines(items: Sequence[Any]) -> list[str]:
    return [f"- `{item}`" for item in items]


def _cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _fmt_float(value: Any, *, digits: int) -> str:
    result = _float_or_none(value)
    if result is None:
        return "n/a"
    return f"{result:.{digits}f}"


def _fmt_probability(value: Any) -> str:
    result = _float_or_none(value)
    if result is None:
        return "n/a"
    if result <= 1.0:
        result *= 100.0
    return f"{result:.2f}%"


def _fmt_edge(value: Any) -> str:
    result = _float_or_none(value)
    if result is None:
        return "n/a"
    return f"{result:+.2f}%"


def _fmt_stake(value: float) -> str:
    return f"{value:.2f}u"


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ApiSportsPaperReportError(f"Paper preview path does not exist: {path}")
    if not path.is_file():
        raise ApiSportsPaperReportError(f"Paper preview path is not a file: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ApiSportsPaperReportError(f"Paper preview path is invalid JSON: {path}") from exc

    if not isinstance(payload, dict):
        raise ApiSportsPaperReportError(f"Paper preview path must contain a JSON object: {path}")

    return payload


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return value


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_or_zero(value: Any) -> float:
    result = _float_or_none(value)
    return result if result is not None else 0.0


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on", "ready", "pass"}
    return bool(value)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
