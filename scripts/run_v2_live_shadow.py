from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.clients.api_football import APIFootballRequestError, api_client
from app.config import settings
from app.core.match_state import MatchState, build_match_state
from app.normalizers.fixtures import normalize_live_fixtures
from app.normalizers.odds import normalize_live_odds
from app.normalizers.statistics import normalize_fixture_statistics
from app.v2.runtime.shadow_discord_notifier import V2ShadowDiscordNotifier
from app.v2.runtime.live_shadow_bridge import LiveShadowBridge


LOGGER_NAME = "run_v2_live_shadow"
INITIAL_STATISTICS_FETCH_LIMIT = 6
STEADY_STATISTICS_FETCH_LIMIT = 8
INITIAL_ODDS_FETCH_LIMIT = 8
STEADY_ODDS_FETCH_LIMIT = 10
STALE_CACHE_GRACE_MULTIPLIER = 3
TOP_BET_RELIABILITY_MIN_CYCLES = 2
MIN_END_OF_RUN_WINDOW_SECONDS = 8.0
END_OF_RUN_ODDS_COOLDOWN_GUARD_SECONDS = 2.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _slug_now() -> str:
    return _utc_now().strftime("%Y%m%d_%H%M%S")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "NA"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _phase_rank(status: str) -> int:
    normalized = str(status or "").upper().strip()
    if normalized == "2H":
        return 4
    if normalized == "HT":
        return 3
    if normalized == "1H":
        return 2
    if normalized in {"LIVE", "INPLAY"}:
        return 1
    return 0


def _is_finished_status(status: str) -> bool:
    if not bool(getattr(settings, "skip_finished_statuses", True)):
        return False
    return str(status or "").upper().strip() in {"FT", "AET", "PEN", "CANC", "ABD", "SUSP"}


def _prepare_live_fixture_rows(fixtures: list[dict[str, Any]], *, max_active_matches: int) -> list[dict[str, Any]]:
    active = [row for row in fixtures if not _is_finished_status(str(row.get("status") or ""))]
    active.sort(
        key=lambda row: (_phase_rank(str(row.get("status") or "")), _safe_int(row.get("minute"), 0)),
        reverse=True,
    )
    if max_active_matches > 0:
        active = active[:max_active_matches]
    return active


def _default_v1_board_csv_path() -> Path:
    return Path(settings.csv_export_path) / "board_v5.csv"


def load_v1_documentary_references(
    csv_path: str | Path | None = None,
) -> tuple[dict[int, dict[str, Any]], dict[str, Any] | None]:
    path = Path(csv_path or _default_v1_board_csv_path())
    if not path.exists():
        return {}, None

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return {}, None

    match_documents: dict[int, dict[str, Any]] = {}
    board_best: dict[str, Any] | None = None

    for raw_row in rows:
        fixture_id = _safe_int(raw_row.get("fixture_id"), 0)
        if fixture_id <= 0:
            continue

        document = {
            "fixture_id": fixture_id,
            "market_key": raw_row.get("market"),
            "side": raw_row.get("side"),
            "line": _safe_float(raw_row.get("line")),
            "bookmaker": raw_row.get("bookmaker"),
            "odds_decimal": _safe_float(raw_row.get("odds")),
            "diagnostics": {
                "documentary_status": raw_row.get("documentary_status"),
                "real_status": raw_row.get("real_status"),
                "price_state": raw_row.get("price_state"),
                "executable": raw_row.get("executable"),
            },
        }

        if fixture_id not in match_documents:
            match_documents[fixture_id] = document
        if board_best is None:
            board_best = document

    return match_documents, board_best


@dataclass(slots=True)
class _SimpleTTLCache:
    ttl_seconds: int
    values: dict[int, tuple[float, Any]] = field(default_factory=dict)

    def get(self, key: int) -> Any | None:
        entry = self.values.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.monotonic() - ts > max(1, self.ttl_seconds):
            self.values.pop(key, None)
            return None
        return value

    def put(self, key: int, value: Any) -> None:
        self.values[key] = (time.monotonic(), value)

    def get_stale(self, key: int, *, max_age_seconds: int | None = None) -> Any | None:
        entry = self.values.get(key)
        if entry is None:
            return None
        ts, value = entry
        max_age = max(1, int(max_age_seconds or (self.ttl_seconds * STALE_CACHE_GRACE_MULTIPLIER)))
        if time.monotonic() - ts > max_age:
            self.values.pop(key, None)
            return None
        return value


@dataclass(slots=True)
class LiveShadowRunStats:
    cycle_count: int = 0
    fixture_count_total: int = 0
    quotes_total: int = 0
    projection_count_by_family: Counter[str] = field(default_factory=Counter)
    board_best_by_family: Counter[str] = field(default_factory=Counter)
    shadow_alert_tier_counts: Counter[str] = field(default_factory=Counter)
    match_gate_state_counts: Counter[str] = field(default_factory=Counter)
    elite_refusal_reason_counts: Counter[str] = field(default_factory=Counter)
    watchlist_refusal_reason_counts: Counter[str] = field(default_factory=Counter)
    top_bet_eligible_true_count: int = 0
    top_bet_guardrail: str = "insufficient_cycles"

    def ingest_cycle(self, *, states: list[MatchState], payload: dict[str, Any]) -> None:
        self.cycle_count += 1
        self.fixture_count_total += len(states)
        self.quotes_total += sum(len(getattr(state, "quotes", []) or []) for state in states)

        for match_result in payload.get("match_results", []) or []:
            for market_key, count in (match_result.get("projection_counts", {}) or {}).items():
                self.projection_count_by_family[str(market_key)] += _safe_int(count, 0)
            priority = match_result.get("priority", {}) or {}
            match_gate_state = str(priority.get("match_gate_state") or "UNKNOWN").upper().strip()
            self.match_gate_state_counts[match_gate_state or "UNKNOWN"] += 1

        board_projection = (payload.get("board_best", {}) or {}).get("best_projection")
        if isinstance(board_projection, dict):
            market_key = str(board_projection.get("market_key") or "").upper().strip()
            if market_key:
                self.board_best_by_family[market_key] += 1

        shadow_governance = payload.get("shadow_governance", {}) or {}
        for reason in shadow_governance.get("elite_refusal_reasons", []) or []:
            normalized = str(reason or "").strip()
            if normalized:
                self.elite_refusal_reason_counts[normalized] += 1
        for reason in shadow_governance.get("watchlist_refusal_reasons", []) or []:
            normalized = str(reason or "").strip()
            if normalized:
                self.watchlist_refusal_reason_counts[normalized] += 1

        alert_tier = str(payload.get("shadow_alert_tier") or payload.get("shadow_governance", {}).get("shadow_alert_tier") or "NONE")
        self.shadow_alert_tier_counts[alert_tier] += 1

        if bool(payload.get("top_bet_eligible")):
            self.top_bet_eligible_true_count += 1

        self.top_bet_guardrail = _top_bet_guardrail_label(self.cycle_count)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cycle_count": self.cycle_count,
            "fixture_count_total": self.fixture_count_total,
            "quotes_total": self.quotes_total,
            "projection_count_by_family": dict(sorted(self.projection_count_by_family.items())),
            "board_best_by_family": dict(sorted(self.board_best_by_family.items())),
            "shadow_alert_tier_counts": dict(sorted(self.shadow_alert_tier_counts.items())),
            "match_gate_state_counts": dict(sorted(self.match_gate_state_counts.items())),
            "elite_refusal_reason_counts": dict(sorted(self.elite_refusal_reason_counts.items())),
            "watchlist_refusal_reason_counts": dict(sorted(self.watchlist_refusal_reason_counts.items())),
            "top_bet_eligible_true_count": self.top_bet_eligible_true_count,
            "top_bet_guardrail": self.top_bet_guardrail,
        }


def _top_bet_guardrail_label(cycle_count: int) -> str:
    return "stable_enough_for_reading" if cycle_count >= TOP_BET_RELIABILITY_MIN_CYCLES else "early_cycle_not_reliable"


def _shadow_discord_webhook_url() -> str:
    return str(os.getenv("V2_SHADOW_DISCORD_WEBHOOK") or settings.discord_webhook_doc or "").strip()


def _configure_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def _fetch_live_fixture_rows(*, max_active_matches: int, logger: logging.Logger) -> list[dict[str, Any]]:
    raw_live = api_client.get_live_fixtures()
    normalized = normalize_live_fixtures(raw_live)
    prepared = _prepare_live_fixture_rows(normalized, max_active_matches=max_active_matches)
    logger.info("live_fetch fixtures_raw=%s fixtures_active=%s", len(normalized), len(prepared))
    return prepared


def _minute_priority_score(minute: int) -> float:
    if minute < 18:
        return 0.22
    if minute < 24:
        return 0.48
    if minute <= 72:
        return 1.00
    if minute <= 82:
        return 0.82
    if minute <= 88:
        return 0.58
    return 0.34


def _pre_context_priority_score(fixture_row: dict[str, Any]) -> float:
    status = str(fixture_row.get("status") or "").upper().strip()
    minute = _safe_int(fixture_row.get("minute"), 0)
    home_goals = _safe_int(fixture_row.get("home_goals"), 0)
    away_goals = _safe_int(fixture_row.get("away_goals"), 0)
    total_goals = home_goals + away_goals
    goal_diff = abs(home_goals - away_goals)

    phase_score = min(1.0, max(0.20, _phase_rank(status) / 4.0))
    minute_score = _minute_priority_score(minute)

    score_state_score = 1.0
    if goal_diff >= 2:
        score_state_score = 0.64
    elif goal_diff == 1:
        score_state_score = 0.88

    if total_goals >= 5:
        score_state_score *= 0.76
    elif total_goals == 0 and minute < 28:
        score_state_score *= 0.82

    return 0.42 * minute_score + 0.30 * phase_score + 0.28 * score_state_score


def _fixture_fetch_priority(
    fixture_row: dict[str, Any],
    *,
    stats_cache: _SimpleTTLCache,
    odds_cache: _SimpleTTLCache,
    previous_v2_priority_by_fixture: dict[int, float],
) -> float:
    fixture_id = _safe_int(fixture_row.get("fixture_id"), 0)
    base_score = _pre_context_priority_score(fixture_row)
    previous_v2_priority = previous_v2_priority_by_fixture.get(fixture_id, 0.0)
    previous_v2_score = min(1.0, max(0.0, previous_v2_priority / 10.0))
    stats_cache_bonus = 0.12 if stats_cache.get_stale(fixture_id) is not None else 0.0
    odds_cache_bonus = 0.10 if odds_cache.get_stale(fixture_id) is not None else 0.0
    first_half_bonus = 0.05 if str(fixture_row.get("status") or "").upper().strip() in {"1H", "HT"} else 0.0
    return base_score + 0.22 * previous_v2_score + stats_cache_bonus + odds_cache_bonus + first_half_bonus


def _select_fetch_fixture_ids(
    fixture_rows: list[dict[str, Any]],
    *,
    limit: int,
    stats_cache: _SimpleTTLCache,
    odds_cache: _SimpleTTLCache,
    previous_v2_priority_by_fixture: dict[int, float],
) -> set[int]:
    if limit <= 0:
        return set()

    ranked = sorted(
        fixture_rows,
        key=lambda row: (
            _fixture_fetch_priority(
                row,
                stats_cache=stats_cache,
                odds_cache=odds_cache,
                previous_v2_priority_by_fixture=previous_v2_priority_by_fixture,
            ),
            _safe_int(row.get("minute"), 0),
        ),
        reverse=True,
    )
    return {int(row["fixture_id"]) for row in ranked[:limit]}


def _endpoint_cooldown_remaining(endpoint_name: str) -> float:
    until = getattr(api_client, "_endpoint_cooldowns", {}).get(endpoint_name)
    if until is None:
        return 0.0
    remaining = until - time.monotonic()
    return max(0.0, float(remaining))


def _end_of_run_stop_reason(*, remaining_seconds: float, odds_cooldown_remaining: float) -> str | None:
    remaining = max(0.0, float(remaining_seconds or 0.0))
    odds_cooldown = max(0.0, float(odds_cooldown_remaining or 0.0))

    if remaining <= 0.0:
        return "deadline_reached"
    if remaining < MIN_END_OF_RUN_WINDOW_SECONDS:
        return "end_of_run_remaining_window"
    if odds_cooldown > 0.0 and remaining <= odds_cooldown + END_OF_RUN_ODDS_COOLDOWN_GUARD_SECONDS:
        return "end_of_run_odds_cooldown_guard"
    return None


def _is_api_backpressure_error(exc: Exception) -> bool:
    text = str(exc).strip().lower()
    return "cooldown active" in text or "rate limited" in text or "too many requests" in text


def _fetch_statistics_rows(
    fixture_rows: list[dict[str, Any]],
    *,
    cache: _SimpleTTLCache,
    logger: logging.Logger,
    target_fixture_ids: set[int],
) -> dict[int, dict[str, Any]]:
    if not bool(getattr(settings, "api_include_statistics", True)):
        return {}

    out: dict[int, dict[str, Any]] = {}
    endpoint_remaining = _endpoint_cooldown_remaining("statistics")
    if endpoint_remaining > 0.0:
        logger.info(
            "statistics_fetch_skipped endpoint_cooldown_remaining=%.1fs targets=%s",
            endpoint_remaining,
            len(target_fixture_ids),
        )

    for row in fixture_rows:
        fixture_id = int(row["fixture_id"])
        cached = cache.get(fixture_id)
        if cached is not None:
            out[fixture_id] = dict(cached)
            continue

        if fixture_id not in target_fixture_ids:
            stale = cache.get_stale(fixture_id)
            out[fixture_id] = {} if stale is None else dict(stale)
            continue

        if endpoint_remaining > 0.0:
            stale = cache.get_stale(fixture_id)
            out[fixture_id] = {} if stale is None else dict(stale)
            continue

        try:
            payload = normalize_fixture_statistics(
                api_client.get_fixture_statistics(fixture_id),
                expected_home_team_id=row.get("home_team_id"),
                expected_away_team_id=row.get("away_team_id"),
            )
            cache.put(fixture_id, payload)
            out[fixture_id] = payload
        except APIFootballRequestError as exc:
            stale = cache.get_stale(fixture_id)
            logger.warning("statistics_fetch_failed fixture_id=%s error=%s", fixture_id, exc)
            out[fixture_id] = {} if stale is None else dict(stale)
            if _is_api_backpressure_error(exc):
                endpoint_remaining = max(endpoint_remaining, _endpoint_cooldown_remaining("statistics"), 1.0)
        except Exception as exc:  # pragma: no cover - defensive live guardrail
            stale = cache.get_stale(fixture_id)
            logger.warning("statistics_normalization_failed fixture_id=%s error=%s", fixture_id, exc)
            out[fixture_id] = {} if stale is None else dict(stale)

    return out


def _fetch_odds_rows(
    fixture_rows: list[dict[str, Any]],
    *,
    cache: _SimpleTTLCache,
    logger: logging.Logger,
    target_fixture_ids: set[int],
) -> dict[int, list[dict[str, Any]]]:
    if not bool(getattr(settings, "api_include_odds", True)):
        return {}

    out: dict[int, list[dict[str, Any]]] = {}
    endpoint_remaining = _endpoint_cooldown_remaining("odds_live")
    if endpoint_remaining > 0.0:
        logger.info(
            "odds_fetch_skipped endpoint_cooldown_remaining=%.1fs targets=%s",
            endpoint_remaining,
            len(target_fixture_ids),
        )

    for row in fixture_rows:
        fixture_id = int(row["fixture_id"])
        cached = cache.get(fixture_id)
        if cached is not None:
            out[fixture_id] = list(cached)
            continue

        if fixture_id not in target_fixture_ids:
            stale = cache.get_stale(fixture_id)
            out[fixture_id] = [] if stale is None else list(stale)
            continue

        if endpoint_remaining > 0.0:
            stale = cache.get_stale(fixture_id)
            out[fixture_id] = [] if stale is None else list(stale)
            continue

        try:
            payload = normalize_live_odds(api_client.get_live_odds(fixture_id))
            cache.put(fixture_id, payload)
            out[fixture_id] = payload
        except APIFootballRequestError as exc:
            stale = cache.get_stale(fixture_id)
            logger.warning("odds_fetch_failed fixture_id=%s error=%s", fixture_id, exc)
            out[fixture_id] = [] if stale is None else list(stale)
            if _is_api_backpressure_error(exc):
                endpoint_remaining = max(endpoint_remaining, _endpoint_cooldown_remaining("odds_live"), 1.0)
        except Exception as exc:  # pragma: no cover - defensive live guardrail
            stale = cache.get_stale(fixture_id)
            logger.warning("odds_normalization_failed fixture_id=%s error=%s", fixture_id, exc)
            out[fixture_id] = [] if stale is None else list(stale)

    return out


def _build_match_states(
    fixture_rows: list[dict[str, Any]],
    *,
    statistics_by_fixture: dict[int, dict[str, Any]],
    odds_by_fixture: dict[int, list[dict[str, Any]]],
) -> list[MatchState]:
    states: list[MatchState] = []
    for row in fixture_rows:
        fixture_id = int(row["fixture_id"])
        states.append(
            build_match_state(
                row,
                stats_row=statistics_by_fixture.get(fixture_id, {}),
                odds_rows=odds_by_fixture.get(fixture_id, []),
                lineups_rows=[],
                players_rows=[],
            )
        )
    return states


def _summarize_cycle(payload: dict[str, Any]) -> dict[str, Any]:
    board_projection = (payload.get("board_best", {}) or {}).get("best_projection") or {}
    return {
        "fixture_count": len(payload.get("match_results", []) or []),
        "board_market": board_projection.get("market_key"),
        "board_side": board_projection.get("side"),
        "top_bet_eligible": bool(payload.get("top_bet_eligible")),
        "shadow_alert_tier": str(payload.get("shadow_alert_tier") or payload.get("shadow_governance", {}).get("shadow_alert_tier") or "NONE"),
        "top_bet_guardrail": _top_bet_guardrail_label(_safe_int(payload.get("cycle_index"), 0)),
        "compared_match_count": (payload.get("comparison_summary", {}) or {}).get("compared_match_count", 0),
        "v2_divergence_count": (payload.get("comparison_summary", {}) or {}).get("v2_divergence_count", 0),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run THE BOT V2 in live shadow mode only.")
    parser.add_argument("--minutes", type=int, default=45, help="Supervised run duration in minutes.")
    parser.add_argument("--heartbeat", type=int, default=45, help="Seconds between live shadow cycles.")
    parser.add_argument("--max-active-matches", type=int, default=26, help="Maximum active live matches per cycle.")
    parser.add_argument("--export-jsonl", type=str, default=None, help="Optional JSONL export path.")
    parser.add_argument("--export-log", type=str, default=None, help="Optional log file path.")
    return parser.parse_args()


def run_live_shadow_supervision(
    *,
    minutes: int,
    heartbeat: int,
    max_active_matches: int,
    export_jsonl: str | Path | None = None,
    export_log: str | Path | None = None,
) -> dict[str, Any]:
    timestamp_slug = _slug_now()
    export_dir = Path("exports/v2")
    jsonl_path = Path(export_jsonl) if export_jsonl else export_dir / f"run_v2_live_shadow_{timestamp_slug}.jsonl"
    log_path = Path(export_log) if export_log else export_dir / f"run_v2_live_shadow_{timestamp_slug}.log"

    logger = _configure_logging(log_path)
    stats_cache = _SimpleTTLCache(ttl_seconds=int(getattr(settings, "api_stats_cache_ttl_seconds", 120) or 120))
    odds_cache = _SimpleTTLCache(ttl_seconds=int(getattr(settings, "api_odds_cache_ttl_seconds", 25) or 25))
    bridge = LiveShadowBridge(export_path=jsonl_path)
    notifier = V2ShadowDiscordNotifier()
    aggregate = LiveShadowRunStats()
    previous_v2_priority_by_fixture: dict[int, float] = {}

    logger.info(
        "v2_live_shadow_start minutes=%s heartbeat=%s max_active_matches=%s export_jsonl=%s export_log=%s discord_shadow_enabled=%s discord_watchlist_enabled=%s",
        minutes,
        heartbeat,
        max_active_matches,
        jsonl_path,
        log_path,
        bool(_shadow_discord_webhook_url()),
        notifier.send_watchlist,
    )

    deadline = time.monotonic() + max(0, minutes) * 60
    cycle_index = 0

    try:
        while True:
            remaining_before_cycle = deadline - time.monotonic()
            odds_cooldown_before_cycle = _endpoint_cooldown_remaining("odds_live")
            stop_reason = _end_of_run_stop_reason(
                remaining_seconds=remaining_before_cycle,
                odds_cooldown_remaining=odds_cooldown_before_cycle,
            )
            if stop_reason is not None:
                logger.info(
                    "cycle_stop reason=%s remaining=%.1fs odds_cooldown_remaining=%.1fs before_next_cycle=%s",
                    stop_reason,
                    max(0.0, remaining_before_cycle),
                    odds_cooldown_before_cycle,
                    cycle_index + 1,
                )
                break

            cycle_index += 1
            cycle_start = time.monotonic()
            logger.info("cycle_start idx=%s", cycle_index)

            try:
                fixture_rows = _fetch_live_fixture_rows(max_active_matches=max_active_matches, logger=logger)
                statistics_limit = INITIAL_STATISTICS_FETCH_LIMIT if cycle_index == 1 else STEADY_STATISTICS_FETCH_LIMIT
                odds_limit = INITIAL_ODDS_FETCH_LIMIT if cycle_index == 1 else STEADY_ODDS_FETCH_LIMIT
                statistics_target_fixture_ids = _select_fetch_fixture_ids(
                    fixture_rows,
                    limit=min(statistics_limit, len(fixture_rows)),
                    stats_cache=stats_cache,
                    odds_cache=odds_cache,
                    previous_v2_priority_by_fixture=previous_v2_priority_by_fixture,
                )
                odds_target_fixture_ids = _select_fetch_fixture_ids(
                    fixture_rows,
                    limit=min(odds_limit, len(fixture_rows)),
                    stats_cache=stats_cache,
                    odds_cache=odds_cache,
                    previous_v2_priority_by_fixture=previous_v2_priority_by_fixture,
                )
                logger.info(
                    "cycle_fetch_plan idx=%s stats_targets=%s odds_targets=%s snapshot_only=%s",
                    cycle_index,
                    len(statistics_target_fixture_ids),
                    len(odds_target_fixture_ids),
                    max(0, len(fixture_rows) - len(statistics_target_fixture_ids | odds_target_fixture_ids)),
                )
                statistics_by_fixture = _fetch_statistics_rows(
                    fixture_rows,
                    cache=stats_cache,
                    logger=logger,
                    target_fixture_ids=statistics_target_fixture_ids,
                )
                odds_by_fixture = _fetch_odds_rows(
                    fixture_rows,
                    cache=odds_cache,
                    logger=logger,
                    target_fixture_ids=odds_target_fixture_ids,
                )
                states = _build_match_states(
                    fixture_rows,
                    statistics_by_fixture=statistics_by_fixture,
                    odds_by_fixture=odds_by_fixture,
                )
                v1_match_documents, v1_board_best = load_v1_documentary_references()

                payload = bridge.run_live_states(
                    states,
                    v1_match_documents=v1_match_documents or None,
                    v1_board_best=v1_board_best,
                )
                payload["cycle_index"] = cycle_index
                aggregate.ingest_cycle(states=states, payload=payload)
                previous_v2_priority_by_fixture = {
                    int(match_result["fixture_id"]): float(
                        ((match_result.get("intelligence", {}) or {}).get("fixture_priority_score") or 0.0)
                    )
                    for match_result in payload.get("match_results", []) or []
                }
                cycle_summary = _summarize_cycle(payload)
                states_by_fixture = {int(state.fixture_id): state for state in states}
                discord_result = notifier.notify_from_payload(
                    payload,
                    states_by_fixture=states_by_fixture,
                    top_bet_guardrail=cycle_summary["top_bet_guardrail"],
                )
                logger.info(
                    "discord_shadow idx=%s sent=%s reason=%s alert_tier=%s dedupe_key=%s stability_key=%s stability_count=%s status_code=%s",
                    cycle_index,
                    discord_result.sent,
                    discord_result.reason,
                    discord_result.alert_tier,
                    discord_result.dedupe_key,
                    discord_result.stability_key,
                    discord_result.stability_count,
                    discord_result.status_code,
                )
                logger.info(
                    "cycle_done idx=%s fixtures=%s board_market=%s board_side=%s shadow_alert_tier=%s top_bet_eligible=%s top_bet_guardrail=%s compared=%s divergences=%s",
                    cycle_index,
                    cycle_summary["fixture_count"],
                    cycle_summary["board_market"],
                    cycle_summary["board_side"],
                    cycle_summary["shadow_alert_tier"],
                    cycle_summary["top_bet_eligible"],
                    cycle_summary["top_bet_guardrail"],
                    cycle_summary["compared_match_count"],
                    cycle_summary["v2_divergence_count"],
                )
            except APIFootballRequestError as exc:
                logger.warning("cycle_api_failure idx=%s error=%s", cycle_index, exc)
            except Exception as exc:  # pragma: no cover - live guardrail
                logger.exception("cycle_unhandled_failure idx=%s error=%s", cycle_index, exc)

            remaining = deadline - time.monotonic()
            odds_cooldown_remaining = _endpoint_cooldown_remaining("odds_live")
            stop_reason = _end_of_run_stop_reason(
                remaining_seconds=remaining,
                odds_cooldown_remaining=odds_cooldown_remaining,
            )
            if stop_reason is not None:
                logger.info(
                    "cycle_stop reason=%s remaining=%.1fs odds_cooldown_remaining=%.1fs after_cycle=%s",
                    stop_reason,
                    max(0.0, remaining),
                    odds_cooldown_remaining,
                    cycle_index,
                )
                break

            sleep_seconds = min(float(max(1, heartbeat)), max(0.0, remaining))
            elapsed = time.monotonic() - cycle_start
            logger.info("cycle_sleep idx=%s elapsed=%.1fs sleep=%.1fs", cycle_index, elapsed, sleep_seconds)
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:  # pragma: no cover - manual run guardrail
        logger.warning("v2_live_shadow_interrupted_by_user")

    summary = aggregate.to_dict()
    logger.info("v2_live_shadow_summary %s", summary)

    result = {
        "export_jsonl": str(jsonl_path.resolve()),
        "export_log": str(log_path.resolve()),
        "summary": summary,
    }
    return result


def _print_final_summary(result: dict[str, Any]) -> None:
    summary = result["summary"]
    print("V2 live shadow run complete.")
    print(f"JSONL export: {result['export_jsonl']}")
    print(f"Log export:   {result['export_log']}")
    print("Summary:")
    print(f"  cycle_count: {summary['cycle_count']}")
    print(f"  fixture_count_total: {summary['fixture_count_total']}")
    print(f"  quotes_total: {summary['quotes_total']}")
    print(f"  projection_count_by_family: {summary['projection_count_by_family']}")
    print(f"  board_best_by_family: {summary['board_best_by_family']}")
    print(f"  shadow_alert_tier_counts: {summary['shadow_alert_tier_counts']}")
    print(f"  match_gate_state_counts: {summary['match_gate_state_counts']}")
    print(f"  watchlist_refusal_reason_counts: {summary['watchlist_refusal_reason_counts']}")
    print(f"  elite_refusal_reason_counts: {summary['elite_refusal_reason_counts']}")
    print(f"  top_bet_eligible_true_count: {summary['top_bet_eligible_true_count']}")
    print(f"  top_bet_guardrail: {summary['top_bet_guardrail']}")


def main() -> int:
    args = parse_args()
    result = run_live_shadow_supervision(
        minutes=max(0, int(args.minutes)),
        heartbeat=max(1, int(args.heartbeat)),
        max_active_matches=max(1, int(args.max_active_matches)),
        export_jsonl=args.export_jsonl,
        export_log=args.export_log,
    )
    _print_final_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
