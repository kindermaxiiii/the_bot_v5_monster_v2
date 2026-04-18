from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

TRUE_SET = {"1", "true", "yes", "y", "on"}


def _get_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return default if value is None else str(value).strip()


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in TRUE_SET


@dataclass(frozen=True)
class Settings:
    # ------------------------------------------------------------------
    # App / DB
    # ------------------------------------------------------------------
    app_env: str = _get_str("APP_ENV", "dev")
    db_url: str = _get_str("DB_URL", "sqlite:///the_bot_v5.db")

    # ------------------------------------------------------------------
    # API Football
    # ------------------------------------------------------------------
    api_football_base_url: str = _get_str("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io")
    api_football_key: str = _get_str("API_FOOTBALL_KEY", "")
    api_football_key_header: str = _get_str("API_FOOTBALL_KEY_HEADER", "x-apisports-key")
    api_timeout_seconds: int = _get_int("API_TIMEOUT_SECONDS", 20)
    api_retry_attempts: int = _get_int("API_RETRY_ATTEMPTS", 3)
    api_retry_backoff_seconds: float = _get_float("API_RETRY_BACKOFF_SECONDS", 1.5)
    api_rate_limit_cooldown_seconds: float = _get_float("API_RATE_LIMIT_COOLDOWN_SECONDS", 25.0)
    api_rate_limit_cooldown_statistics_seconds: float = _get_float(
        "API_RATE_LIMIT_COOLDOWN_STATISTICS_SECONDS",
        _get_float("API_STATISTICS_RATE_LIMIT_COOLDOWN_SECONDS", 25.0),
    )
    api_rate_limit_cooldown_odds_seconds: float = _get_float(
        "API_RATE_LIMIT_COOLDOWN_ODDS_SECONDS",
        _get_float("API_ODDS_RATE_LIMIT_COOLDOWN_SECONDS", 10.0),
    )

    api_include_lineups: bool = _get_bool("API_INCLUDE_LINEUPS", False)
    api_include_players: bool = _get_bool("API_INCLUDE_PLAYERS", False)
    api_include_odds: bool = _get_bool("API_INCLUDE_ODDS", True)
    api_include_statistics: bool = _get_bool("API_INCLUDE_STATISTICS", True)

    max_active_matches: int = _get_int("MAX_ACTIVE_MATCHES", 26)
    live_fetch_limit: int = _get_int("LIVE_FETCH_LIMIT", 30)

    # ------------------------------------------------------------------
    # Discord
    # ------------------------------------------------------------------
    discord_webhook_real: str = _get_str("DISCORD_WEBHOOK_REAL", "")
    discord_webhook_doc: str = _get_str("DISCORD_WEBHOOK_DOC", "")
    discord_webhook_logs: str = _get_str("DISCORD_WEBHOOK_LOGS", "")

    discord_timeout_seconds: int = _get_int("DISCORD_TIMEOUT_SECONDS", 12)
    discord_retry_attempts: int = _get_int("DISCORD_RETRY_ATTEMPTS", 3)
    discord_retry_backoff_seconds: float = _get_float("DISCORD_RETRY_BACKOFF_SECONDS", 1.5)
    discord_rate_limit_cooldown_seconds: float = _get_float("DISCORD_RATE_LIMIT_COOLDOWN_SECONDS", 2.5)

    dispatch_dedupe_seconds: int = _get_int("DISPATCH_DEDUPE_SECONDS", 180)

    # ------------------------------------------------------------------
    # Books
    # ------------------------------------------------------------------
    primary_bookmaker_name: str = _get_str("PRIMARY_BOOKMAKER_NAME", "Betify")
    fallback_bookmaker_name: str = _get_str("FALLBACK_BOOKMAKER_NAME", "Bet365")
    documentary_bookmaker_name: str = _get_str("DOCUMENTARY_BOOKMAKER_NAME", "Bet365")

    # ------------------------------------------------------------------
    # Runtime cadence / windows
    # ------------------------------------------------------------------
    heartbeat_seconds: int = _get_int("HEARTBEAT_SECONDS", 45)
    skip_finished_statuses: bool = _get_bool("SKIP_FINISHED_STATUSES", True)

    dispatch_minute_min: int = _get_int("DISPATCH_MINUTE_MIN", 1)
    dispatch_minute_max: int = _get_int("DISPATCH_MINUTE_MAX", 92)

    boot_warmup_seconds: int = _get_int("BOOT_WARMUP_SECONDS", 90)
    min_fixture_observations_before_dispatch: int = _get_int("MIN_FIXTURE_OBSERVATIONS_BEFORE_DISPATCH", 2)
    min_minute_after_boot_for_dispatch: int = _get_int("MIN_MINUTE_AFTER_BOOT_FOR_DISPATCH", 20)

    runtime_log_top_candidates: bool = _get_bool("RUNTIME_LOG_TOP_CANDIDATES", True)
    runtime_top_candidates_limit: int = _get_int("RUNTIME_TOP_CANDIDATES_LIMIT", 3)
    runtime_rescue_enabled: bool = _get_bool("RUNTIME_RESCUE_ENABLED", False)
    runtime_deep_fetch_minute_min: int = _get_int("RUNTIME_DEEP_FETCH_MINUTE_MIN", 24)
    runtime_deep_fetch_fixture_limit: int = _get_int("RUNTIME_DEEP_FETCH_FIXTURE_LIMIT", 14)
    runtime_statistics_fetch_fixture_limit: int = _get_int("RUNTIME_STATISTICS_FETCH_FIXTURE_LIMIT", 8)
    runtime_odds_fetch_fixture_limit: int = _get_int("RUNTIME_ODDS_FETCH_FIXTURE_LIMIT", 10)
    runtime_odds_priority_fixture_limit: int = _get_int("RUNTIME_ODDS_PRIORITY_FIXTURE_LIMIT", 6)
    runtime_pre_warmup_odds_priority_min: float = _get_float("RUNTIME_PRE_WARMUP_ODDS_PRIORITY_MIN", 7.2)
    runtime_skip_odds_until_warmup_ready: bool = _get_bool("RUNTIME_SKIP_ODDS_UNTIL_WARMUP_READY", True)
    runtime_deep_fetch_minute_max: int = _get_int("RUNTIME_DEEP_FETCH_MINUTE_MAX", 92)
    api_stats_cache_ttl_seconds: int = _get_int("API_STATS_CACHE_TTL_SECONDS", 120)
    api_odds_cache_ttl_seconds: int = _get_int("API_ODDS_CACHE_TTL_SECONDS", 25)
    api_stats_stale_cache_grace_seconds: int = _get_int("API_STATS_STALE_CACHE_GRACE_SECONDS", 300)
    api_odds_stale_cache_grace_seconds: int = _get_int("API_ODDS_STALE_CACHE_GRACE_SECONDS", 90)

    # ------------------------------------------------------------------
    # Governance / alert policy
    # ------------------------------------------------------------------
    max_tickets_per_match: int = _get_int("MAX_TICKETS_PER_MATCH", 2)
    max_real_tickets_per_match: int = _get_int("MAX_REAL_TICKETS_PER_MATCH", 1)
    max_real_alerts_per_cycle: int = _get_int("MAX_REAL_ALERTS_PER_CYCLE", 2)
    max_doc_alerts_per_cycle: int = _get_int("MAX_DOC_ALERTS_PER_CYCLE", 0)

    allow_documentary_dispatch: bool = _get_bool("ALLOW_DOCUMENTARY_DISPATCH", False)
    documentary_requires_executable: bool = _get_bool("DOCUMENTARY_REQUIRES_EXECUTABLE", True)
    require_executable_for_real: bool = _get_bool("REQUIRE_EXECUTABLE_FOR_REAL", True)
    real_only_top_bets: bool = _get_bool("REAL_ONLY_TOP_BETS", False)

    fixture_cooldown_minutes: int = _get_int("FIXTURE_COOLDOWN_MINUTES", 12)
    same_signal_block_minutes: int = _get_int("SAME_SIGNAL_BLOCK_MINUTES", 18)
    same_family_block_minutes: int = _get_int("SAME_FAMILY_BLOCK_MINUTES", 10)

    # ------------------------------------------------------------------
    # Storage / exports
    # ------------------------------------------------------------------
    store_raw_payloads: bool = _get_bool("STORE_RAW_PAYLOADS", True)
    csv_export_path: str = _get_str("CSV_EXPORT_PATH", "exports")

    # ------------------------------------------------------------------
    # Quality defaults
    # ------------------------------------------------------------------
    competition_quality_default: float = _get_float("COMPETITION_QUALITY_DEFAULT", 0.60)
    feed_quality_default: float = _get_float("FEED_QUALITY_DEFAULT", 0.58)
    market_quality_default: float = _get_float("MARKET_QUALITY_DEFAULT", 0.62)

    # ------------------------------------------------------------------
    # Global decision thresholds
    # ------------------------------------------------------------------
    min_edge_real: float = _get_float("MIN_EDGE_REAL", 0.060)
    min_edge_top_bet: float = _get_float("MIN_EDGE_TOP_BET", 0.100)
    min_edge_doc: float = _get_float("MIN_EDGE_DOC", 0.070)

    min_ev_real: float = _get_float("MIN_EV_REAL", 0.050)
    min_ev_top_bet: float = _get_float("MIN_EV_TOP_BET", 0.100)

    min_prob_gap_real: float = _get_float("MIN_PROB_GAP_REAL", 0.025)
    min_regime_confidence_real: float = _get_float("MIN_REGIME_CONFIDENCE_REAL", 0.64)
    min_regime_confidence_top_bet: float = _get_float("MIN_REGIME_CONFIDENCE_TOP_BET", 0.72)

    confidence_real_cap: float = _get_float("CONFIDENCE_REAL_CAP", 8.1)
    confidence_top_bet_cap: float = _get_float("CONFIDENCE_TOP_BET_CAP", 8.9)
    confidence_doc_cap: float = _get_float("CONFIDENCE_DOC_CAP", 7.2)
    min_display_confidence_real: float = _get_float("MIN_DISPLAY_CONFIDENCE_REAL", 6.2)
    min_display_confidence_top_bet: float = _get_float("MIN_DISPLAY_CONFIDENCE_TOP_BET", 7.4)
    top_bet_min_fixture_priority_score: float = _get_float("TOP_BET_MIN_FIXTURE_PRIORITY_SCORE", 6.0)

    max_chaos_real: float = _get_float("MAX_CHAOS_REAL", 0.72)

    # ------------------------------------------------------------------
    # Engines enabled
    # ------------------------------------------------------------------
    over_under_enabled: bool = _get_bool("OVER_UNDER_ENABLED", True)
    first_half_enabled: bool = _get_bool("FIRST_HALF_ENABLED", False)
    btts_enabled: bool = _get_bool("BTTS_ENABLED", False)
    team_totals_enabled: bool = _get_bool("TEAM_TOTALS_ENABLED", False)
    result_engine_enabled: bool = _get_bool("RESULT_ENGINE_ENABLED", False)
    correct_score_enabled: bool = _get_bool("CORRECT_SCORE_ENABLED", False)
    correct_score_real_enabled: bool = _get_bool("CORRECT_SCORE_REAL_ENABLED", False)
    correct_score_max_doc_candidates: int = _get_int("CORRECT_SCORE_MAX_DOC_CANDIDATES", 0)

    # ------------------------------------------------------------------
    # O/U live doctrine
    # ------------------------------------------------------------------
    ou_one_candidate_per_match: bool = _get_bool("OU_ONE_CANDIDATE_PER_MATCH", True)

    under_doc_minute_u05: int = _get_int("UNDER_DOC_MINUTE_U05", 78)
    under_doc_minute_u15: int = _get_int("UNDER_DOC_MINUTE_U15", 72)
    under_doc_minute_u25_score2plus: int = _get_int("UNDER_DOC_MINUTE_U25_SCORE2PLUS", 66)
    under_doc_minute_u25_score0or1: int = _get_int("UNDER_DOC_MINUTE_U25_SCORE0OR1", 58)

    under_real_minute_u05: int = _get_int("UNDER_REAL_MINUTE_U05", 82)
    under_real_minute_u15: int = _get_int("UNDER_REAL_MINUTE_U15", 75)
    under_real_minute_u25_score2plus: int = _get_int("UNDER_REAL_MINUTE_U25_SCORE2PLUS", 68)
    under_real_minute_u25_score0or1: int = _get_int("UNDER_REAL_MINUTE_U25_SCORE0OR1", 60)

    over_real_max_goals_needed_after55: float = _get_float("OVER_REAL_MAX_GOALS_NEEDED_AFTER55", 1.0)
    over_real_max_goals_needed_after65: float = _get_float("OVER_REAL_MAX_GOALS_NEEDED_AFTER65", 0.5)
    over_doc_max_goals_needed_after60: float = _get_float("OVER_DOC_MAX_GOALS_NEEDED_AFTER60", 1.0)
    over_doc_max_goals_needed_after70: float = _get_float("OVER_DOC_MAX_GOALS_NEEDED_AFTER70", 0.5)
    resolution_pressure_real_max: float = _get_float("RESOLUTION_PRESSURE_REAL_MAX", 0.050)
    late_fragility_real_max: float = _get_float("LATE_FRAGILITY_REAL_MAX", 0.070)
    under_small_goal_budget_real_max_minute: int = _get_int("UNDER_SMALL_GOAL_BUDGET_REAL_MAX_MINUTE", 62)
    under_one_goal_budget_real_max_minute: int = _get_int("UNDER_ONE_GOAL_BUDGET_REAL_MAX_MINUTE", 56)
    under_two_goal_budget_real_max_minute: int = _get_int("UNDER_TWO_GOAL_BUDGET_REAL_MAX_MINUTE", 34)
    under_goal_budget_time_conflict_min_remaining_minutes: int = _get_int(
        "UNDER_GOAL_BUDGET_TIME_CONFLICT_MIN_REMAINING_MINUTES",
        32,
    )
    under_one_goal_budget_expectancy_real_max: float = _get_float(
        "UNDER_ONE_GOAL_BUDGET_EXPECTANCY_REAL_MAX",
        1.10,
    )
    under_two_goal_budget_expectancy_real_max: float = _get_float(
        "UNDER_TWO_GOAL_BUDGET_EXPECTANCY_REAL_MAX",
        1.65,
    )
    under_early_fragility_real_max: float = _get_float("UNDER_EARLY_FRAGILITY_REAL_MAX", 0.78)
    late_over_0_5_nil_nil_real_ban_minute: int = _get_int("LATE_OVER_0_5_NIL_NIL_REAL_BAN_MINUTE", 76)
    late_over_1_5_small_score_strict_minute: int = _get_int("LATE_OVER_1_5_SMALL_SCORE_STRICT_MINUTE", 68)
    late_over_1_5_small_score_min_regime_confidence: float = _get_float(
        "LATE_OVER_1_5_SMALL_SCORE_MIN_REGIME_CONFIDENCE",
        0.72,
    )
    late_over_1_5_small_score_min_calibration_confidence: float = _get_float(
        "LATE_OVER_1_5_SMALL_SCORE_MIN_CALIBRATION_CONFIDENCE",
        0.56,
    )
    late_over_1_5_small_score_min_data_quality: float = _get_float(
        "LATE_OVER_1_5_SMALL_SCORE_MIN_DATA_QUALITY",
        0.54,
    )
    late_over_1_5_small_score_max_resolution_distance: float = _get_float(
        "LATE_OVER_1_5_SMALL_SCORE_MAX_RESOLUTION_DISTANCE",
        0.5,
    )
    late_extreme_total_line_threshold: float = _get_float("LATE_EXTREME_TOTAL_LINE_THRESHOLD", 5.5)
    late_extreme_total_ban_minute: int = _get_int("LATE_EXTREME_TOTAL_BAN_MINUTE", 78)
    late_extreme_total_max_goals_needed: float = _get_float("LATE_EXTREME_TOTAL_MAX_GOALS_NEEDED", 0.0)
    calibration_under_early_goal_budget_shrink_max: float = _get_float(
        "CALIBRATION_UNDER_EARLY_GOAL_BUDGET_SHRINK_MAX",
        0.10,
    )

    late_state_jump_blend: float = _get_float("LATE_STATE_JUMP_BLEND", 0.12)
    late_state_jump_minute: int = _get_int("LATE_STATE_JUMP_MINUTE", 68)
    late_state_jump_max_goal_diff: int = _get_int("LATE_STATE_JUMP_MAX_GOAL_DIFF", 2)


settings = Settings()
