from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

from app.core.match_state import MatchState, TeamLiveStats


def _load_runner_module():
    script_path = Path("scripts/run_v2_live_shadow.py")
    spec = importlib.util.spec_from_file_location("run_v2_live_shadow", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_load_v1_documentary_references_reads_board_csv() -> None:
    runner = _load_runner_module()
    csv_path = Path("tests/v2/fixtures/board_v5_runner_test.csv")

    match_documents, board_best = runner.load_v1_documentary_references(csv_path)

    assert match_documents[1001]["market_key"] == "OU_FT"
    assert match_documents[1001]["line"] == 2.5
    assert board_best is not None
    assert board_best["fixture_id"] == 1001
    assert board_best["market_key"] == "OU_FT"


def test_live_shadow_run_stats_aggregates_cycle_metrics() -> None:
    runner = _load_runner_module()
    stats = runner.LiveShadowRunStats()
    state = MatchState(
        fixture_id=2001,
        minute=70,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=0,
        home=TeamLiveStats(name="Home"),
        away=TeamLiveStats(name="Away"),
        quotes=[object(), object(), object()],
    )
    payload = {
        "match_results": [
            {"projection_counts": {"OU_FT": 2, "BTTS": 1}},
            {"projection_counts": {"RESULT": 3}},
        ],
        "board_best": {"best_projection": {"market_key": "BTTS"}},
        "shadow_alert_tier": "WATCHLIST",
        "shadow_governance": {
            "elite_refusal_reasons": ["elite_thresholds_not_met", "competition_too_weak"],
            "watchlist_refusal_reasons": ["insufficient_stats"],
        },
        "top_bet_eligible": True,
    }

    stats.ingest_cycle(states=[state], payload=payload)
    summary = stats.to_dict()

    assert summary["cycle_count"] == 1
    assert summary["fixture_count_total"] == 1
    assert summary["quotes_total"] == 3
    assert summary["projection_count_by_family"]["OU_FT"] == 2
    assert summary["projection_count_by_family"]["BTTS"] == 1
    assert summary["projection_count_by_family"]["RESULT"] == 3
    assert summary["board_best_by_family"]["BTTS"] == 1
    assert summary["shadow_alert_tier_counts"]["WATCHLIST"] == 1
    assert summary["match_gate_state_counts"]["UNKNOWN"] == 2
    assert summary["elite_refusal_reason_counts"]["elite_thresholds_not_met"] == 1
    assert summary["watchlist_refusal_reason_counts"]["insufficient_stats"] == 1
    assert summary["top_bet_eligible_true_count"] == 1


def test_live_shadow_run_stats_aggregates_match_gate_states() -> None:
    runner = _load_runner_module()
    stats = runner.LiveShadowRunStats()
    payload = {
        "match_results": [
            {"projection_counts": {"OU_FT": 1}, "priority": {"match_gate_state": "DOC_ONLY"}},
            {"projection_counts": {"RESULT": 1}, "priority": {"match_gate_state": "MATCH_UNDER_REVIEW"}},
            {"projection_counts": {"BTTS": 1}, "priority": {"match_gate_state": "MATCH_ELIGIBLE"}},
        ],
        "board_best": {},
        "shadow_alert_tier": "NONE",
        "shadow_governance": {},
        "top_bet_eligible": False,
    }

    stats.ingest_cycle(states=[], payload=payload)
    summary = stats.to_dict()

    assert summary["match_gate_state_counts"]["DOC_ONLY"] == 1
    assert summary["match_gate_state_counts"]["MATCH_UNDER_REVIEW"] == 1
    assert summary["match_gate_state_counts"]["MATCH_ELIGIBLE"] == 1


def test_end_of_run_stop_reason_blocks_tiny_tail_cycles_and_cooldown_traps() -> None:
    runner = _load_runner_module()

    assert runner._end_of_run_stop_reason(remaining_seconds=5.0, odds_cooldown_remaining=0.0) == "end_of_run_remaining_window"
    assert runner._end_of_run_stop_reason(remaining_seconds=6.5, odds_cooldown_remaining=4.9) == "end_of_run_remaining_window"
    assert runner._end_of_run_stop_reason(remaining_seconds=10.0, odds_cooldown_remaining=8.8) == "end_of_run_odds_cooldown_guard"
    assert runner._end_of_run_stop_reason(remaining_seconds=14.0, odds_cooldown_remaining=4.0) is None
