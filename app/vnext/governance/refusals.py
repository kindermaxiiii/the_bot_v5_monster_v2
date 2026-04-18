from __future__ import annotations


MATCH_LEVEL_REFUSALS = {
    "no_best_candidate",
    "candidate_not_selectable",
    "family_not_allowed",
    "posterior_too_weak",
    "directionality_too_weak",
    "support_too_weak",
    "confidence_too_weak",
}

BOARD_LEVEL_REFUSALS = {
    "board_not_dominant",
    "elite_thresholds_not_met",
    "watchlist_capacity_reached",
    "elite_capacity_reached",
    "better_match_already_promoted",
}
