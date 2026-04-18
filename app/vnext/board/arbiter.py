from __future__ import annotations

from dataclasses import replace

from app.vnext.board.models import BoardEntry, BoardSnapshot
from app.vnext.board.ranking import board_score
from app.vnext.governance.models import GovernanceThresholds, PromotionDecision
from app.vnext.governance.promoter import evaluate_match_level
from app.vnext.governance.rules import DEFAULT_THRESHOLDS
from app.vnext.selection.models import MatchMarketSelectionResult


def _elite_eligible(
    decision: PromotionDecision,
    selection_result: MatchMarketSelectionResult,
    *,
    thresholds: GovernanceThresholds,
) -> bool:
    if decision.internal_status != "READY":
        return False
    candidate = selection_result.best_candidate.candidate if selection_result.best_candidate else None
    if candidate is None:
        return False
    if candidate.support_score < thresholds.elite_min_support:
        return False
    if candidate.confidence_score < thresholds.elite_min_confidence:
        return False
    if selection_result.best_candidate.selection_score < thresholds.elite_min_score:
        return False
    return True


def _watchlist_eligible(
    decision: PromotionDecision,
    selection_result: MatchMarketSelectionResult,
    *,
    thresholds: GovernanceThresholds,
) -> bool:
    if decision.internal_status != "READY":
        return False
    candidate = selection_result.best_candidate.candidate if selection_result.best_candidate else None
    if candidate is None:
        return False
    if candidate.support_score < thresholds.watchlist_min_support:
        return False
    if candidate.confidence_score < thresholds.watchlist_min_confidence:
        return False
    if selection_result.best_candidate.selection_score < thresholds.watchlist_min_score:
        return False
    return True


def build_board_snapshot(
    selection_results: tuple[MatchMarketSelectionResult, ...],
    *,
    thresholds: GovernanceThresholds = DEFAULT_THRESHOLDS,
) -> BoardSnapshot:
    decisions: list[PromotionDecision] = []
    base_entries: list[BoardEntry] = []
    scored_ready: list[tuple[MatchMarketSelectionResult, PromotionDecision, float]] = []

    for selection_result in selection_results:
        decision = evaluate_match_level(selection_result, thresholds=thresholds)
        decisions.append(decision)
        score = board_score(selection_result)
        entry = BoardEntry(
            fixture_id=selection_result.translation_result.posterior_result.prior_result.fixture_id,
            internal_status=decision.internal_status,
            public_status="NO_BET",
            board_score=score,
            rank=0,
            match_refusals=decision.match_refusals,
            board_refusals=decision.board_refusals,
            selection_result=selection_result,
        )
        base_entries.append(entry)
        if decision.internal_status == "READY" and selection_result.best_candidate is not None:
            scored_ready.append((selection_result, decision, score))

    scored_ready.sort(key=lambda item: item[2], reverse=True)
    elite_promoted: list[int] = []
    watchlist_promoted: list[int] = []

    elite_candidate_index = None
    if scored_ready:
        top_selection, top_decision, top_score = scored_ready[0]
        gap = top_score - (scored_ready[1][2] if len(scored_ready) > 1 else 1.0)
        if gap >= thresholds.elite_min_dominance_gap and _elite_eligible(top_decision, top_selection, thresholds=thresholds):
            elite_candidate_index = 0

    for idx, (selection_result, decision, score) in enumerate(scored_ready):
        fixture_id = selection_result.translation_result.posterior_result.prior_result.fixture_id
        entry_index = next(i for i, entry in enumerate(base_entries) if entry.fixture_id == fixture_id)
        entry = base_entries[entry_index]
        board_refusals = list(entry.board_refusals)

        if elite_candidate_index is not None and idx == elite_candidate_index:
            if len(elite_promoted) >= thresholds.max_elite:
                board_refusals.append("elite_capacity_reached")
            else:
                elite_promoted.append(entry_index)
                base_entries[entry_index] = replace(entry, public_status="ELITE", board_refusals=tuple(board_refusals))
                continue
        elif idx == 0:
            board_refusals.append("elite_thresholds_not_met")

        if _watchlist_eligible(decision, selection_result, thresholds=thresholds):
            if len(watchlist_promoted) >= thresholds.max_watchlist:
                board_refusals.append("watchlist_capacity_reached")
            else:
                watchlist_promoted.append(entry_index)
                base_entries[entry_index] = replace(entry, public_status="WATCHLIST", board_refusals=tuple(board_refusals))
                continue
        else:
            board_refusals.append("board_not_dominant")

        if elite_candidate_index is not None and idx != elite_candidate_index:
            board_refusals.append("better_match_already_promoted")
        base_entries[entry_index] = replace(entry, board_refusals=tuple(board_refusals))

    ranked_entries = sorted(base_entries, key=lambda entry: entry.board_score, reverse=True)
    final_entries = []
    for rank, entry in enumerate(ranked_entries, start=1):
        final_entries.append(replace(entry, rank=rank))

    elite_count = sum(1 for entry in final_entries if entry.public_status == "ELITE")
    watchlist_count = sum(1 for entry in final_entries if entry.public_status == "WATCHLIST")
    return BoardSnapshot(
        entries=tuple(final_entries),
        elite_count=elite_count,
        watchlist_count=watchlist_count,
    )
