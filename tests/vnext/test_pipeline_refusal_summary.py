from __future__ import annotations

from app.vnext.board.arbiter import build_board_snapshot
from app.vnext.pipeline.builder import build_publishable_pipeline
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result


def test_governance_vs_execution_refusals_separated() -> None:
    posterior = build_reference_posterior_result()
    selection = build_match_market_selection_result(posterior)
    board = build_board_snapshot((selection,))

    pipeline = build_publishable_pipeline(board, ())

    result = pipeline.results[0]
    assert result.governance_refusal_summary is not None
    assert result.execution_refusal_summary is not None
    assert "execution_missing_for_match" in result.execution_refusal_summary
