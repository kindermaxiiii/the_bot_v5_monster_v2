from __future__ import annotations

from app.vnext.markets.translators import translate_market_candidates
from app.vnext.selection.match_selector import build_match_market_selection_result
from tests.vnext.live_factories import build_reference_posterior_result


def test_result_can_exist_but_cannot_win_match_selection() -> None:
    posterior_result = build_reference_posterior_result()
    translation = translate_market_candidates(posterior_result)
    selection = build_match_market_selection_result(posterior_result)

    assert any(candidate.family == "RESULT" for candidate in translation.candidates)
    assert selection.best_candidate is not None
    assert selection.best_candidate.candidate.family != "RESULT"
