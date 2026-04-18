from __future__ import annotations

from app.vnext.markets.translators import translate_market_candidates
from tests.vnext.live_factories import build_reference_posterior_result


def test_market_translators_generate_core_candidates() -> None:
    posterior_result = build_reference_posterior_result()
    translation = translate_market_candidates(posterior_result)
    by_key = {candidate.line_template.key: candidate for candidate in translation.candidates}

    assert "OU_FT_OVER_CORE" in by_key
    assert "TEAM_TOTAL_HOME_OVER_CORE" in by_key
    assert by_key["OU_FT_OVER_CORE"].exists is True
    assert by_key["OU_FT_OVER_CORE"].line_template.suggested_line_family == "over_1_5_or_2_5"


def test_candidates_distinguish_exists_blocked_and_selectable() -> None:
    posterior_result = build_reference_posterior_result()
    translation = translate_market_candidates(posterior_result)

    assert any(candidate.exists for candidate in translation.candidates)
    assert any(candidate.is_blocked for candidate in translation.candidates)
    assert any(candidate.is_selectable for candidate in translation.candidates if candidate.family != "RESULT")
