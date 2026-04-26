from app.fqis.contracts.core import ExecutableBet
from app.fqis.contracts.enums import MarketFamily, MarketSide, Period, TeamRole, ThesisKey
from app.fqis.risk.gates import apply_risk_gates


def _build_bet(
    *,
    odds: float = 1.90,
    edge: float = 0.05,
    ev: float = 0.08,
    strength: float = 0.80,
    confidence: float = 0.78,
) -> ExecutableBet:
    return ExecutableBet(
        event_id=701,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        family=MarketFamily.TEAM_TOTAL_AWAY,
        side=MarketSide.UNDER,
        period=Period.FT,
        team_role=TeamRole.AWAY,
        line=1.5,
        bookmaker_id=1,
        bookmaker_name="BookA",
        odds_decimal=odds,
        p_real=0.60,
        p_implied=0.52,
        edge=edge,
        ev=ev,
        score_stat=0.80,
        score_exec=0.85,
        score_final=0.82,
        rationale=(f"strength={strength:.4f}", f"confidence={confidence:.4f}"),
    )


def test_risk_gate_accepts_valid_bet() -> None:
    bet = _build_bet()

    decision = apply_risk_gates(
        bet,
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert decision.accepted is True
    assert decision.rejections == ()


def test_risk_gate_rejects_low_edge() -> None:
    bet = _build_bet(edge=0.005)

    decision = apply_risk_gates(
        bet,
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert decision.accepted is False
    assert any("edge=" in detail for _, _, detail in decision.rejections)


def test_risk_gate_rejects_price_too_high() -> None:
    bet = _build_bet(odds=3.10)

    decision = apply_risk_gates(
        bet,
        min_strength=0.70,
        min_confidence=0.70,
        min_edge=0.02,
        min_ev=0.01,
        min_odds=1.50,
        max_odds=2.80,
    )

    assert decision.accepted is False
    assert any("odds=" in detail for _, _, detail in decision.rejections)