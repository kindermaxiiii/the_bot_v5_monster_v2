from __future__ import annotations

from app.clients.discord import DiscordSendResult
from app.core.match_state import MatchState, TeamLiveStats
from app.v2.runtime.shadow_discord_notifier import V2ShadowDiscordNotifier


def _payload(
    *,
    top_bet_eligible: bool = True,
    shadow_alert_tier: str = "ELITE",
    score: str = "1-0",
    bookmaker: str = "bet365",
    odds_decimal: float | None = 1.92,
    price_state: str = "VIVANT",
    executable: bool = True,
) -> dict:
    priority = {
        "fixture_id": 9001,
        "q_match": 8.4 if shadow_alert_tier == "ELITE" else 6.3,
        "q_stats": 7.8,
        "q_odds": 7.1,
        "q_live": 6.9,
        "q_competition": 7.4 if shadow_alert_tier == "ELITE" else 4.8,
        "q_noise": 2.2 if shadow_alert_tier == "ELITE" else 4.1,
        "priority_tier": "ELITE_CANDIDATE" if shadow_alert_tier == "ELITE" else "WATCHLIST_CANDIDATE",
    }
    best_projection = {
        "market_key": "TEAM_TOTAL",
        "side": "HOME_UNDER",
        "line": 1.5,
        "bookmaker": bookmaker,
        "odds_decimal": odds_decimal,
        "calibrated_probability": 0.64,
        "market_no_vig_probability": 0.54,
        "edge": 0.10,
        "expected_value": 0.23,
        "price_state": price_state,
        "executable": executable,
    }
    return {
        "source_mode": "live_shadow",
        "top_bet_eligible": top_bet_eligible,
        "shadow_alert_tier": shadow_alert_tier,
        "product": {
            "shadow_alert_tier": shadow_alert_tier,
            "top_bet_eligible": top_bet_eligible,
            "board_best": {
                "best_projection": dict(best_projection),
                "board_dominance_score": 0.58,
                "top_bet_eligible": top_bet_eligible,
                "shadow_alert_tier": shadow_alert_tier,
                "diagnostics": {"best_fixture_id": 9001},
            },
            "match_results": [
                {
                    "fixture_id": 9001,
                    "minute": 76,
                    "score": score,
                    "priority": {
                        "fixture_id": 9001,
                        "q_match": priority["q_match"],
                        "q_stats": priority["q_stats"],
                        "q_odds": priority["q_odds"],
                        "priority_tier": priority["priority_tier"],
                    },
                    "match_best": {
                        "best_projection": dict(best_projection),
                        "second_best_projection": None,
                        "dominance_score": 0.41,
                        "candidate_count": 3,
                    },
                }
            ],
        },
        "shadow_governance": {
            "shadow_alert_tier": shadow_alert_tier,
            "elite_shadow_eligible": shadow_alert_tier == "ELITE",
            "watchlist_shadow_eligible": shadow_alert_tier in {"ELITE", "WATCHLIST"},
            "best_fixture_id": 9001,
            "best_match_priority": priority,
        },
        "board_best": {
            "best_projection": dict(best_projection),
            "board_dominance_score": 0.58,
            "top_bet_eligible": top_bet_eligible,
            "shadow_alert_tier": shadow_alert_tier,
            "diagnostics": {"best_fixture_id": 9001, "best_match_priority": priority},
        },
        "match_results": [
            {
                "fixture_id": 9001,
                "minute": 76,
                "score": score,
                "priority": priority,
                "match_best": {"dominance_score": 0.41},
            }
        ],
        "debug": {
            "shadow_governance": {
                "shadow_alert_tier": shadow_alert_tier,
            }
        },
    }


def _state() -> MatchState:
    return MatchState(
        fixture_id=9001,
        competition_id=71,
        competition_name="Liga 3",
        country_name="Portugal",
        minute=76,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=0,
        home=TeamLiveStats(name="Moncao"),
        away=TeamLiveStats(name="Vianense"),
    )


def test_shadow_discord_notifier_sends_elite_message_after_two_confirmations() -> None:
    sent_payloads: list[tuple[str, dict]] = []

    def fake_sender(webhook_url: str, embed: dict) -> DiscordSendResult:
        sent_payloads.append((webhook_url, embed))
        return DiscordSendResult(ok=True, status_code=200)

    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=fake_sender,
    )
    first = notifier.notify_from_payload(
        _payload(top_bet_eligible=True, shadow_alert_tier="ELITE"),
        states_by_fixture={9001: _state()},
        top_bet_guardrail="early_cycle_not_reliable",
    )
    second = notifier.notify_from_payload(
        _payload(top_bet_eligible=True, shadow_alert_tier="ELITE"),
        states_by_fixture={9001: _state()},
        top_bet_guardrail="stable_enough_for_reading",
    )

    assert first.sent is False
    assert first.reason == "waiting_stability_confirmation"
    assert first.stability_count == 1
    assert second.sent is True
    assert second.reason == "sent"
    assert second.stability_count == 2
    assert second.alert_tier == "ELITE"
    assert len(sent_payloads) == 1
    embed = sent_payloads[0][1]
    assert embed["title"] == "\U0001f7e1 ELITE"
    assert "Moncao vs Vianense" in embed["description"]
    assert "\U0001f1f5\U0001f1f9 Liga 3 | 76' | 1-0" in embed["description"]
    assert "Moncao moins de 1.5 but" in embed["description"]
    market_field = next(field for field in embed["fields"] if field["name"] == "Marche")
    assert "Moncao moins de 1.5 but" in market_field["value"]
    assert "Ligne 1.5" in market_field["value"]
    price_field = next(field for field in embed["fields"] if field["name"] == "Prix")
    assert "bet365 @ 1.92" in price_field["value"]
    assert "manuel non confirme" in price_field["value"]
    confidence_field = next(field for field in embed["fields"] if field["name"] == "Confiance")
    assert "/100" in confidence_field["value"]
    assert "Q_match 8.400" in confidence_field["value"]
    assert "ELITE_CANDIDATE" in confidence_field["value"]
    model_field = next(field for field in embed["fields"] if field["name"] == "Modele")
    assert "p_model 0.640" in model_field["value"]
    assert "p_no_vig 0.540" in model_field["value"]
    assert "edge +0.100" in model_field["value"]
    assert "EV +0.230" in model_field["value"]
    search_field = next(field for field in embed["fields"] if field["name"] == "Ou chercher")
    assert "Total equipe" in search_field["value"]
    assert "Total equipe domicile moins de 1.5" in search_field["value"]
    assert "Fin de match | Moncao" in search_field["value"]
    assert not any(field["name"] == "Dominance" for field in embed["fields"])
    assert "aucune execution bookmaker" in embed["footer"]["text"]


def test_shadow_discord_notifier_sends_watchlist_by_default() -> None:
    sent_payloads: list[dict] = []

    def fake_sender(webhook_url: str, embed: dict) -> DiscordSendResult:
        sent_payloads.append(embed)
        return DiscordSendResult(ok=True, status_code=200)

    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=fake_sender,
    )

    first = notifier.notify_from_payload(
        _payload(top_bet_eligible=False, shadow_alert_tier="WATCHLIST"),
        states_by_fixture={9001: _state()},
    )
    second = notifier.notify_from_payload(
        _payload(top_bet_eligible=False, shadow_alert_tier="WATCHLIST"),
        states_by_fixture={9001: _state()},
    )

    assert first.sent is False
    assert first.reason == "waiting_stability_confirmation"
    assert second.sent is True
    assert second.alert_tier == "WATCHLIST"
    assert sent_payloads[0]["title"] == "\U0001f7e0 WATCHLIST"


def test_shadow_discord_notifier_human_reading_translates_supported_market_families() -> None:
    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=lambda webhook_url, embed: DiscordSendResult(ok=True, status_code=200),
    )
    state = _state()

    assert notifier._human_reading({"market_key": "OU_FT", "side": "OVER", "line": 2.5}, state) == "Plus de 2.5 buts FT"
    assert notifier._human_reading({"market_key": "OU_1H", "side": "UNDER", "line": 0.5}, state) == "Moins de 0.5 but 1re mi-temps"
    assert notifier._human_reading({"market_key": "BTTS", "side": "NO", "line": None}, state) == "Les deux equipes ne marquent pas"
    assert notifier._human_reading({"market_key": "RESULT", "side": "AWAY", "line": None}, state) == "Victoire Vianense"
    assert notifier._human_reading({"market_key": "TEAM_TOTAL", "side": "AWAY_OVER", "line": 1.5}, state) == "Vianense plus de 1.5 but"


def test_shadow_discord_notifier_builds_human_betify_search_hints() -> None:
    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=lambda webhook_url, embed: DiscordSendResult(ok=True, status_code=200),
    )
    state = _state()

    btts_hint = notifier._betify_search_hint({"market_key": "BTTS", "side": "NO", "line": None}, state)
    result_hint = notifier._betify_search_hint({"market_key": "RESULT", "side": "AWAY", "line": None}, state)
    team_total_hint = notifier._betify_search_hint({"market_key": "TEAM_TOTAL", "side": "AWAY_UNDER", "line": 1.5}, state)

    assert btts_hint == {
        "family": "BTTS",
        "probable_label": "Les deux equipes ne marquent pas (BTTS Non)",
        "period": "Fin de match",
        "team_concerned": "-",
    }
    assert result_hint == {
        "family": "1X2",
        "probable_label": "1X2 exterieur",
        "period": "Fin de match",
        "team_concerned": "Vianense",
    }
    assert team_total_hint == {
        "family": "Total equipe",
        "probable_label": "Total equipe exterieure moins de 1.5",
        "period": "Fin de match",
        "team_concerned": "Vianense",
    }


def test_shadow_discord_notifier_can_send_watchlist_when_enabled() -> None:
    sent_payloads: list[dict] = []

    def fake_sender(webhook_url: str, embed: dict) -> DiscordSendResult:
        sent_payloads.append(embed)
        return DiscordSendResult(ok=True, status_code=200)

    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=fake_sender,
        send_watchlist=True,
    )

    first = notifier.notify_from_payload(
        _payload(top_bet_eligible=False, shadow_alert_tier="WATCHLIST"),
        states_by_fixture={9001: _state()},
        top_bet_guardrail="stable_enough_for_reading",
    )
    second = notifier.notify_from_payload(
        _payload(top_bet_eligible=False, shadow_alert_tier="WATCHLIST"),
        states_by_fixture={9001: _state()},
        top_bet_guardrail="stable_enough_for_reading",
    )

    assert first.sent is False
    assert first.reason == "waiting_stability_confirmation"
    assert second.sent is True
    assert second.alert_tier == "WATCHLIST"
    assert sent_payloads[0]["title"] == "\U0001f7e0 WATCHLIST"


def test_shadow_discord_notifier_default_watchlist_mode_is_enabled() -> None:
    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=lambda webhook_url, embed: DiscordSendResult(ok=True, status_code=200),
    )

    assert notifier.send_watchlist is True


def test_shadow_discord_notifier_skips_non_publishable_projection_by_default() -> None:
    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=lambda webhook_url, embed: DiscordSendResult(ok=True, status_code=200),
    )

    result = notifier.notify_from_payload(
        _payload(top_bet_eligible=True, bookmaker="", odds_decimal=None, price_state="DEGRADE_MAIS_VIVANT", executable=False),
        states_by_fixture={9001: _state()},
    )

    assert result.sent is False
    assert result.reason == "projection_not_publishable_for_shadow_discord"


def test_shadow_discord_notifier_dedupes_same_board_best_but_resends_on_score_change() -> None:
    sent_payloads: list[dict] = []

    def fake_sender(webhook_url: str, embed: dict) -> DiscordSendResult:
        sent_payloads.append(embed)
        return DiscordSendResult(ok=True, status_code=200)

    notifier = V2ShadowDiscordNotifier(
        webhook_url="https://discord.example/webhook",
        sender=fake_sender,
    )

    first = notifier.notify_from_payload(_payload(top_bet_eligible=True, shadow_alert_tier="ELITE"), states_by_fixture={9001: _state()})
    third = notifier.notify_from_payload(
        _payload(top_bet_eligible=True, shadow_alert_tier="ELITE"),
        states_by_fixture={9001: _state()},
    )
    fourth = notifier.notify_from_payload(
        _payload(top_bet_eligible=True, shadow_alert_tier="ELITE", score="2-0"),
        states_by_fixture={
            9001: MatchState(
                fixture_id=9001,
                minute=83,
                phase="2H",
                status="2H",
                home_goals=2,
                away_goals=0,
                home=TeamLiveStats(name="Moncao"),
                away=TeamLiveStats(name="Vianense"),
            )
        },
    )

    assert first.sent is False
    assert first.reason == "waiting_stability_confirmation"
    assert third.sent is True
    assert fourth.sent is True
    duplicate = notifier.notify_from_payload(
        _payload(top_bet_eligible=True, shadow_alert_tier="ELITE", score="2-0"),
        states_by_fixture={
            9001: MatchState(
                fixture_id=9001,
                minute=84,
                phase="2H",
                status="2H",
                home_goals=2,
                away_goals=0,
                home=TeamLiveStats(name="Moncao"),
                away=TeamLiveStats(name="Vianense"),
            )
        },
    )
    assert duplicate.sent is False
    assert duplicate.reason == "duplicate_board_best"
    assert len(sent_payloads) == 2
