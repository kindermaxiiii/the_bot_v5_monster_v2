from __future__ import annotations

from app.vnext.live.models import LiveBreakEventsBlock, LiveSnapshot


def _clip(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def detect_break_events(
    current: LiveSnapshot,
    previous: LiveSnapshot | None,
) -> LiveBreakEventsBlock:
    if previous is None:
        return LiveBreakEventsBlock(
            goal_scored=False,
            home_goal_scored=False,
            away_goal_scored=False,
            equalizer_event=False,
            lead_change_event=False,
            two_goal_gap_event=False,
            red_card_occurred=False,
            home_red_card=False,
            away_red_card=False,
            event_clarity_score=0.0,
        )

    home_goal_delta = current.home_goals - previous.home_goals
    away_goal_delta = current.away_goals - previous.away_goals
    home_red_delta = current.home_red_cards - previous.home_red_cards
    away_red_delta = current.away_red_cards - previous.away_red_cards

    goal_scored = (home_goal_delta > 0) or (away_goal_delta > 0)
    home_goal_scored = home_goal_delta > 0
    away_goal_scored = away_goal_delta > 0
    red_card_occurred = (home_red_delta > 0) or (away_red_delta > 0)
    home_red_card = home_red_delta > 0
    away_red_card = away_red_delta > 0

    previous_diff = previous.home_goals - previous.away_goals
    current_diff = current.home_goals - current.away_goals
    equalizer_event = goal_scored and previous_diff != 0 and current_diff == 0
    lead_change_event = (
        goal_scored
        and previous_diff != 0
        and current_diff != 0
        and ((previous_diff > 0 and current_diff < 0) or (previous_diff < 0 and current_diff > 0))
    )
    two_goal_gap_event = abs(previous_diff) < 2 and abs(current_diff) >= 2 and goal_scored

    coherence = 1.0
    if home_goal_delta < 0 or away_goal_delta < 0 or home_red_delta < 0 or away_red_delta < 0:
        coherence = 0.2
    elif goal_scored or red_card_occurred:
        changes = sum(
            int(flag)
            for flag in (
                home_goal_scored,
                away_goal_scored,
                home_red_card,
                away_red_card,
            )
        )
        coherence = 0.95 if changes == 1 else 0.8
    else:
        coherence = 0.6

    return LiveBreakEventsBlock(
        goal_scored=goal_scored,
        home_goal_scored=home_goal_scored,
        away_goal_scored=away_goal_scored,
        equalizer_event=equalizer_event,
        lead_change_event=lead_change_event,
        two_goal_gap_event=two_goal_gap_event,
        red_card_occurred=red_card_occurred,
        home_red_card=home_red_card,
        away_red_card=away_red_card,
        event_clarity_score=_clip(coherence),
    )
