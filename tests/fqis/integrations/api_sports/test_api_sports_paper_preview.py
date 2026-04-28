
import json

from app.fqis.integrations.api_sports.paper_preview import (
    ApiSportsPaperPreviewConfig,
    build_api_sports_paper_preview,
    write_api_sports_paper_preview,
)


def _candidates():
    return [
        {
            "match": "Team A vs Team B",
            "market": "Total Goals",
            "selection": "Over 2.5",
            "odds": 1.92,
            "model_probability": 0.568,
            "bookmaker": "SampleBook",
        },
        {
            "match": "Team C vs Team D",
            "market": "Draw No Bet",
            "selection": "Team C DNB",
            "odds": 1.89,
            "model_probability": 0.535,
            "bookmaker": "SampleBook",
        },
        {
            "match": "Team E vs Team F",
            "market": "1X2",
            "selection": "Team E Win",
            "odds": 1.42,
            "model_probability": 0.68,
            "bookmaker": "SampleBook",
        },
    ]


def test_paper_preview_classifies_bets_watchlist_and_rejected():
    preview = build_api_sports_paper_preview(
        candidates=_candidates(),
        config=ApiSportsPaperPreviewConfig(
            max_stake_units=0.05,
            min_bet_edge=0.05,
            min_watch_edge=0.01,
            max_bets=5,
        ),
    )

    assert preview.status == "READY"
    assert preview.mode == "PAPER_ONLY"
    assert preview.real_staking_enabled is False
    assert preview.max_stake_units == 0.05
    assert len(preview.bets) == 1
    assert len(preview.watchlist) == 1
    assert len(preview.rejected) == 1
    assert preview.bets[0].stake_units <= 0.05
    assert preview.bets[0].decision == "PAPER_BET"


def test_paper_preview_empty_is_ready():
    preview = build_api_sports_paper_preview()

    assert preview.status == "READY"
    assert preview.bets == ()
    assert preview.watchlist == ()
    assert preview.rejected == ()
    assert preview.errors == ()


def test_write_paper_preview_creates_output_file(tmp_path):
    output_path = tmp_path / "paper_preview.json"

    preview = write_api_sports_paper_preview(
        candidates=_candidates(),
        output_path=output_path,
    )

    assert preview.status == "READY"
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["mode"] == "PAPER_ONLY"
    assert payload["real_staking_enabled"] is False
    assert payload["max_stake_units"] == 0.05
    assert set(payload) >= {"bets", "watchlist", "rejected", "errors"}


def test_paper_preview_loads_candidates_from_file(tmp_path):
    candidates_path = tmp_path / "candidates.json"
    candidates_path.write_text(json.dumps({"candidates": _candidates()}), encoding="utf-8")

    preview = build_api_sports_paper_preview(candidates_path=candidates_path)

    assert preview.status == "READY"
    assert len(preview.bets) == 1
