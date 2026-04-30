import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_level3_stats_coverage_diagnostic.py"

SAFETY_FLAGS = [
    "can_execute_real_bets",
    "can_enable_live_staking",
    "can_mutate_ledger",
    "live_staking_allowed",
    "promotion_allowed",
]


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def event_payload(team_name: str = "Home") -> dict:
    return {
        "get": "fixtures/events",
        "errors": [],
        "response": [
            {
                "time": {"elapsed": 12, "extra": None},
                "team": {"id": 1, "name": team_name},
                "type": "Card",
                "detail": "Yellow Card",
            }
        ],
        "results": 1,
    }


def stats_payload(home: str, away: str) -> dict:
    return {
        "get": "fixtures/statistics",
        "errors": [],
        "response": [
            {
                "team": {"id": 1, "name": home},
                "statistics": [{"type": "Shots on Goal", "value": 3}],
            },
            {
                "team": {"id": 2, "name": away},
                "statistics": [{"type": "Shots on Goal", "value": 2}],
            },
        ],
        "results": 2,
    }


def empty_stats_payload() -> dict:
    return {"get": "fixtures/statistics", "errors": [], "response": [], "results": 0}


def fixture(fid: int, home: str, away: str, minute: int) -> dict:
    return {
        "fixture_id": fid,
        "home_team": home,
        "away_team": away,
        "match": f"{home} vs {away}",
        "elapsed": minute,
        "live": True,
    }


def test_level3_stats_coverage_diagnostic_controlled_fixtures(tmp_path):
    py_compile.compile(str(SCRIPT), doraise=True)

    decision_dir = tmp_path / "decision_bridge_live"
    level3_dir = tmp_path / "level3_live_state"
    output_dir = tmp_path / "orchestrator"
    run_dir = decision_dir / "run_20260430_120000"

    write_json(
        run_dir / "inplay_fixtures.json",
        {
            "fixtures": [
                fixture(101, "Raw Missing FC", "Endpoint Gap", 24),
                fixture(102, "Parser FC", "Drop United", 39),
                fixture(103, "Events FC", "No Stats Town", 52),
                fixture(104, "Trade Ready FC", "Stats United", 61),
            ]
        },
    )
    write_json(
        level3_dir / "latest_level3_live_state.json",
        {
            "fixtures": [
                {
                    "fixture_id": "101",
                    "match": "Raw Missing FC vs Endpoint Gap",
                    "minute": 24,
                    "events_available": True,
                    "stats_available": False,
                    "state_ready": True,
                    "trade_ready": False,
                },
                {
                    "fixture_id": "102",
                    "match": "Parser FC vs Drop United",
                    "minute": 39,
                    "events_available": True,
                    "stats_available": False,
                    "state_ready": True,
                    "trade_ready": False,
                },
                {
                    "fixture_id": "103",
                    "match": "Events FC vs No Stats Town",
                    "minute": 52,
                    "events_available": True,
                    "stats_available": False,
                    "state_ready": True,
                    "trade_ready": False,
                },
                {
                    "fixture_id": "104",
                    "match": "Trade Ready FC vs Stats United",
                    "minute": 61,
                    "events_available": True,
                    "stats_available": True,
                    "state_ready": True,
                    "trade_ready": True,
                },
            ],
        },
    )

    for fid, team in [(101, "Raw Missing FC"), (102, "Parser FC"), (103, "Events FC"), (104, "Trade Ready FC")]:
        write_json(level3_dir / "raw" / f"fixture_{fid}_events.json", event_payload(team))

    write_json(level3_dir / "raw" / "fixture_102_statistics.json", stats_payload("Parser FC", "Drop United"))
    write_json(level3_dir / "raw" / "fixture_103_statistics.json", empty_stats_payload())
    write_json(level3_dir / "raw" / "fixture_104_statistics.json", stats_payload("Trade Ready FC", "Stats United"))

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--decision-dir",
            str(decision_dir),
            "--level3-dir",
            str(level3_dir),
            "--output-dir",
            str(output_dir),
        ],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    payload = json.loads((output_dir / "latest_level3_stats_coverage_diagnostic.json").read_text(encoding="utf-8"))
    by_fixture = {row["fixture_id"]: row for row in payload["fixtures"]}
    summary = payload["summary"]

    assert summary["fixtures_seen"] == 4
    assert summary["events_available"] == 4
    assert summary["raw_stats_available"] == 2
    assert summary["parsed_stats_available"] == 1
    assert summary["events_only_no_stats"] == 2
    assert summary["stats_parser_empty"] == 1
    assert summary["stats_endpoint_missing"] == 1

    assert by_fixture["101"]["provider_stats_status"] == "STATS_ENDPOINT_MISSING"
    assert by_fixture["101"]["reason"] == "STATS_ENDPOINT_MISSING"
    assert by_fixture["102"]["has_stats_raw"] is True
    assert by_fixture["102"]["has_stats_parsed"] is False
    assert by_fixture["102"]["parser_status"] == "PARSER_DROPPED_RAW_STATS"
    assert by_fixture["102"]["reason"] == "STATS_RESPONSE_PRESENT_PARSER_EMPTY"
    assert by_fixture["103"]["has_events"] is True
    assert by_fixture["103"]["provider_stats_status"] == "STATS_EMPTY_RESPONSE"
    assert by_fixture["103"]["reason"] == "PROVIDER_EVENTS_ONLY_NO_STATS"
    assert by_fixture["104"]["has_stats_raw"] is True
    assert by_fixture["104"]["has_stats_parsed"] is True
    assert by_fixture["104"]["trade_ready_eligible"] is True
    assert by_fixture["104"]["reason"] == "STATS_AVAILABLE_TRADE_READY_ELIGIBLE"

    for flag in SAFETY_FLAGS:
        assert payload[flag] is False
        assert payload["safety"][flag] is False
    assert payload["read"]["ledger_mutation_performed"] is False
    assert payload["read"]["bookmaker_execution_performed"] is False

    markdown = (output_dir / "latest_level3_stats_coverage_diagnostic.md").read_text(encoding="utf-8")
    assert "Level 3 Stats Coverage Diagnostic" in markdown
    assert "STATS_RESPONSE_PRESENT_PARSER_EMPTY" in markdown
