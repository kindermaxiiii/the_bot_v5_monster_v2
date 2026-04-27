import json

from scripts.fqis_api_sports_quality_gate import main


def _ready_payload():
    return {
        "fixtures": [{"fixture_key": "api_sports:fixture:1"}],
        "odds_offers": [
            {
                "fixture_key": "api_sports:fixture:1",
                "provider_bookmaker_id": "8",
                "bookmaker_name": "Book",
                "provider_market_key": "api_sports:pre_match:5",
                "mapping_status": "MAPPED",
                "normalization_status": "OK",
                "selection": "OVER",
                "line": 2.5,
                "decimal_odds": 1.91,
            }
        ],
    }


def test_quality_gate_script_missing_input_returns_safe_failure(tmp_path, capsys):
    code = main(["--input", str(tmp_path / "missing.json")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "FAILED"


def test_quality_gate_script_writes_report(tmp_path):
    input_path = tmp_path / "normalized.json"
    output_path = tmp_path / "quality_report.json"
    input_path.write_text(json.dumps(_ready_payload()), encoding="utf-8")

    code = main(["--input", str(input_path), "--output", str(output_path)])

    assert code == 0
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["status"] == "PASS"
    assert report["ready"] is True