import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_calibration_audit_report.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_calibration_report.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def test_calibration_audit_compiles_runs_outputs_safe_report_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script()

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] in {"READY", "REVIEW", "EMPTY"}
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["promotion_allowed"] is False


def test_calibration_audit_fixture_computes_bins_brier_and_log_loss(tmp_path: Path):
    input_path = tmp_path / "settlement.json"
    output_json = tmp_path / "calibration.json"
    output_md = tmp_path / "calibration.md"
    input_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "settlement_status": "SETTLED",
                        "result_status": "WIN",
                        "calibrated_probability": 0.80,
                        "market_key": "OU_FT",
                        "selection": "Over 1.5",
                        "research_bucket": "STRICT_OVER_RESEARCH",
                        "minute": 55,
                    },
                    {
                        "settlement_status": "SETTLED",
                        "result_status": "LOSS",
                        "calibrated_probability": 0.60,
                        "market_key": "OU_FT",
                        "selection": "Over 1.5",
                        "research_bucket": "STRICT_OVER_RESEARCH",
                        "minute": 56,
                    },
                    {
                        "settlement_status": "SETTLED",
                        "result_status": "WIN",
                        "calibrated_probability": 0.50,
                        "market_key": "BTTS",
                        "selection": "Yes",
                        "research_bucket": "STRICT_MARKET_RESEARCH",
                        "minute": 44,
                    },
                    {
                        "settlement_status": "PENDING",
                        "result_status": "",
                        "calibrated_probability": 0.90,
                        "market_key": "OU_FT",
                        "selection": "Over 1.5",
                        "research_bucket": "STRICT_OVER_RESEARCH",
                        "minute": 57,
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    run_script(
        "--input-path",
        str(input_path),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["total_rows"] == 4
    assert payload["eligible_settled_rows"] == 3
    assert payload["brier_score"] == 0.216667
    assert len(payload["calibration_bins"]) == 10
    assert payload["by_market"]["OU_FT"]["eligible_settled_rows"] == 2
    assert payload["by_research_bucket"]["STRICT_OVER_RESEARCH"]["empirical_hit_rate"] == 0.5
    assert payload["by_research_bucket_market_selection"]["STRICT_OVER_RESEARCH||OU_FT||Over 1.5"][
        "eligible_settled_rows"
    ] == 2
    assert "INSUFFICIENT_SAMPLE:3<100" in payload["warning_flags"]


def test_calibration_audit_missing_outcome_schema_is_review_not_fake_calibration(tmp_path: Path):
    input_path = tmp_path / "settlement_missing_schema.json"
    output_json = tmp_path / "calibration.json"
    output_md = tmp_path / "calibration.md"
    input_path.write_text(json.dumps({"rows": [{"calibrated_probability": 0.7}]}), encoding="utf-8")

    run_script(
        "--input-path",
        str(input_path),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    )

    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["status"] == "REVIEW"
    assert payload["eligible_settled_rows"] == 0
    assert payload["brier_score"] is None
    assert "result_status" in payload["missing_columns"]
    assert "settlement_status" in payload["missing_columns"]
    assert "MISSING_REQUIRED_CALIBRATION_SCHEMA" in payload["warning_flags"]
