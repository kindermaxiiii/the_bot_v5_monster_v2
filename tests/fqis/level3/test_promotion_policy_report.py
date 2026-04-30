import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_promotion_policy_report.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_promotion_policy_report.json"

BUCKET = "STRICT_OVER_RESEARCH"
MARKET = "Total Goals FT"
GROUP = f"{BUCKET}||{MARKET}"


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


def write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def good_ranker(path: Path, *, red_flags: list[str] | None = None) -> Path:
    return write_json(
        path,
        {
            "status": "READY",
            "ranked_alerts": [
                {
                    "research_bucket": BUCKET,
                    "market": MARKET,
                    "data_tier": "STRICT_EVENTS_PLUS_STATS",
                    "bucket_policy_action": "KEEP_RESEARCH_BUCKET",
                    "red_flags": red_flags or [],
                }
            ],
        },
    )


def good_clv(path: Path, *, sample: int = 200) -> Path:
    return write_json(
        path,
        {
            "status": "READY",
            "by_research_bucket_market": {
                GROUP: {
                    "total_records": sample,
                    "eligible_records": sample,
                    "favorable_move_rate": 0.60,
                    "favorable_move_count": int(sample * 0.6),
                    "unfavorable_move_count": sample - int(sample * 0.6),
                    "flat_move_count": 0,
                    "odds_delta_mean": -0.08,
                }
            },
            "can_execute_real_bets": False,
            "can_enable_live_staking": False,
            "can_mutate_ledger": False,
            "promotion_allowed": False,
        },
    )


def good_calibration(path: Path, *, settled: int = 200) -> Path:
    return write_json(
        path,
        {
            "status": "READY",
            "by_research_bucket_market": {
                GROUP: {
                    "eligible_settled_rows": settled,
                    "brier_score": 0.20,
                    "absolute_calibration_error": 0.05,
                    "empirical_hit_rate": 0.56,
                    "avg_predicted_probability": 0.55,
                }
            },
            "can_execute_real_bets": False,
            "can_enable_live_staking": False,
            "can_mutate_ledger": False,
            "promotion_allowed": False,
        },
    )


def good_bucket_policy(path: Path, *, roi: float = 0.05) -> Path:
    return write_json(
        path,
        {
            "status": "READY",
            "buckets": {
                BUCKET: {
                    "action": "KEEP_RESEARCH_BUCKET",
                    "settled": 200,
                    "roi": roi,
                }
            },
        },
    )


def run_with_inputs(tmp_path: Path, *, ranker: Path, clv: Path, calibration: Path, bucket_policy: Path) -> dict:
    output_json = tmp_path / "promotion.json"
    output_md = tmp_path / "promotion.md"
    run_script(
        "--ranker-path",
        str(ranker),
        "--clv-path",
        str(clv),
        "--calibration-path",
        str(calibration),
        "--bucket-policy-path",
        str(bucket_policy),
        "--output-json",
        str(output_json),
        "--output-md",
        str(output_md),
    )
    return json.loads(output_json.read_text(encoding="utf-8"))


def test_promotion_policy_compiles_runs_outputs_safe_report_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    run_script()

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] in {"READY", "REVIEW"}
    assert payload["promotion_allowed"] is False
    assert payload["final_verdict"] == "NO_PROMOTION_KEEP_RESEARCH"
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False


def test_promotion_policy_hard_red_flags_block_promotion(tmp_path: Path):
    payload = run_with_inputs(
        tmp_path,
        ranker=good_ranker(tmp_path / "ranker.json", red_flags=["KILL_OR_QUARANTINE_BUCKET"]),
        clv=good_clv(tmp_path / "clv.json"),
        calibration=good_calibration(tmp_path / "calibration.json"),
        bucket_policy=good_bucket_policy(tmp_path / "bucket_policy.json"),
    )

    evaluation = payload["evaluations"][0]
    assert payload["promotion_allowed"] is False
    assert evaluation["promotion_allowed"] is False
    assert evaluation["recommended_state"] == "QUARANTINE"
    assert "HARD_RED_FLAGS_PRESENT" in evaluation["blockers"]


def test_promotion_policy_missing_clv_blocks_promotion(tmp_path: Path):
    payload = run_with_inputs(
        tmp_path,
        ranker=good_ranker(tmp_path / "ranker.json"),
        clv=tmp_path / "missing_clv.json",
        calibration=good_calibration(tmp_path / "calibration.json"),
        bucket_policy=good_bucket_policy(tmp_path / "bucket_policy.json"),
    )

    evaluation = payload["evaluations"][0]
    assert payload["promotion_allowed"] is False
    assert "PROXY_CLV_MISSING_OR_NOT_READY" in evaluation["blockers"]


def test_promotion_policy_missing_calibration_blocks_promotion(tmp_path: Path):
    payload = run_with_inputs(
        tmp_path,
        ranker=good_ranker(tmp_path / "ranker.json"),
        clv=good_clv(tmp_path / "clv.json"),
        calibration=tmp_path / "missing_calibration.json",
        bucket_policy=good_bucket_policy(tmp_path / "bucket_policy.json"),
    )

    evaluation = payload["evaluations"][0]
    assert payload["promotion_allowed"] is False
    assert "CALIBRATION_MISSING_OR_NOT_READY" in evaluation["blockers"]


def test_promotion_policy_small_sample_blocks_promotion(tmp_path: Path):
    payload = run_with_inputs(
        tmp_path,
        ranker=good_ranker(tmp_path / "ranker.json"),
        clv=good_clv(tmp_path / "clv.json", sample=5),
        calibration=good_calibration(tmp_path / "calibration.json", settled=5),
        bucket_policy=good_bucket_policy(tmp_path / "bucket_policy.json"),
    )

    evaluation = payload["evaluations"][0]
    assert payload["promotion_allowed"] is False
    assert "SAMPLE_SIZE_TOO_SMALL" in evaluation["blockers"]
    assert "SETTLED_SAMPLE_SIZE_TOO_SMALL" in evaluation["blockers"]
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
