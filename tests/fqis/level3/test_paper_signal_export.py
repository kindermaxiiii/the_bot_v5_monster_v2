import hashlib
import json
import py_compile
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "fqis_paper_signal_export.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_signal_export.json"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_paper_signal_export.md"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def nested_keys(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield key
            yield from nested_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from nested_keys(child)


def test_paper_signal_export_compiles_runs_and_is_paper_only():
    before = sha256(LEDGER)
    py_compile.compile(str(SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert sha256(LEDGER) == before
    assert OUT_JSON.exists()
    assert OUT_MD.exists()

    payload = json.loads(OUT_JSON.read_text(encoding="utf-8"))
    assert payload["status"] == "READY"
    assert payload["safety"]["can_execute_real_bets"] is False
    assert payload["safety"]["can_enable_live_staking"] is False
    assert payload["safety"]["can_mutate_ledger"] is False
    assert payload["safety"]["promotion_allowed"] is False
    assert payload["safety"]["live_staking_allowed"] is False

    signals = payload.get("signals") or []
    forbidden_stake_fields = {"stake", "stake_size", "unit_stake", "stake_units", "amount"}
    for signal in signals:
        assert signal["paper_only"] is True
        assert signal["paper_action"] in {
            "PAPER_PRODUCTION_SIM_ONLY",
            "PAPER_RESEARCH_WATCH",
            "PAPER_REJECTED_NO_ACTION",
        }
        assert signal["raw_safety_flags"]["can_execute_real_bets"] is False
        assert forbidden_stake_fields.isdisjoint(set(nested_keys(signal)))
