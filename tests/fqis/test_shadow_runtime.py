import json
from pathlib import Path

from app.fqis.runtime.shadow import build_demo_shadow_input, run_shadow_cycle, write_shadow_jsonl


def test_run_shadow_cycle_returns_inspectable_record() -> None:
    shadow_input = build_demo_shadow_input()

    record = run_shadow_cycle(shadow_input)

    assert record["engine"] == "fqis"
    assert record["mode"] == "shadow"
    assert record["status"] == "ok"
    assert record["thesis_count"] >= 1
    assert record["thesis_result_count"] >= 1
    assert record["accepted_bet_count"] >= 1
    assert record["best_accepted_bet"] is not None


def test_write_shadow_jsonl_writes_one_json_record(tmp_path: Path) -> None:
    shadow_input = build_demo_shadow_input()
    record = run_shadow_cycle(shadow_input)
    export_path = tmp_path / "fqis_shadow.jsonl"

    write_shadow_jsonl(record, export_path)

    lines = export_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    loaded = json.loads(lines[0])
    assert loaded["engine"] == "fqis"
    assert loaded["mode"] == "shadow"

    