import json
from pathlib import Path

from app.fqis.runtime.batch_shadow import (
    build_demo_shadow_inputs,
    run_shadow_batch,
    write_shadow_batch_jsonl,
)


def test_build_demo_shadow_inputs_returns_multiple_matches() -> None:
    shadow_inputs = build_demo_shadow_inputs()

    assert len(shadow_inputs) >= 3
    assert len({item.live_match_row["event_id"] for item in shadow_inputs}) == len(shadow_inputs)


def test_run_shadow_batch_returns_records_and_summary() -> None:
    shadow_inputs = build_demo_shadow_inputs()

    batch_result = run_shadow_batch(shadow_inputs)

    assert len(batch_result.records) == len(shadow_inputs)
    assert batch_result.summary["engine"] == "fqis"
    assert batch_result.summary["mode"] == "shadow_batch"
    assert batch_result.summary["status"] == "ok"
    assert batch_result.summary["match_count"] == len(shadow_inputs)
    assert batch_result.summary["accepted_match_count"] >= 1
    assert batch_result.summary["total_thesis_count"] >= len(shadow_inputs)


def test_write_shadow_batch_jsonl_writes_one_line_per_match(tmp_path: Path) -> None:
    shadow_inputs = build_demo_shadow_inputs()
    batch_result = run_shadow_batch(shadow_inputs)
    export_path = tmp_path / "fqis_shadow_batch.jsonl"

    write_shadow_batch_jsonl(batch_result.records, export_path)

    lines = export_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == len(shadow_inputs)

    first = json.loads(lines[0])
    assert first["engine"] == "fqis"
    assert first["mode"] == "shadow"
    assert "batch_index" in first

    