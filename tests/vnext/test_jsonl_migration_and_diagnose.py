from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.jsonl_migration import migrate_jsonl
from app.vnext.ops.jsonl_diagnose import diagnose_jsonl


def _write_minimal_old_row(path: Path) -> None:
    # An older-style JSONL row with minimal fixture_audits fields
    row = {
        "cycle_id": 1,
        "timestamp_utc": datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc).isoformat(),
        "fixture_count_seen": 1,
        "pipeline_publish_count": 0,
        "fixture_audits": [
            {
                "fixture_id": 999,
                "match_label": "Lions vs Falcons",
                "competition_label": "Premier Test",
                "governed_public_status": "WATCHLIST",
                "publish_status": "PUBLISH"
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


def test_migrate_and_diagnose(tmp_path: Path) -> None:
    src = tmp_path / f"old_export_{uuid4().hex}.jsonl"
    _write_minimal_old_row(src)
    # Diagnose before migration
    pre = diagnose_jsonl(src)
    assert pre["total_rows"] == 1
    assert pre["rows_with_missing_audits"] == 1

    dst = tmp_path / f"migrated_{uuid4().hex}.jsonl"
    summary = migrate_jsonl(src, dst)
    assert summary["total_rows"] == 1
    # After migration the migrated file should contain canonical keys
    post = diagnose_jsonl(dst)
    assert post["total_rows"] == 1
    assert post["rows_with_missing_audits"] == 0
