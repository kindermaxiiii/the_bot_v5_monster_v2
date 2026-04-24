from __future__ import annotations

import json
from pathlib import Path
from typing import List

from app.vnext.ops.jsonl_migration import CANONICAL_FIXTURE_AUDIT_DEFAULTS


def diagnose_jsonl(path: Path, max_samples: int = 5) -> dict:
    """
    Scan JSONL file and identify rows where any fixture_audit is missing canonical keys.
    Returns a summary dict:
      - total_rows
      - rows_with_missing_audits
      - sample_row_indices (list up to max_samples)
    """
    text = path.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    total = 0
    rows_with_missing = 0
    samples: List[int] = []

    canonical_keys = set(CANONICAL_FIXTURE_AUDIT_DEFAULTS.keys())

    for idx, line in enumerate(lines, start=1):
        total += 1
        try:
            obj = json.loads(line)
        except Exception:
            # corrupted JSON line - count as problem
            rows_with_missing += 1
            if len(samples) < max_samples:
                samples.append(idx)
            continue

        audits = obj.get("fixture_audits")
        if not isinstance(audits, list):
            # missing audits entirely is a problem
            rows_with_missing += 1
            if len(samples) < max_samples:
                samples.append(idx)
            continue

        # check each audit
        problem = False
        for audit in audits:
            if not isinstance(audit, dict):
                problem = True
                break
            audit_keys = set(audit.keys())
            if not canonical_keys.issubset(audit_keys):
                problem = True
                break

        if problem:
            rows_with_missing += 1
            if len(samples) < max_samples:
                samples.append(idx)

    return {
        "total_rows": total,
        "rows_with_missing_audits": rows_with_missing,
        "sample_row_indices": samples,
    }
