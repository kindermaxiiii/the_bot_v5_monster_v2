from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

# Canonical fixture audit keys and their default values:
CANONICAL_FIXTURE_AUDIT_DEFAULTS = {
    "fixture_id": None,
    "match_label": None,
    "competition_label": None,
    "governed_public_status": None,
    "publish_status": None,
    "template_key": None,
    "bookmaker_id": None,
    "line": None,
    "odds_decimal": None,
    "governance_refusal_summary": [],
    "execution_refusal_summary": [],
    "candidate_not_selectable_reason": None,
    "translated_candidate_count": None,
    "selectable_candidate_count": None,
    "best_candidate_family": None,
    "best_candidate_exists": None,
    "best_candidate_selectable": None,
    "best_candidate_blockers": [],
    "distinct_candidate_blockers_summary": [],
    "execution_candidate_count": None,
    "execution_selectable_count": None,
    "attempted_template_keys": [],
    "offer_present_template_keys": [],
    "missing_offer_template_keys": [],
    "blocked_execution_reasons_summary": [],
    "final_execution_refusal_reason": None,
    "publishability_score": None,
    "template_binding_score": None,
    "bookmaker_diversity_score": None,
    "price_integrity_score": None,
    "retrievability_score": None,
    "source": "runtime_fixture_audit.v1",
}


def _normalize_audit_entry(audit: dict) -> dict:
    """
    Ensure audit dict contains all canonical keys. Does not change values
    already present. Returns a new dict.
    """
    out: dict = {}
    # copy existing fields first (preserve original types)
    for k, v in audit.items():
        out[k] = v

    # fill missing canonical keys with defaults
    for key, default in CANONICAL_FIXTURE_AUDIT_DEFAULTS.items():
        if key not in out:
            # for mutable defaults, copy them to avoid shared lists
            if isinstance(default, list):
                out[key] = list(default)
            else:
                out[key] = default

    # ensure refusal summaries and blockers are lists
    for list_key in (
        "governance_refusal_summary",
        "execution_refusal_summary",
        "best_candidate_blockers",
        "distinct_candidate_blockers_summary",
        "attempted_template_keys",
        "offer_present_template_keys",
        "missing_offer_template_keys",
        "blocked_execution_reasons_summary",
    ):
        if out.get(list_key) is None:
            out[list_key] = []
        else:
            # if it's not a list, coerce to list
            if not isinstance(out[list_key], list):
                out[list_key] = list(out[list_key])

    return out


def migrate_jsonl(input_path: Path, output_path: Path) -> dict:
    """
    Read input_path (JSONL), write output_path (JSONL) with canonicalized fixture_audits.
    Returns a summary dict with counts.
    """
    input_text = input_path.read_text(encoding="utf-8")
    lines = [ln for ln in input_text.splitlines() if ln.strip()]
    total_rows = 0
    updated_rows = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as out:
        for line in lines:
            total_rows += 1
            row = json.loads(line)
            modified = False
            audits = row.get("fixture_audits")
            if isinstance(audits, list):
                new_audits = []
                for audit in audits:
                    if isinstance(audit, dict):
                        normalized = _normalize_audit_entry(audit)
                        new_audits.append(normalized)
                        # check whether anything changed (presence of missing keys)
                        # If audit has fewer keys than canonical set, mark modified
                        if set(normalized.keys()) != set(audit.keys()):
                            modified = True
                    else:
                        # non-dict entries: keep as-is
                        new_audits.append(audit)
                        modified = True
                row["fixture_audits"] = new_audits

            if modified:
                updated_rows += 1

            out.write(json.dumps(row, ensure_ascii=True) + "\n")

    return {"total_rows": total_rows, "updated_rows": updated_rows}
