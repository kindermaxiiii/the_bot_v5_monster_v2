from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.vnext_cycle_diagnostics import diagnose_vnext_cycle_export_for_fqis


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose vnext cycle-level JSONL export for FQIS.")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = diagnose_vnext_cycle_export_for_fqis(Path(args.source_path))

    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        gaps = report["fqis_gap_assessment"]
        collections = report["collection_counts"]
        coverage = report["fixture_id_coverage"]

        print(
            "fqis_vnext_cycle_diagnostics "
            f"status={report['status']} "
            f"cycles={report['cycles_read']} "
            f"fixture_audits={collections['fixture_audits']} "
            f"publication_records={collections['publication_records']} "
            f"payloads={collections['payloads']} "
            f"refusal_summaries={collections['refusal_summaries']} "
            f"unique_fixture_ids={coverage['all_nested_unique_fixture_ids']} "
            f"can_reconstruct_price_candidates={gaps['can_reconstruct_price_candidates']} "
            f"can_build_full_fqis_input={gaps['can_build_full_fqis_input']} "
            f"next={gaps['recommended_next_step']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

    