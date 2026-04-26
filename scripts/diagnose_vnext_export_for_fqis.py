from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.vnext_export_diagnostics import diagnose_vnext_export_for_fqis


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose vnext JSONL export for FQIS conversion readiness.")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")

    args = parser.parse_args()

    report = diagnose_vnext_export_for_fqis(Path(args.source_path))

    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        readiness = report["conversion_readiness_counts"]
        print(
            "fqis_vnext_export_diagnostics "
            f"status={report['status']} "
            f"rows={report['rows_read']} "
            f"valid_json={report['rows_valid_json']} "
            f"invalid_json={report['rows_invalid_json']} "
            f"probably_convertible={report['probably_convertible_rows']} "
            f"readiness={readiness}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())