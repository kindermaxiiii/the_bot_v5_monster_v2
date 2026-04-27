from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.vnext_export_converter import convert_vnext_export_to_fqis_input


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert vnext export JSONL to FQIS shadow input JSONL.")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--json", action="store_true", help="Print full conversion report as JSON.")

    args = parser.parse_args()

    report = convert_vnext_export_to_fqis_input(
        Path(args.source_path),
        Path(args.output_path),
    )

    inspection = None
    if report.rows_converted > 0:
        inspection = inspect_shadow_input_file(Path(args.output_path))

    payload = {
        "status": "ok",
        "source_path": str(report.source_path),
        "output_path": str(report.output_path),
        "rows_read": report.rows_read,
        "rows_converted": report.rows_converted,
        "rows_rejected": report.rows_rejected,
        "rejection_reasons": report.rejection_reasons,
        "inspection": inspection,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_vnext_export_conversion_complete "
            f"status=ok "
            f"rows_read={report.rows_read} "
            f"rows_converted={report.rows_converted} "
            f"rows_rejected={report.rows_rejected} "
            f"output_path={report.output_path}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

    