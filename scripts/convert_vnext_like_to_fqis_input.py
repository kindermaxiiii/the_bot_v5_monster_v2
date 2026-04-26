from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.vnext_like_converter import convert_vnext_like_export_to_fqis_input


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert vnext-like JSONL export to FQIS shadow input JSONL.")
    parser.add_argument("--source-path", required=True, help="Path to vnext-like source JSONL.")
    parser.add_argument("--output-path", required=True, help="Path to output FQIS shadow input JSONL.")

    args = parser.parse_args()

    source_path = Path(args.source_path)
    output_path = Path(args.output_path)

    conversion = convert_vnext_like_export_to_fqis_input(source_path, output_path)
    inspection = inspect_shadow_input_file(output_path)

    print(
        "fqis_input_conversion_complete "
        f"status={inspection['status']} "
        f"source_path={conversion.source_path} "
        f"output_path={conversion.output_path} "
        f"rows={conversion.row_count} "
        f"matches={inspection['match_count']} "
        f"offers={inspection['total_offer_count']} "
        f"thesis_keys={','.join(inspection['thesis_keys'])} "
        f"duplicates={inspection['has_duplicates']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

    