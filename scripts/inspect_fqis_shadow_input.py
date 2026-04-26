from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.input_inspector import inspect_shadow_input_file


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect FQIS shadow JSONL input.")
    parser.add_argument("--input-path", required=True, help="Path to FQIS shadow JSONL input.")
    parser.add_argument("--json", action="store_true", help="Print full JSON inspection report.")

    args = parser.parse_args()

    report = inspect_shadow_input_file(Path(args.input_path))

    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_input_inspection "
            f"status={report['status']} "
            f"matches={report['match_count']} "
            f"offers={report['total_offer_count']} "
            f"thesis_keys={','.join(report['thesis_keys'])} "
            f"duplicates={report['has_duplicates']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())