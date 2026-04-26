from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.match_snapshot import (
    build_demo_match_snapshot_records,
    write_match_snapshot_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build FQIS match-level snapshot JSONL.")
    parser.add_argument(
        "--output-path",
        default=None,
        help="Output path. Defaults to exports/fqis/fqis_match_snapshot_<timestamp>.jsonl",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Build demo snapshot records.",
    )

    args = parser.parse_args()

    if not args.demo:
        raise SystemExit("Only --demo mode is available for now.")

    output_path = Path(args.output_path) if args.output_path else _default_output_path()

    records = build_demo_match_snapshot_records()
    result = write_match_snapshot_jsonl(records, output_path)
    inspection = inspect_shadow_input_file(output_path)

    print(
        "fqis_match_snapshot_built "
        f"status={inspection['status']} "
        f"records={result.record_count} "
        f"matches={inspection['match_count']} "
        f"offers={inspection['total_offer_count']} "
        f"event_ids={','.join(str(event_id) for event_id in result.event_ids)} "
        f"output_path={result.output_path}"
    )

    return 0


def _default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Path("exports") / "fqis" / f"fqis_match_snapshot_{timestamp}.jsonl"


if __name__ == "__main__":
    raise SystemExit(main())

    