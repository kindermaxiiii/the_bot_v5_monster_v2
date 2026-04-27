from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.input_inspector import inspect_shadow_input_file
from app.fqis.runtime.provider_adapter import build_provider_snapshot_from_jsonl


def main() -> int:
    parser = argparse.ArgumentParser(description="Build FQIS match-level snapshot from provider rows JSONL.")
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--output-path", required=True)

    args = parser.parse_args()

    output_path = Path(args.output_path)

    result = build_provider_snapshot_from_jsonl(
        Path(args.source_path),
        output_path,
    )
    inspection = inspect_shadow_input_file(output_path)

    print(
        "fqis_provider_snapshot_built "
        f"status={inspection['status']} "
        f"records={result.record_count} "
        f"matches={inspection['match_count']} "
        f"offers={inspection['total_offer_count']} "
        f"event_ids={','.join(str(event_id) for event_id in result.event_ids)} "
        f"output_path={result.output_path}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

    