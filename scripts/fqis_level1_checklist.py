from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.operations.operator_checklist import (
    build_level1_operator_checklist,
    level1_operator_checklist_to_record,
    write_level1_operator_checklist_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate FQIS Niveau 1 operator checklist.")
    parser.add_argument("--profile", default="demo")
    parser.add_argument("--profile-path", default=None)
    parser.add_argument("--latest-status-path", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--strict-exit-code", action="store_true")
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = build_level1_operator_checklist(
        profile_name=args.profile,
        profile_path=Path(args.profile_path) if args.profile_path else None,
        latest_status_path=Path(args.latest_status_path) if args.latest_status_path else None,
    )
    record = level1_operator_checklist_to_record(report)

    if args.output_path:
        write_level1_operator_checklist_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=True, sort_keys=True))
    else:
        print(
            "fqis_level1_operator_checklist "
            f"status={report.status} "
            f"profile={report.profile_name} "
            f"readiness={report.readiness} "
            f"ready={str(report.is_ready).lower()} "
            f"items={report.item_count} "
            f"pass={report.pass_count} "
            f"warn={report.warn_count} "
            f"fail={report.fail_count} "
            f"blocking={report.blocking_count}"
        )

    if args.strict_exit_code and not report.is_ready:
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
