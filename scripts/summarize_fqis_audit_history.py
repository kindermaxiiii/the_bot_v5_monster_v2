from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.reporting.audit_history import (
    audit_history_report_to_record,
    build_audit_history_report_from_bundle_root,
    build_audit_history_report_from_manifest_paths,
    write_audit_history_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize multi-run FQIS audit bundle history.")
    parser.add_argument("--bundle-root", default=None)
    parser.add_argument("--manifest-path", action="append", default=[])
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = _build_report(args)
    record = audit_history_report_to_record(report)

    if args.output_path:
        write_audit_history_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        roi = record["metric_summaries"]["roi"]
        brier = record["metric_summaries"]["brier_score"]
        clv = record["metric_summaries"]["clv_beat_rate"]

        print(
            "fqis_audit_history_report "
            f"status={report.status} "
            f"runs={report.run_count} "
            f"health_counts={report.health_counts} "
            f"flags={report.total_flag_count} "
            f"fails={report.total_fail_count} "
            f"warns={report.total_warn_count} "
            f"infos={report.total_info_count} "
            f"roi_latest={_format_optional(roi['latest'])} "
            f"roi_change={_format_optional(roi['change'])} "
            f"brier_latest={_format_optional(brier['latest'])} "
            f"clv_beat_rate_latest={_format_optional(clv['latest'])} "
            f"total_size_bytes={report.total_size_bytes}"
        )

    return 0


def _build_report(args: argparse.Namespace):
    manifest_paths = tuple(Path(path) for path in args.manifest_path)

    if args.bundle_root:
        root_report = build_audit_history_report_from_bundle_root(Path(args.bundle_root))

        if not manifest_paths:
            return root_report

        combined_paths = tuple(Path(path) for path in root_report.source_paths) + manifest_paths
        return build_audit_history_report_from_manifest_paths(combined_paths)

    if manifest_paths:
        return build_audit_history_report_from_manifest_paths(manifest_paths)

    raise SystemExit("provide --bundle-root or at least one --manifest-path")


def _format_optional(value: object) -> str:
    if value is None:
        return "NA"

    return f"{float(value):.6f}"


if __name__ == "__main__":
    raise SystemExit(main())

    