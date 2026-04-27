from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.reporting.audit_gates import AuditHistoryGateThresholds
from app.fqis.reporting.production_readiness import (
    evaluate_production_readiness_from_bundle_root,
    evaluate_production_readiness_from_manifest_paths,
    production_readiness_report_to_record,
    write_production_readiness_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate FQIS production-shadow readiness.")
    parser.add_argument("--bundle-root", default=None)
    parser.add_argument("--manifest-path", action="append", default=[])
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--json", action="store_true")

    parser.add_argument("--min-run-count", type=int, default=2)
    parser.add_argument("--relaxed", action="store_true")

    args = parser.parse_args()

    thresholds = _thresholds(args)
    report = _build_report(args, thresholds)
    record = production_readiness_report_to_record(report)

    if args.output_path:
        write_production_readiness_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_production_readiness "
            f"status={report.status} "
            f"readiness={report.readiness_status} "
            f"level={report.readiness_level} "
            f"gate_decision={report.gate_decision} "
            f"runs={report.run_count} "
            f"checklist={report.checklist_count} "
            f"blockers={report.blocker_count} "
            f"warnings={report.warning_count} "
            f"failures={report.failure_count} "
            f"go={str(report.is_go).lower()}"
        )

    return 0


def _build_report(args: argparse.Namespace, thresholds: AuditHistoryGateThresholds):
    manifest_paths = tuple(Path(path) for path in args.manifest_path)

    if args.bundle_root:
        if manifest_paths:
            from app.fqis.reporting.audit_history import discover_audit_manifest_paths

            root_paths = discover_audit_manifest_paths(Path(args.bundle_root))
            return evaluate_production_readiness_from_manifest_paths(
                tuple([*root_paths, *manifest_paths]),
                thresholds=thresholds,
            )

        return evaluate_production_readiness_from_bundle_root(
            Path(args.bundle_root),
            thresholds=thresholds,
        )

    if manifest_paths:
        return evaluate_production_readiness_from_manifest_paths(
            manifest_paths,
            thresholds=thresholds,
        )

    raise SystemExit("provide --bundle-root or at least one --manifest-path")


def _thresholds(args: argparse.Namespace) -> AuditHistoryGateThresholds:
    if args.relaxed:
        return AuditHistoryGateThresholds(
            min_run_count=args.min_run_count,
            max_total_warn_count_warn=99,
            max_total_warn_count_fail=100,
            max_abs_latest_model_market_delta_warn=1.0,
            max_abs_latest_model_market_delta_fail=2.0,
            max_latest_model_only_count_warn=99,
            max_latest_model_only_count_fail=100,
        )

    return AuditHistoryGateThresholds(min_run_count=args.min_run_count)


if __name__ == "__main__":
    raise SystemExit(main())

    