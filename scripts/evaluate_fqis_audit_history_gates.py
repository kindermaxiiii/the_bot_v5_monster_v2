from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.reporting.audit_gates import (
    AuditHistoryGateThresholds,
    audit_history_gate_report_to_record,
    evaluate_audit_history_from_bundle_root,
    evaluate_audit_history_from_manifest_paths,
    write_audit_history_gate_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate FQIS audit history against institutional gates.")
    parser.add_argument("--bundle-root", default=None)
    parser.add_argument("--manifest-path", action="append", default=[])
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--json", action="store_true")

    parser.add_argument("--min-run-count", type=int, default=2)
    parser.add_argument("--min-latest-roi-warn", type=float, default=0.0)
    parser.add_argument("--min-latest-roi-fail", type=float, default=-0.10)
    parser.add_argument("--max-latest-brier-warn", type=float, default=0.25)
    parser.add_argument("--max-latest-brier-fail", type=float, default=0.35)
    parser.add_argument("--min-latest-clv-beat-rate-warn", type=float, default=0.50)
    parser.add_argument("--min-latest-clv-beat-rate-fail", type=float, default=0.40)
    parser.add_argument("--max-abs-latest-model-market-delta-warn", type=float, default=0.25)
    parser.add_argument("--max-abs-latest-model-market-delta-fail", type=float, default=0.50)
    parser.add_argument("--max-latest-model-only-count-warn", type=int, default=0)
    parser.add_argument("--max-latest-model-only-count-fail", type=int, default=10)
    parser.add_argument("--max-latest-clv-missing-count-warn", type=int, default=0)
    parser.add_argument("--max-latest-clv-missing-count-fail", type=int, default=5)
    parser.add_argument("--max-total-fail-count", type=int, default=0)
    parser.add_argument("--max-total-warn-count-warn", type=int, default=0)
    parser.add_argument("--max-total-warn-count-fail", type=int, default=10)
    parser.add_argument("--max-health-fail-count", type=int, default=0)

    args = parser.parse_args()

    thresholds = AuditHistoryGateThresholds(
        min_run_count=args.min_run_count,
        min_latest_roi_warn=args.min_latest_roi_warn,
        min_latest_roi_fail=args.min_latest_roi_fail,
        max_latest_brier_warn=args.max_latest_brier_warn,
        max_latest_brier_fail=args.max_latest_brier_fail,
        min_latest_clv_beat_rate_warn=args.min_latest_clv_beat_rate_warn,
        min_latest_clv_beat_rate_fail=args.min_latest_clv_beat_rate_fail,
        max_abs_latest_model_market_delta_warn=args.max_abs_latest_model_market_delta_warn,
        max_abs_latest_model_market_delta_fail=args.max_abs_latest_model_market_delta_fail,
        max_latest_model_only_count_warn=args.max_latest_model_only_count_warn,
        max_latest_model_only_count_fail=args.max_latest_model_only_count_fail,
        max_latest_clv_missing_count_warn=args.max_latest_clv_missing_count_warn,
        max_latest_clv_missing_count_fail=args.max_latest_clv_missing_count_fail,
        max_total_fail_count=args.max_total_fail_count,
        max_total_warn_count_warn=args.max_total_warn_count_warn,
        max_total_warn_count_fail=args.max_total_warn_count_fail,
        max_health_fail_count=args.max_health_fail_count,
    )

    report = _build_report(args, thresholds)
    record = audit_history_gate_report_to_record(report)

    if args.output_path:
        write_audit_history_gate_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_audit_history_gates "
            f"status={report.status} "
            f"decision={report.decision} "
            f"runs={report.run_count} "
            f"gates={report.gate_count} "
            f"pass={report.pass_count} "
            f"warn={report.warn_count} "
            f"fail={report.fail_count} "
            f"acceptable={str(report.is_acceptable).lower()}"
        )

    return 0


def _build_report(args: argparse.Namespace, thresholds: AuditHistoryGateThresholds):
    manifest_paths = tuple(Path(path) for path in args.manifest_path)

    if args.bundle_root:
        if manifest_paths:
            from app.fqis.reporting.audit_history import discover_audit_manifest_paths

            root_paths = discover_audit_manifest_paths(Path(args.bundle_root))
            return evaluate_audit_history_from_manifest_paths(
                tuple([*root_paths, *manifest_paths]),
                thresholds=thresholds,
            )

        return evaluate_audit_history_from_bundle_root(
            Path(args.bundle_root),
            thresholds=thresholds,
        )

    if manifest_paths:
        return evaluate_audit_history_from_manifest_paths(
            manifest_paths,
            thresholds=thresholds,
        )

    raise SystemExit("provide --bundle-root or at least one --manifest-path")


if __name__ == "__main__":
    raise SystemExit(main())

    