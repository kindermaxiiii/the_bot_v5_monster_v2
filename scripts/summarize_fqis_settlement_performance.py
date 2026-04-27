from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.performance.metrics import (
    build_performance_report_from_json,
    performance_report_to_record,
    write_performance_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize FQIS settlement performance metrics.")
    parser.add_argument("--settlement-path", required=True)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--bucket-size", type=float, default=0.10)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = build_performance_report_from_json(
        Path(args.settlement_path),
        bucket_size=args.bucket_size,
    )
    record = performance_report_to_record(report)

    if args.output_path:
        write_performance_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_performance_report "
            f"status={report.status} "
            f"bets={report.bet_count} "
            f"settled={report.settled_bet_count} "
            f"graded={report.graded_bet_count} "
            f"won={report.won_count} "
            f"lost={report.lost_count} "
            f"push={report.push_count} "
            f"unsettled={report.unsettled_count} "
            f"hit_rate={_format_optional(report.hit_rate)} "
            f"roi={_format_optional(report.roi)} "
            f"brier={_format_optional(report.brier_score)} "
            f"avg_odds={_format_optional(report.average_odds)} "
            f"avg_p_real={_format_optional(report.average_p_real)} "
            f"calibration_buckets={len(report.calibration_buckets)}"
        )

    return 0


def _format_optional(value: float | None) -> str:
    if value is None:
        return "NA"

    return f"{value:.6f}"


if __name__ == "__main__":
    raise SystemExit(main())

    