from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.performance.clv import (
    build_clv_report_from_json,
    clv_report_to_record,
    write_clv_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize FQIS CLV against closing odds.")
    parser.add_argument("--settlement-path", required=True)
    parser.add_argument("--closing-path", required=True)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = build_clv_report_from_json(
        settlement_path=Path(args.settlement_path),
        closing_path=Path(args.closing_path),
    )
    record = clv_report_to_record(report)

    if args.output_path:
        write_clv_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_clv_report "
            f"status={report.status} "
            f"bets={report.bet_count} "
            f"priced={report.priced_count} "
            f"missing={report.missing_count} "
            f"beat={report.beat_count} "
            f"not_beat={report.not_beat_count} "
            f"beat_rate={_format_optional(report.beat_rate)} "
            f"avg_clv_odds={_format_optional(report.average_clv_odds_delta)} "
            f"avg_clv_pct={_format_optional(report.average_clv_percent)} "
            f"avg_clv_implied_delta={_format_optional(report.average_clv_implied_probability_delta)}"
        )

    return 0


def _format_optional(value: float | None) -> str:
    if value is None:
        return "NA"

    return f"{value:.6f}"


if __name__ == "__main__":
    raise SystemExit(main())

    