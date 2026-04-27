from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.settlement.ledger import (
    settlement_report_to_record,
    settle_hybrid_shadow_batch_from_jsonl,
    write_settlement_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Settle FQIS hybrid shadow accepted bets against final scores.")
    parser.add_argument("--batch-path", required=True)
    parser.add_argument("--results-path", required=True)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=Path(args.batch_path),
        results_path=Path(args.results_path),
        stake=args.stake,
    )
    record = settlement_report_to_record(report)

    if args.output_path:
        write_settlement_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_settlement_report "
            f"status={report.status} "
            f"accepted_bets={report.accepted_bet_count} "
            f"settled_bets={report.settled_bet_count} "
            f"unsettled_bets={report.unsettled_bet_count} "
            f"won={report.won_count} "
            f"lost={report.lost_count} "
            f"push={report.push_count} "
            f"total_staked={report.total_staked:.2f} "
            f"total_profit={report.total_profit:.6f} "
            f"roi={_format_optional(report.roi)}"
        )

    return 0


def _format_optional(value: float | None) -> str:
    if value is None:
        return "NA"

    return f"{value:.6f}"


if __name__ == "__main__":
    raise SystemExit(main())

    