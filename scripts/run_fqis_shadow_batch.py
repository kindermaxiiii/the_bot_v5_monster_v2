from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.batch_shadow import (
    build_demo_shadow_inputs,
    run_shadow_batch,
    write_shadow_batch_jsonl,
)
from app.fqis.runtime.input_loader import load_shadow_inputs_from_jsonl


def main() -> int:
    parser = argparse.ArgumentParser(description="Run FQIS shadow batch cycle.")
    parser.add_argument(
        "--input-path",
        default=None,
        help="Optional JSONL input path. If omitted, demo inputs are used.",
    )
    parser.add_argument(
        "--export-path",
        default=None,
        help="Path to JSONL export. Defaults to exports/fqis/run_fqis_shadow_batch_<timestamp>.jsonl",
    )
    parser.add_argument("--min-strength", type=float, default=0.70)
    parser.add_argument("--min-confidence", type=float, default=0.70)
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--min-ev", type=float, default=0.01)
    parser.add_argument("--min-odds", type=float, default=1.50)
    parser.add_argument("--max-odds", type=float, default=2.80)

    args = parser.parse_args()

    export_path = Path(args.export_path) if args.export_path else _default_export_path()

    if args.input_path:
        shadow_inputs = load_shadow_inputs_from_jsonl(Path(args.input_path))
        source = "jsonl"
    else:
        shadow_inputs = build_demo_shadow_inputs()
        source = "demo"

    batch_result = run_shadow_batch(
        shadow_inputs,
        min_strength=args.min_strength,
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        min_ev=args.min_ev,
        min_odds=args.min_odds,
        max_odds=args.max_odds,
    )

    write_shadow_batch_jsonl(batch_result.records, export_path)

    summary = batch_result.summary

    print(
        "fqis_shadow_batch_complete "
        f"status={summary['status']} "
        f"source={source} "
        f"matches={summary['match_count']} "
        f"accepted_matches={summary['accepted_match_count']} "
        f"rejected_matches={summary['rejected_match_count']} "
        f"accepted_bets={summary['total_accepted_bet_count']} "
        f"acceptance_rate={summary['acceptance_rate']:.2%} "
        f"export_path={export_path}"
    )

    return 0


def _default_export_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Path("exports") / "fqis" / f"run_fqis_shadow_batch_{timestamp}.jsonl"


if __name__ == "__main__":
    raise SystemExit(main())