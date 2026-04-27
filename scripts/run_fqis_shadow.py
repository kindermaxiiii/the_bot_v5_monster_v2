from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.runtime.shadow import build_demo_shadow_input, run_shadow_cycle, write_shadow_jsonl


def main() -> int:
    parser = argparse.ArgumentParser(description="Run FQIS shadow cycle.")
    parser.add_argument(
        "--export-path",
        default=None,
        help="Path to JSONL export. Defaults to exports/fqis/run_fqis_shadow_<timestamp>.jsonl",
    )
    parser.add_argument("--min-strength", type=float, default=0.70)
    parser.add_argument("--min-confidence", type=float, default=0.70)
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--min-ev", type=float, default=0.01)
    parser.add_argument("--min-odds", type=float, default=1.50)
    parser.add_argument("--max-odds", type=float, default=2.80)

    args = parser.parse_args()

    export_path = Path(args.export_path) if args.export_path else _default_export_path()

    shadow_input = build_demo_shadow_input()
    record = run_shadow_cycle(
        shadow_input,
        min_strength=args.min_strength,
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        min_ev=args.min_ev,
        min_odds=args.min_odds,
        max_odds=args.max_odds,
    )

    write_shadow_jsonl(record, export_path)

    print(
        "fqis_shadow_complete "
        f"status={record['status']} "
        f"event_id={record['event_id']} "
        f"theses={record['thesis_count']} "
        f"accepted_bets={record['accepted_bet_count']} "
        f"export_path={export_path}"
    )

    return 0


def _default_export_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return Path("exports") / "fqis" / f"run_fqis_shadow_{timestamp}.jsonl"


if __name__ == "__main__":
    raise SystemExit(main())