from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.probability.hybrid import HybridProbabilityConfig
from app.fqis.runtime.hybrid_shadow import (
    build_demo_hybrid_shadow_input,
    run_hybrid_shadow_cycle,
    write_hybrid_shadow_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run FQIS hybrid model/market shadow cycle.")
    parser.add_argument("--export-path", default=None)
    parser.add_argument("--model-weight", type=float, default=0.70)
    parser.add_argument("--market-weight", type=float, default=0.30)
    parser.add_argument("--min-strength", type=float, default=0.70)
    parser.add_argument("--min-confidence", type=float, default=0.70)
    parser.add_argument("--min-edge", type=float, default=0.01)
    parser.add_argument("--min-ev", type=float, default=0.0)
    parser.add_argument("--min-odds", type=float, default=1.50)
    parser.add_argument("--max-odds", type=float, default=2.80)

    args = parser.parse_args()

    shadow_input = build_demo_hybrid_shadow_input()
    hybrid_config = HybridProbabilityConfig(
        model_weight=args.model_weight,
        market_weight=args.market_weight,
    )

    outcome = run_hybrid_shadow_cycle(
        shadow_input,
        hybrid_config=hybrid_config,
        min_strength=args.min_strength,
        min_confidence=args.min_confidence,
        min_edge=args.min_edge,
        min_ev=args.min_ev,
        min_odds=args.min_odds,
        max_odds=args.max_odds,
    )

    export_path = Path(args.export_path) if args.export_path else _default_export_path()
    write_hybrid_shadow_jsonl(outcome, export_path)

    print(
        "fqis_hybrid_shadow_complete "
        f"status={outcome.status} "
        f"event_id={outcome.event_id} "
        f"theses={outcome.thesis_count} "
        f"accepted_bets={outcome.accepted_bet_count} "
        f"hybrid_probabilities={outcome.hybrid_probability_count} "
        f"hybrid_count={outcome.hybrid_count} "
        f"model_only_count={outcome.model_only_count} "
        f"export_path={export_path}"
    )

    return 0


def _default_export_path() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    return Path("exports") / "fqis" / f"run_fqis_hybrid_shadow_{stamp}.jsonl"


if __name__ == "__main__":
    raise SystemExit(main())

    