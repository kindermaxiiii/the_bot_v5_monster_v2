
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.paper_preview import (  # noqa: E402
    ApiSportsPaperPreviewConfig,
    ApiSportsPaperPreviewError,
    build_api_sports_paper_preview,
    write_api_sports_paper_preview,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports paper bet preview runner.")
    parser.add_argument("--candidates", help="JSON file containing paper candidate bets.")
    parser.add_argument("--output", help="Output paper_preview.json path.")
    parser.add_argument("--sample", action="store_true", help="Use deterministic sample paper bets.")
    parser.add_argument("--max-stake-units", type=float)
    parser.add_argument("--min-bet-edge", type=float)
    parser.add_argument("--min-watch-edge", type=float)
    parser.add_argument("--max-bets", type=int)
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsPaperPreviewConfig.from_env()
        config = ApiSportsPaperPreviewConfig(
            max_stake_units=args.max_stake_units if args.max_stake_units is not None else base_config.max_stake_units,
            min_bet_edge=args.min_bet_edge if args.min_bet_edge is not None else base_config.min_bet_edge,
            min_watch_edge=args.min_watch_edge if args.min_watch_edge is not None else base_config.min_watch_edge,
            max_bets=args.max_bets if args.max_bets is not None else base_config.max_bets,
        )

        if args.output:
            preview = write_api_sports_paper_preview(
                candidates_path=args.candidates,
                output_path=args.output,
                sample=args.sample,
                config=config,
            )
        else:
            preview = build_api_sports_paper_preview(
                candidates_path=args.candidates,
                sample=args.sample,
                config=config,
            )

        print(json.dumps(preview.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
        return 1 if args.require_ready and preview.status != "READY" else 0

    except (ApiSportsPaperPreviewError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
