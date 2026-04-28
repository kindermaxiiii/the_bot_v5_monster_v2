
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.live_market_snapshot import (  # noqa: E402
    ApiSportsLiveMarketSnapshotConfig,
    ApiSportsLiveMarketSnapshotError,
    render_api_sports_live_market_snapshot_markdown,
    write_api_sports_live_market_snapshot,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports live market snapshot builder.")
    parser.add_argument("--input", required=True, help="Input paper_candidates.json path.")
    parser.add_argument("--output", required=True, help="Output live_market_snapshot.json path.")
    parser.add_argument("--markdown", help="Optional markdown output path.")
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--min-bookmakers", type=int)
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsLiveMarketSnapshotConfig.from_env()
        config = ApiSportsLiveMarketSnapshotConfig(
            max_rows=args.max_rows if args.max_rows is not None else base_config.max_rows,
            min_bookmakers=args.min_bookmakers if args.min_bookmakers is not None else base_config.min_bookmakers,
        )

        result = write_api_sports_live_market_snapshot(
            source_path=args.input,
            output_path=args.output,
            markdown_path=args.markdown,
            config=config,
        )

        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))

        if args.markdown:
            print(render_api_sports_live_market_snapshot_markdown(result))

        return 1 if args.require_ready and not result.ready else 0

    except ApiSportsLiveMarketSnapshotError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
