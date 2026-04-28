
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.paper_report import (  # noqa: E402
    ApiSportsPaperReportConfig,
    ApiSportsPaperReportError,
    build_api_sports_paper_report,
    write_api_sports_paper_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports paper report renderer.")
    parser.add_argument("--preview", help="Path to paper_preview.json.")
    parser.add_argument("--output", help="Output paper_report.md path.")
    parser.add_argument("--title")
    parser.add_argument("--no-watchlist", action="store_true")
    parser.add_argument("--no-rejected", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsPaperReportConfig.from_env()
        config = ApiSportsPaperReportConfig(
            title=args.title or base_config.title,
            include_watchlist=False if args.no_watchlist else base_config.include_watchlist,
            include_rejected=False if args.no_rejected else base_config.include_rejected,
        )

        if args.output:
            report = write_api_sports_paper_report(
                preview_path=args.preview,
                output_path=args.output,
                config=config,
            )
        else:
            report = build_api_sports_paper_report(
                preview_path=args.preview,
                config=config,
            )

        print(report.markdown)
        return 1 if args.require_ready and report.status != "READY" else 0

    except ApiSportsPaperReportError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
