from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.client import ApiSportsClient
from app.fqis.integrations.api_sports.config import ApiSportsConfig, ApiSportsConfigError
from app.fqis.integrations.api_sports.market_discovery import (
    ApiSportsMarketSource,
    build_market_discovery_report,
    discover_all_markets,
    discover_markets,
)


def _load_dotenv_if_available() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def main() -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports market discovery probe.")
    parser.add_argument("--source", choices=("all", "pre_match", "live"), default="all")
    parser.add_argument("--search", default=None)
    parser.add_argument("--include-unmapped", action="store_true")
    args = parser.parse_args()

    _load_dotenv_if_available()

    try:
        config = ApiSportsConfig.from_env(require_key=True)
    except ApiSportsConfigError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2

    client = ApiSportsClient(config)

    try:
        if args.source == "all":
            candidates = discover_all_markets(
                client,
                search=args.search,
                include_unmapped=args.include_unmapped,
            )
        else:
            candidates = discover_markets(
                client,
                source=ApiSportsMarketSource(args.source),
                search=args.search,
                include_unmapped=args.include_unmapped,
            )

        report: dict[str, Any] = build_market_discovery_report(candidates)
        report["config"] = config.redacted()
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0

    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "mode": "shadow_only_market_discovery",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "config": config.redacted(),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())