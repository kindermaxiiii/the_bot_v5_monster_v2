from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.client import ApiSportsClient
from app.fqis.integrations.api_sports.config import ApiSportsConfig, ApiSportsConfigError
from app.fqis.integrations.api_sports.snapshots import ApiSportsSnapshotCollector, ApiSportsSnapshotWriter


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
    parser = argparse.ArgumentParser(description="FQIS API-Sports fixtures/odds raw snapshot collector.")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--timezone", default="Europe/Paris")
    parser.add_argument("--skip-odds", action="store_true")
    parser.add_argument("--include-live", action="store_true")
    parser.add_argument("--max-odds-pages", type=int, default=5)
    parser.add_argument(
        "--snapshot-dir",
        default=os.getenv("APISPORTS_SNAPSHOT_DIR", "data/snapshots/api_sports"),
    )
    args = parser.parse_args()

    _load_dotenv_if_available()

    try:
        config = ApiSportsConfig.from_env(require_key=True)
    except ApiSportsConfigError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2

    report: dict[str, Any] = {
        "status": "STARTED",
        "mode": "shadow_only_fixtures_odds_snapshot",
        "provider": "api_sports_api_football",
        "config": config.redacted(),
        "request": {
            "date": args.date,
            "timezone": args.timezone,
            "include_odds": not args.skip_odds,
            "include_live": args.include_live,
            "max_odds_pages": args.max_odds_pages,
            "snapshot_dir": args.snapshot_dir,
        },
    }

    try:
        client = ApiSportsClient(config)
        writer = ApiSportsSnapshotWriter(Path(args.snapshot_dir))
        collector = ApiSportsSnapshotCollector(client=client, writer=writer)
        manifest = collector.collect_date(
            date=args.date,
            timezone=args.timezone,
            include_odds=not args.skip_odds,
            include_live=args.include_live,
            max_odds_pages=args.max_odds_pages,
        )
        report["status"] = "COMPLETED"
        report["manifest"] = manifest.to_dict()
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:
        report["status"] = "FAILED"
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
