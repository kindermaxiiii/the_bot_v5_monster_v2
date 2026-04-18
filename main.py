from __future__ import annotations

import argparse
import logging
import time

from app.config import settings
from app.db.session import init_db
from app.jobs.runtime_cycle import RuntimeCycle

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("main")


def main() -> None:
    parser = argparse.ArgumentParser(description="THE BOT V5 — Live Football Institutional Engine")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--live", action="store_true", help="run forever using HEARTBEAT_SECONDS")
    args = parser.parse_args()

    init_db()
    runtime = RuntimeCycle()

    if args.once or not args.live:
        logger.info("running one-shot mode")
        runtime.run_once()
        return

    logger.info("running live mode heartbeat=%ss", settings.heartbeat_seconds)
    cycle = 0
    try:
        while True:
            cycle += 1
            logger.info("cycle start index=%s", cycle)
            runtime.run_once()
            logger.info("cycle end index=%s sleep=%ss", cycle, max(5, settings.heartbeat_seconds))
            time.sleep(max(5, settings.heartbeat_seconds))
    except KeyboardInterrupt:
        logger.info("shutdown requested by user")


if __name__ == "__main__":
    main()
