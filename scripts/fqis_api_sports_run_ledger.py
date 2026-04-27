
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.run_ledger import (  # noqa: E402
    ApiSportsRunLedgerError,
    append_run_ledger_entry,
    build_run_ledger_entry,
    default_run_ledger_path,
    summarize_run_ledger,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports pipeline run ledger.")
    parser.add_argument("--manifest", help="Path to pipeline_manifest.json to record.")
    parser.add_argument("--ledger", default=str(default_run_ledger_path()))
    parser.add_argument("--summary", action="store_true", help="Print ledger summary after recording.")
    parser.add_argument("--allow-duplicates", action="store_true")
    args = parser.parse_args(argv)

    ledger_path = Path(args.ledger)

    try:
        output: dict[str, object]

        if args.manifest:
            manifest_path = Path(args.manifest)
            if not manifest_path.exists():
                print(
                    json.dumps(
                        {"status": "FAILED", "reason": f"Manifest path does not exist: {manifest_path}"},
                        indent=2,
                        ensure_ascii=False,
                    )
                )
                return 2

            entry = build_run_ledger_entry(manifest_path)
            appended = append_run_ledger_entry(
                ledger_path,
                entry,
                dedupe=not args.allow_duplicates,
            )
            output = {
                "status": "RECORDED" if appended else "SKIPPED_DUPLICATE",
                "ledger_path": str(ledger_path),
                "entry": entry.to_dict(),
            }
        else:
            output = {
                "status": "SUMMARY",
                "ledger_path": str(ledger_path),
            }

        if args.summary or not args.manifest:
            output["summary"] = summarize_run_ledger(ledger_path).to_dict()

        print(json.dumps(output, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    except ApiSportsRunLedgerError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
