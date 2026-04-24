#!/usr/bin/env python3
from pathlib import Path
import argparse
import json
from app.vnext.ops.jsonl_migration import migrate_jsonl


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str)
    parser.add_argument("--output", type=str, default="")
    args = parser.parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(json.dumps({"error": "input_not_found", "path": str(input_path)}))
        return 2
    output_path = Path(args.output) if args.output else input_path.with_suffix(input_path.suffix + ".migrated.jsonl")
    summary = migrate_jsonl(input_path, output_path)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
