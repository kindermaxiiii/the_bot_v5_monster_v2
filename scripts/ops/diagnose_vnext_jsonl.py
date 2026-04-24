#!/usr/bin/env python3
from pathlib import Path
import argparse
import json
from app.vnext.ops.jsonl_diagnose import diagnose_jsonl


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str)
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        print(json.dumps({"error": "input_not_found", "path": str(input_path)}))
        return 2
    summary = diagnose_jsonl(input_path, max_samples=args.samples)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
