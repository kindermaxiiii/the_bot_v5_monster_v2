from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.vnext.ops.runtime_cli import (
    EXIT_PATH_UNWRITABLE,
    EXIT_REPLAY_SOURCE_FAILED,
    EXIT_SUCCESS,
    probe_file_output_path,
    write_json_document,
)
from app.vnext.ops.reporter import build_runtime_report, format_runtime_report
from app.vnext.ops.replay import replay_runtime_export


def _write_report(path: Path, report: dict[str, object]) -> None:
    write_json_document(path, report)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_path", type=str)
    parser.add_argument("--report", type=str, default="")
    args = parser.parse_args()

    if args.report:
        try:
            probe_file_output_path(Path(args.report))
        except OSError:
            print(
                f"vnext_replay_error reason=path_unwritable path={args.report}",
                file=sys.stderr,
            )
            return EXIT_PATH_UNWRITABLE

    try:
        cycles = replay_runtime_export(Path(args.jsonl_path))
        report = build_runtime_report(cycles)
        print(format_runtime_report(report))

        if args.report:
            _write_report(Path(args.report), report)
    except FileNotFoundError:
        print(
            f"vnext_replay_error reason=replay_source_missing path={args.jsonl_path}",
            file=sys.stderr,
        )
        return EXIT_REPLAY_SOURCE_FAILED
    except ValueError:
        print(
            f"vnext_replay_error reason=replay_source_invalid path={args.jsonl_path}",
            file=sys.stderr,
        )
        return EXIT_REPLAY_SOURCE_FAILED
    except OSError:
        target = args.report or args.jsonl_path
        print(
            f"vnext_replay_error reason=path_unwritable path={target}",
            file=sys.stderr,
        )
        return EXIT_PATH_UNWRITABLE

    return EXIT_SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())
