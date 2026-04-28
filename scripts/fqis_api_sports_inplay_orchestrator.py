from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
TAIL_CHARS = 4000
ORCHESTRATOR_MODE = "INPLAY_ORCHESTRATOR"


@dataclass(frozen=True)
class OrchestratorStep:
    name: str
    command: tuple[str, ...]
    return_code: int
    stdout_tail: str
    stderr_tail: str

    @property
    def ok(self) -> bool:
        return self.return_code == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "return_code": self.return_code,
            "command": list(self.command),
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports in-play orchestration runner.")
    parser.add_argument("--output-dir", default="data/pipeline/api_sports", help="Output directory.")
    parser.add_argument("--api-key", help="API-Sports API key. Prefer env/.env in normal use.")
    parser.add_argument("--base-url", default="https://v3.football.api-sports.io")
    parser.add_argument("--fixture", type=int, help="Optional focused fixture id for live odds.")
    parser.add_argument("--max-candidates", type=int, default=100)
    parser.add_argument("--min-bookmakers", type=int, default=1)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir)
    diagnostics_dir = output_dir / "diagnostics"
    output_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "fixtures": output_dir / "inplay_fixtures.json",
        "candidates": output_dir / "paper_candidates.json",
        "snapshot_json": output_dir / "live_market_snapshot.json",
        "snapshot_md": output_dir / "live_market_snapshot.md",
        "raw_odds": diagnostics_dir / "odds_live_raw.json",
        "diagnostics_json": output_dir / "live_odds_coverage_diagnostics.json",
        "diagnostics_md": output_dir / "live_odds_coverage_diagnostics.md",
        "summary_json": output_dir / "inplay_orchestrator_summary.json",
        "summary_md": output_dir / "inplay_orchestrator_summary.md",
    }

    steps: list[OrchestratorStep] = []

    fixtures_args = [
        "--output",
        str(paths["fixtures"]),
        "--require-ready",
    ]
    if args.sample:
        fixtures_args.append("--sample")
    else:
        fixtures_args.extend(["--base-url", args.base_url])
        if args.api_key:
            fixtures_args.extend(["--api-key", args.api_key])

    steps.append(_run_script("inplay_fixtures", "fqis_api_sports_inplay_fixtures.py", fixtures_args))
    _echo_step(steps[-1])

    if steps[-1].ok:
        candidates_args = [
            "--output",
            str(paths["candidates"]),
            "--require-ready",
            "--max-candidates",
            str(args.max_candidates),
        ]
        if args.sample:
            candidates_args.append("--sample")
        else:
            candidates_args.extend(["--base-url", args.base_url])
            if args.fixture is not None:
                candidates_args.extend(["--fixture", str(args.fixture)])
            if args.api_key:
                candidates_args.extend(["--api-key", args.api_key])

        steps.append(
            _run_script(
                "inplay_live_odds_candidates",
                "fqis_api_sports_inplay_live_odds_candidates.py",
                candidates_args,
            )
        )
        _echo_step(steps[-1])

    if steps and steps[-1].ok:
        snapshot_args = [
            "--input",
            str(paths["candidates"]),
            "--output",
            str(paths["snapshot_json"]),
            "--markdown",
            str(paths["snapshot_md"]),
            "--require-ready",
            "--min-bookmakers",
            str(args.min_bookmakers),
        ]

        steps.append(
            _run_script(
                "live_market_snapshot",
                "fqis_api_sports_live_market_snapshot.py",
                snapshot_args,
            )
        )
        _echo_step(steps[-1])

    if steps and steps[-1].ok:
        diagnostics_args = [
            "--output",
            str(paths["diagnostics_json"]),
            "--markdown",
            str(paths["diagnostics_md"]),
            "--require-ready",
        ]

        if args.sample:
            diagnostics_args.append("--sample")
        else:
            diagnostics_args.extend(
                [
                    "--fixtures",
                    str(paths["fixtures"]),
                    "--candidates",
                    str(paths["candidates"]),
                    "--raw-output",
                    str(paths["raw_odds"]),
                    "--base-url",
                    args.base_url,
                ]
            )
            if args.fixture is not None:
                diagnostics_args.extend(["--fixture", str(args.fixture)])
            if args.api_key:
                diagnostics_args.extend(["--api-key", args.api_key])

        steps.append(
            _run_script(
                "live_odds_coverage_diagnostics",
                "fqis_api_sports_live_odds_coverage_diagnostics.py",
                diagnostics_args,
            )
        )
        _echo_step(steps[-1])

    summary = build_inplay_orchestrator_summary(paths=paths, steps=steps)
    paths["summary_json"].write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    paths["summary_md"].write_text(render_inplay_orchestrator_markdown(summary), encoding="utf-8")

    print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))
    print(render_inplay_orchestrator_markdown(summary))

    ready = bool(summary["ready"])
    return 1 if args.require_ready and not ready else 0


def build_inplay_orchestrator_summary(
    *,
    paths: dict[str, Path],
    steps: list[OrchestratorStep],
) -> dict[str, Any]:
    payloads = {
        "fixtures": _load_json_if_exists(paths["fixtures"]),
        "candidates": _load_json_if_exists(paths["candidates"]),
        "snapshot": _load_json_if_exists(paths["snapshot_json"]),
        "diagnostics": _load_json_if_exists(paths["diagnostics_json"]),
    }

    failed_steps = [step for step in steps if not step.ok]
    ready = not failed_steps and len(steps) == 4

    warnings: list[str] = [
        "INPLAY_ORCHESTRATION",
        "OBSERVATION_ONLY",
        "NO_REAL_STAKING",
        "NO_MODEL_EDGE_VALIDATION",
    ]

    for payload in payloads.values():
        if isinstance(payload, dict):
            for warning in payload.get("warnings", []):
                if warning not in warnings:
                    warnings.append(str(warning))

    if failed_steps and "ORCHESTRATOR_STEP_FAILED" not in warnings:
        warnings.append("ORCHESTRATOR_STEP_FAILED")

    metrics = {
        "fixtures_total": _summary_int(payloads["fixtures"], "fixtures_total"),
        "rejected_fixtures_total": _summary_int(payloads["fixtures"], "rejected_total"),
        "candidates_total": _summary_int(payloads["candidates"], "candidates_total"),
        "candidate_rejections_total": _summary_int(payloads["candidates"], "rejected_total"),
        "snapshot_rows_total": _summary_int(payloads["snapshot"], "rows_total"),
        "diagnostics_live_fixtures_total": _summary_int(payloads["diagnostics"], "live_fixtures_total"),
        "diagnostics_live_odds_fixtures_total": _summary_int(payloads["diagnostics"], "live_odds_fixtures_total"),
        "diagnostics_matched_fixture_odds_total": _summary_int(payloads["diagnostics"], "matched_fixture_odds_total"),
        "diagnostics_candidates_total": _summary_int(payloads["diagnostics"], "candidates_total"),
    }

    return {
        "status": "READY" if ready else "FAILED",
        "ready": ready,
        "mode": ORCHESTRATOR_MODE,
        "real_staking_enabled": False,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "paths": {key: str(value) for key, value in paths.items()},
        "steps": [step.to_dict() for step in steps],
        "metrics": metrics,
        "warnings": warnings,
        "errors": [f"{step.name} failed with return code {step.return_code}" for step in failed_steps],
        "summary": {
            "steps_total": len(steps),
            "failed_steps_total": len(failed_steps),
            "warnings_total": len(warnings),
            "errors_total": len(failed_steps),
            **metrics,
        },
    }


def render_inplay_orchestrator_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# FQIS API-Sports In-Play Orchestrator",
        "",
        "## Summary",
        "",
        f"- Status: **{summary['status']}**",
        f"- Mode: **{summary['mode']}**",
        f"- Real staking enabled: **{str(summary['real_staking_enabled']).lower()}**",
        f"- Steps: **{summary['summary']['steps_total']}**",
        f"- Failed steps: **{summary['summary']['failed_steps_total']}**",
        f"- Fixtures: **{summary['metrics']['fixtures_total']}**",
        f"- Candidates: **{summary['metrics']['candidates_total']}**",
        f"- Snapshot rows: **{summary['metrics']['snapshot_rows_total']}**",
        f"- Live odds fixtures: **{summary['metrics']['diagnostics_live_odds_fixtures_total']}**",
        f"- Matched fixture odds: **{summary['metrics']['diagnostics_matched_fixture_odds_total']}**",
        f"- Generated at UTC: `{summary['generated_at_utc']}`",
        "",
        "> OBSERVATION ONLY. This orchestrator is not a betting signal and never enables real staking.",
        "",
        "## Steps",
        "",
        "| Step | Status | Return Code |",
        "|---|---:|---:|",
    ]

    for step in summary["steps"]:
        lines.append(
            "| {name} | {status} | {return_code} |".format(
                name=step["name"],
                status="OK" if step["ok"] else "FAILED",
                return_code=step["return_code"],
            )
        )

    lines.extend(
        [
            "",
            "## Metrics",
            "",
            "| Metric | Value |",
            "|---|---:|",
        ]
    )

    for key, value in summary["metrics"].items():
        lines.append(f"| `{key}` | {value} |")

    lines.extend(["", "## Warnings", ""])

    for warning in summary["warnings"]:
        lines.append(f"- `{warning}`")

    lines.extend(
        [
            "",
            "## Output Files",
            "",
            "| Artifact | Path |",
            "|---|---|",
        ]
    )

    for key, value in summary["paths"].items():
        lines.append(f"| `{key}` | `{value}` |")

    lines.extend(
        [
            "",
            "## Operator Interpretation",
            "",
            "- If candidates are zero but diagnostics is READY, the pipeline is operational and the live odds feed currently has no tradable supported opportunities.",
            "- If live fixtures are positive and live odds fixtures are zero, API-Sports has live match state but no live betting feed exposure at that instant.",
            "- If snapshot rows are positive, they remain observation-only until independent model probabilities and edge gates are attached.",
            "- Do not place real stakes from this orchestrator.",
            "",
        ]
    )

    return "\n".join(lines)


def _run_script(name: str, script_name: str, args: list[str]) -> OrchestratorStep:
    script = REPO_ROOT / "scripts" / script_name
    command = (sys.executable, str(script), *args)

    completed = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )

    return OrchestratorStep(
        name=name,
        command=command,
        return_code=completed.returncode,
        stdout_tail=(completed.stdout or "")[-TAIL_CHARS:],
        stderr_tail=(completed.stderr or "")[-TAIL_CHARS:],
    )


def _echo_step(step: OrchestratorStep) -> None:
    print(f"\n===== {step.name}: return_code={step.return_code} =====")

    if step.stdout_tail:
        print(step.stdout_tail)

    if step.stderr_tail:
        print(step.stderr_tail, file=sys.stderr)


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return None


def _summary_int(payload: Any, key: str) -> int:
    if not isinstance(payload, dict):
        return 0

    summary = payload.get("summary")
    if isinstance(summary, dict):
        value = summary.get(key)
        if isinstance(value, int):
            return value

    metrics = payload.get("metrics")
    if isinstance(metrics, dict):
        value = metrics.get(key)
        if isinstance(value, int):
            return value

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
