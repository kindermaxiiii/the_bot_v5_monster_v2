from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True, frozen=True)
class AuditHistoryMetricSummary:
    field_name: str
    count: int
    mean: float | None
    minimum: float | None
    maximum: float | None
    latest: float | None
    previous: float | None
    change: float | None


@dataclass(slots=True, frozen=True)
class AuditHistoryRun:
    run_id: str
    generated_at_utc: str
    bundle_dir: str
    health_status: str
    file_count: int
    total_size_bytes: int
    flag_count: int
    fail_count: int
    warn_count: int
    info_count: int
    headline_metrics: dict[str, Any]
    audit_flag_codes: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class AuditHistoryReport:
    status: str
    source_paths: tuple[str, ...]
    run_count: int
    health_counts: dict[str, int]
    total_file_count: int
    total_size_bytes: int
    total_flag_count: int
    total_fail_count: int
    total_warn_count: int
    total_info_count: int
    flag_code_counts: dict[str, int]
    metric_summaries: dict[str, AuditHistoryMetricSummary]
    runs: tuple[AuditHistoryRun, ...]

    @property
    def has_runs(self) -> bool:
        return self.run_count > 0


DEFAULT_HISTORY_METRICS = (
    "accepted_bet_count",
    "settled_bet_count",
    "unsettled_bet_count",
    "roi",
    "hit_rate",
    "brier_score",
    "clv_beat_rate",
    "average_clv_percent",
    "average_clv_odds_delta",
    "p_hybrid_mean",
    "delta_model_market_mean",
    "total_profit",
    "model_only_count",
)


def discover_audit_manifest_paths(bundle_root: Path) -> tuple[Path, ...]:
    if not bundle_root.exists():
        raise FileNotFoundError(f"audit bundle root not found: {bundle_root}")

    paths = tuple(sorted(bundle_root.glob("*/manifest.json")))

    if not paths:
        raise ValueError(f"no audit bundle manifest found under: {bundle_root}")

    return paths


def load_audit_bundle_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"audit bundle manifest not found: {path}")

    try:
        record = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid audit bundle manifest JSON: {path}: {exc}") from exc

    if not isinstance(record, dict):
        raise ValueError(f"audit bundle manifest must be a JSON object: {path}")

    if record.get("source") != "fqis_audit_bundle_manifest":
        raise ValueError(f"not an FQIS audit bundle manifest: {path}")

    return record


def build_audit_history_report_from_bundle_root(
    bundle_root: Path,
    *,
    metric_names: tuple[str, ...] = DEFAULT_HISTORY_METRICS,
) -> AuditHistoryReport:
    manifest_paths = discover_audit_manifest_paths(bundle_root)

    return build_audit_history_report_from_manifest_paths(
        manifest_paths,
        metric_names=metric_names,
    )


def build_audit_history_report_from_manifest_paths(
    manifest_paths: tuple[Path, ...],
    *,
    metric_names: tuple[str, ...] = DEFAULT_HISTORY_METRICS,
) -> AuditHistoryReport:
    if not manifest_paths:
        raise ValueError("manifest_paths must not be empty")

    unique_paths = tuple(dict.fromkeys(Path(path) for path in manifest_paths))
    manifest_records = tuple(load_audit_bundle_manifest(path) for path in unique_paths)

    runs = tuple(
        sorted(
            (
                _manifest_to_history_run(path, record)
                for path, record in zip(unique_paths, manifest_records)
            ),
            key=lambda run: (run.generated_at_utc, run.run_id),
        )
    )

    health_counts = _count_values(run.health_status for run in runs)
    flag_code_counts = _count_values(
        code
        for run in runs
        for code in run.audit_flag_codes
    )

    metric_summaries = {
        metric_name: _summarize_metric(runs, metric_name)
        for metric_name in metric_names
    }

    return AuditHistoryReport(
        status="ok",
        source_paths=tuple(str(path) for path in unique_paths),
        run_count=len(runs),
        health_counts=health_counts,
        total_file_count=sum(run.file_count for run in runs),
        total_size_bytes=sum(run.total_size_bytes for run in runs),
        total_flag_count=sum(run.flag_count for run in runs),
        total_fail_count=sum(run.fail_count for run in runs),
        total_warn_count=sum(run.warn_count for run in runs),
        total_info_count=sum(run.info_count for run in runs),
        flag_code_counts=flag_code_counts,
        metric_summaries=metric_summaries,
        runs=runs,
    )


def audit_history_report_to_record(report: AuditHistoryReport) -> dict[str, Any]:
    return {
        "status": report.status,
        "source": "fqis_audit_history_report",
        "source_paths": list(report.source_paths),
        "run_count": report.run_count,
        "health_counts": dict(report.health_counts),
        "total_file_count": report.total_file_count,
        "total_size_bytes": report.total_size_bytes,
        "total_flag_count": report.total_flag_count,
        "total_fail_count": report.total_fail_count,
        "total_warn_count": report.total_warn_count,
        "total_info_count": report.total_info_count,
        "flag_code_counts": dict(report.flag_code_counts),
        "metric_summaries": {
            field_name: _metric_summary_to_record(summary)
            for field_name, summary in report.metric_summaries.items()
        },
        "runs": [
            _run_to_record(run)
            for run in report.runs
        ],
    }


def write_audit_history_report_json(report: AuditHistoryReport, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            audit_history_report_to_record(report),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _manifest_to_history_run(path: Path, record: dict[str, Any]) -> AuditHistoryRun:
    run_audit_record = _load_run_audit_record_from_manifest(path, record)
    headline_metrics = record.get("headline_metrics") or {}

    if not isinstance(headline_metrics, dict):
        headline_metrics = {}

    audit_flags = run_audit_record.get("audit_flags", []) if run_audit_record else []
    audit_flag_codes = tuple(
        str(flag.get("code"))
        for flag in audit_flags
        if isinstance(flag, dict) and flag.get("code")
    )

    return AuditHistoryRun(
        run_id=str(record.get("run_id", "UNKNOWN")),
        generated_at_utc=str(record.get("generated_at_utc", "")),
        bundle_dir=str(record.get("bundle_dir", "")),
        health_status=str(record.get("health_status", "UNKNOWN")),
        file_count=int(record.get("file_count") or 0),
        total_size_bytes=int(record.get("total_size_bytes") or 0),
        flag_count=int(record.get("flag_count") or 0),
        fail_count=int(record.get("fail_count") or 0),
        warn_count=int(record.get("warn_count") or 0),
        info_count=int(record.get("info_count") or 0),
        headline_metrics=dict(headline_metrics),
        audit_flag_codes=audit_flag_codes,
    )


def _load_run_audit_record_from_manifest(
    manifest_path: Path,
    manifest_record: dict[str, Any],
) -> dict[str, Any] | None:
    files = manifest_record.get("files", [])

    if not isinstance(files, list):
        return None

    run_audit_file = next(
        (
            file
            for file in files
            if isinstance(file, dict) and file.get("role") == "report_run_audit"
        ),
        None,
    )

    if not run_audit_file:
        return None

    candidate_paths = []

    raw_path = run_audit_file.get("path")
    if raw_path:
        candidate_paths.append(Path(str(raw_path)))

    relative_path = run_audit_file.get("relative_path")
    if relative_path:
        candidate_paths.append(manifest_path.parent / str(relative_path))

    for candidate_path in candidate_paths:
        if candidate_path.exists():
            try:
                record = json.loads(candidate_path.read_text(encoding="utf-8-sig"))
            except json.JSONDecodeError:
                return None

            return record if isinstance(record, dict) else None

    return None


def _summarize_metric(
    runs: tuple[AuditHistoryRun, ...],
    metric_name: str,
) -> AuditHistoryMetricSummary:
    values = tuple(
        float(run.headline_metrics[metric_name])
        for run in runs
        if run.headline_metrics.get(metric_name) is not None
    )

    latest = values[-1] if values else None
    previous = values[-2] if len(values) >= 2 else None
    change = latest - previous if latest is not None and previous is not None else None

    return AuditHistoryMetricSummary(
        field_name=metric_name,
        count=len(values),
        mean=sum(values) / len(values) if values else None,
        minimum=min(values) if values else None,
        maximum=max(values) if values else None,
        latest=latest,
        previous=previous,
        change=change,
    )


def _count_values(values) -> dict[str, int]:
    counts: dict[str, int] = {}

    for value in values:
        normalized = str(value)
        counts[normalized] = counts.get(normalized, 0) + 1

    return dict(sorted(counts.items()))


def _metric_summary_to_record(summary: AuditHistoryMetricSummary) -> dict[str, Any]:
    return {
        "field_name": summary.field_name,
        "count": summary.count,
        "mean": summary.mean,
        "min": summary.minimum,
        "max": summary.maximum,
        "latest": summary.latest,
        "previous": summary.previous,
        "change": summary.change,
    }


def _run_to_record(run: AuditHistoryRun) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "generated_at_utc": run.generated_at_utc,
        "bundle_dir": run.bundle_dir,
        "health_status": run.health_status,
        "file_count": run.file_count,
        "total_size_bytes": run.total_size_bytes,
        "flag_count": run.flag_count,
        "fail_count": run.fail_count,
        "warn_count": run.warn_count,
        "info_count": run.info_count,
        "headline_metrics": dict(run.headline_metrics),
        "audit_flag_codes": list(run.audit_flag_codes),
    }