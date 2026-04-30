from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

SETTLEMENT_JSON = RESEARCH_DIR / "latest_research_settlement.json"
OUT_JSON = RESEARCH_DIR / "latest_calibration_report.json"
OUT_MD = RESEARCH_DIR / "latest_calibration_report.md"

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
    "paper_only": True,
}

BIN_EDGES = [round(i / 10, 2) for i in range(11)]
INSUFFICIENT_SAMPLE_THRESHOLD = 100
INSUFFICIENT_BIN_THRESHOLD = 20


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return None


def safe_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").replace("\n", " ").replace("|", "/").strip()
    return text or default


def minute_bucket(value: Any, bucket_size: int = 5) -> str:
    minute = fnum(value)
    if minute is None:
        return "UNKNOWN"
    return str(int(minute // bucket_size) * bucket_size)


def read_rows(path: Path) -> tuple[list[dict[str, Any]], list[str], str | None]:
    try:
        if not path.exists():
            return [], [], "MISSING_INPUT"
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                return list(reader), list(reader.fieldnames or []), None
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return [], [], f"READ_ERROR:{exc}"

    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            dict_rows = [row for row in rows if isinstance(row, dict)]
            fields = sorted({field for row in dict_rows for field in row})
            return dict_rows, fields, None
        return [], sorted(payload.keys()), "ROWS_NOT_FOUND"
    if isinstance(payload, list):
        dict_rows = [row for row in payload if isinstance(row, dict)]
        fields = sorted({field for row in dict_rows for field in row})
        return dict_rows, fields, None
    return [], [], "UNSUPPORTED_INPUT_SHAPE"


def probability_from_row(row: dict[str, Any]) -> float | None:
    for field in ("p_model", "calibrated_probability", "raw_probability"):
        probability = fnum(row.get(field))
        if probability is not None and 0.0 <= probability <= 1.0:
            return probability
    return None


def outcome_from_row(row: dict[str, Any]) -> float | None:
    if str(row.get("settlement_status") or "").upper() != "SETTLED":
        return None
    result = str(row.get("result_status") or row.get("result") or "").upper()
    if result in {"WIN", "WON"}:
        return 1.0
    if result in {"LOSS", "LOST"}:
        return 0.0
    return None


def required_schema(fields: list[str]) -> tuple[list[str], list[str]]:
    field_set = set(fields)
    probability_ok = any(field in field_set for field in ("p_model", "calibrated_probability", "raw_probability"))
    required = ["settlement_status", "result_status", "p_model|calibrated_probability|raw_probability"]
    missing = []
    if "settlement_status" not in field_set:
        missing.append("settlement_status")
    if "result_status" not in field_set and "result" not in field_set:
        missing.append("result_status")
    if not probability_ok:
        missing.append("p_model|calibrated_probability|raw_probability")
    return required, missing


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def brier_score(rows: list[dict[str, Any]]) -> float | None:
    errors = []
    for row in rows:
        p = row.get("_p_model")
        y = row.get("_outcome")
        if p is not None and y is not None:
            errors.append((float(p) - float(y)) ** 2)
    return mean(errors)


def log_loss(rows: list[dict[str, Any]]) -> float | None:
    losses = []
    eps = 1e-15
    for row in rows:
        p = row.get("_p_model")
        y = row.get("_outcome")
        if p is None or y is None:
            continue
        probability = min(1.0 - eps, max(eps, float(p)))
        outcome = float(y)
        losses.append(-(outcome * math.log(probability) + (1.0 - outcome) * math.log(1.0 - probability)))
    return mean(losses)


def calibration_error(rows: list[dict[str, Any]]) -> float | None:
    predicted = mean([float(row["_p_model"]) for row in rows if row.get("_p_model") is not None])
    hit_rate = mean([float(row["_outcome"]) for row in rows if row.get("_outcome") is not None])
    if predicted is None or hit_rate is None:
        return None
    return round(abs(predicted - hit_rate), 6)


def bin_label(lower: float, upper: float) -> str:
    return f"{lower:.2f}-{upper:.2f}"


def probability_bin(probability: float) -> tuple[float, float]:
    if probability >= 1.0:
        return 0.90, 1.00
    lower_index = int(probability * 10)
    lower = round(lower_index / 10, 2)
    upper = round(lower + 0.10, 2)
    return lower, upper


def summarize_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "eligible_settled_rows": len(rows),
        "brier_score": brier_score(rows),
        "log_loss": log_loss(rows),
        "avg_predicted_probability": mean([float(row["_p_model"]) for row in rows]),
        "empirical_hit_rate": mean([float(row["_outcome"]) for row in rows]),
        "absolute_calibration_error": calibration_error(rows),
        "insufficient_sample": len(rows) < INSUFFICIENT_SAMPLE_THRESHOLD,
    }


def group_summary(rows: list[dict[str, Any]], key_name: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(safe_text(row.get(key_name)), []).append(row)
    return {
        key: summarize_group(group_rows)
        for key, group_rows in sorted(grouped.items())
    }


def compound_group_summary(rows: list[dict[str, Any]], key_names: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = "||".join(safe_text(row.get(key_name)) for key_name in key_names)
        grouped.setdefault(key, []).append(row)
    return {
        key: summarize_group(group_rows)
        for key, group_rows in sorted(grouped.items())
    }


def calibration_bins(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[float, float], list[dict[str, Any]]] = {
        (BIN_EDGES[index], BIN_EDGES[index + 1]): []
        for index in range(10)
    }
    for row in rows:
        probability = float(row["_p_model"])
        grouped[probability_bin(probability)].append(row)

    bins = []
    for lower, upper in sorted(grouped):
        bucket_rows = grouped[(lower, upper)]
        bins.append({
            "bin": bin_label(lower, upper),
            "lower_bound": lower,
            "upper_bound": upper,
            "bin_count": len(bucket_rows),
            "avg_predicted_probability": mean([float(row["_p_model"]) for row in bucket_rows]),
            "empirical_hit_rate": mean([float(row["_outcome"]) for row in bucket_rows]),
            "absolute_calibration_error": calibration_error(bucket_rows),
            "insufficient_sample": len(bucket_rows) < INSUFFICIENT_BIN_THRESHOLD,
        })
    return bins


def eligible_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eligible: list[dict[str, Any]] = []
    for row in rows:
        p = probability_from_row(row)
        outcome = outcome_from_row(row)
        if p is None or outcome is None:
            continue
        enriched = dict(row)
        enriched["_p_model"] = p
        enriched["_outcome"] = outcome
        enriched["_market"] = safe_text(row.get("market") or row.get("market_key"))
        enriched["_research_bucket"] = safe_text(row.get("research_bucket"))
        enriched["_minute_bucket"] = minute_bucket(row.get("minute"))
        eligible.append(enriched)
    return eligible


def build_report(input_path: Path = SETTLEMENT_JSON) -> dict[str, Any]:
    generated_at_utc = utc_now()
    rows, fields, read_error = read_rows(input_path)
    required, missing = required_schema(fields)
    eligible = eligible_rows(rows) if not missing else []
    warnings: list[str] = []
    if read_error:
        warnings.append(read_error)
    if missing:
        warnings.append("MISSING_REQUIRED_CALIBRATION_SCHEMA")
    if rows and not eligible and not missing:
        warnings.append("NO_ELIGIBLE_SETTLED_WIN_LOSS_ROWS")
    if 0 < len(eligible) < INSUFFICIENT_SAMPLE_THRESHOLD:
        warnings.append(f"INSUFFICIENT_SAMPLE:{len(eligible)}<{INSUFFICIENT_SAMPLE_THRESHOLD}")

    if read_error and not rows:
        status = "REVIEW"
    elif not rows:
        status = "EMPTY"
    elif missing or not eligible:
        status = "REVIEW"
    else:
        status = "READY"

    for row in eligible:
        row["market"] = row["_market"]
        row["research_bucket"] = row["_research_bucket"]
        row["minute_bucket"] = row["_minute_bucket"]

    summary = summarize_group(eligible) if eligible else {
        "eligible_settled_rows": 0,
        "brier_score": None,
        "log_loss": None,
        "avg_predicted_probability": None,
        "empirical_hit_rate": None,
        "absolute_calibration_error": None,
        "insufficient_sample": True,
    }

    return {
        "mode": "FQIS_CALIBRATION_AUDIT_REPORT",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "source_files_used": [str(input_path)],
        "source_files": {
            "research_settlement": str(input_path),
        },
        "required_schema": required,
        "missing_columns": missing,
        "probability_field_preference": ["p_model", "calibrated_probability", "raw_probability"],
        "outcome_definition": "SETTLED rows with result_status WIN/LOSS only; PUSH/PENDING/UNSETTLED are excluded from calibration.",
        "total_rows": len(rows),
        "eligible_settled_rows": len(eligible),
        "brier_score": summary["brier_score"],
        "log_loss": summary["log_loss"],
        "calibration_bins": calibration_bins(eligible),
        "by_market": group_summary(eligible, "market"),
        "by_research_bucket": group_summary(eligible, "research_bucket"),
        "by_minute_bucket": group_summary(eligible, "minute_bucket"),
        "by_research_bucket_market": compound_group_summary(eligible, ("research_bucket", "market")),
        "warning_flags": sorted(set(warnings)),
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
    }


def write_json_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def write_markdown_report(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# FQIS Calibration Audit Report",
        "",
        "PAPER ONLY | NO REAL BET | NO STAKE | NO EXECUTION",
        "",
        "## Summary",
        "",
        f"- Status: **{report.get('status')}**",
        f"- Generated at UTC: `{report.get('generated_at_utc')}`",
        f"- Total rows: **{report.get('total_rows', 0)}**",
        f"- Eligible settled rows: **{report.get('eligible_settled_rows', 0)}**",
        f"- Brier score: **{report.get('brier_score')}**",
        f"- Log loss: **{report.get('log_loss')}**",
        "",
        "## Warning Flags",
        "",
    ]
    flags = report.get("warning_flags") or []
    if flags:
        lines.extend(f"- {safe_text(flag)}" for flag in flags)
    else:
        lines.append("- NONE")

    lines += [
        "",
        "## Calibration Bins",
        "",
        "| Bin | Count | Avg predicted | Empirical hit rate | Abs error |",
        "|---|---:|---:|---:|---:|",
    ]
    for bucket in report.get("calibration_bins") or []:
        lines.append(
            "| {bin} | {count} | {avg} | {hit} | {err} |".format(
                bin=bucket.get("bin"),
                count=bucket.get("bin_count", 0),
                avg=bucket.get("avg_predicted_probability"),
                hit=bucket.get("empirical_hit_rate"),
                err=bucket.get("absolute_calibration_error"),
            )
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build FQIS settled research calibration audit report.")
    parser.add_argument("--input-path", default=str(SETTLEMENT_JSON))
    parser.add_argument("--output-json", default=str(OUT_JSON))
    parser.add_argument("--output-md", default=str(OUT_MD))
    args = parser.parse_args()

    report = build_report(Path(args.input_path))
    write_json_report(report, Path(args.output_json))
    write_markdown_report(report, Path(args.output_md))

    print(json.dumps({
        "status": report["status"],
        "total_rows": report["total_rows"],
        "eligible_settled_rows": report["eligible_settled_rows"],
        "output_json": str(Path(args.output_json)),
        "output_md": str(Path(args.output_md)),
        "can_execute_real_bets": False,
        "can_enable_live_staking": False,
        "can_mutate_ledger": False,
        "promotion_allowed": False,
    }, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
