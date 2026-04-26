from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.fqis.contracts.enums import ThesisKey
from app.fqis.runtime.shadow import FqisShadowInput


def load_shadow_inputs_from_jsonl(path: Path) -> tuple[FqisShadowInput, ...]:
    if not path.exists():
        raise FileNotFoundError(f"shadow input file not found: {path}")

    inputs: list[FqisShadowInput] = []

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc

        inputs.append(_parse_shadow_input(payload, line_number=line_number))

    return tuple(inputs)


def _parse_shadow_input(payload: dict[str, Any], *, line_number: int) -> FqisShadowInput:
    try:
        live_match_row = dict(payload["live_match_row"])
        live_offer_rows = tuple(dict(row) for row in payload["live_offer_rows"])
        p_real_by_thesis_raw = dict(payload["p_real_by_thesis"])
    except KeyError as exc:
        raise ValueError(f"missing required field on line {line_number}: {exc}") from exc

    return FqisShadowInput(
        live_match_row=live_match_row,
        live_offer_rows=live_offer_rows,
        p_real_by_thesis=_parse_p_real_by_thesis(p_real_by_thesis_raw, line_number=line_number),
    )


def _parse_p_real_by_thesis(
    raw: dict[str, Any],
    *,
    line_number: int,
) -> dict[ThesisKey, dict[str, float]]:
    parsed: dict[ThesisKey, dict[str, float]] = {}

    for thesis_key_raw, intent_probs_raw in raw.items():
        try:
            thesis_key = ThesisKey(str(thesis_key_raw))
        except ValueError as exc:
            raise ValueError(
                f"unknown thesis key on line {line_number}: {thesis_key_raw}"
            ) from exc

        parsed[thesis_key] = {
            str(intent_key): float(probability)
            for intent_key, probability in dict(intent_probs_raw).items()
        }

    return parsed

    