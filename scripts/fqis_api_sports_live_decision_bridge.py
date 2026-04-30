from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
import urllib.request
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from app.core.calibration import CalibrationLayer
from app.core.contracts import MarketProjection
from app.services.execution_layer import ExecutionLayer
from app.fqis.level3_state_classifier import classify_level3_state
from app.fqis.level3_pipeline_router import route_level3_pipeline


MODE = "FQIS_LEVEL2_DECISION_BRIDGE"
MODEL_VERSION = "live_poisson_decision_bridge_v1"
SUPPORTED_RAW_MARKETS = {"Match Goals", "Over/Under Line"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        x = float(str(value).replace(",", ".").strip())
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default


def safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def line_is_standard_half_goal(line: float) -> bool:
    frac = abs(line - math.floor(line))
    return abs(frac - 0.5) < 1e-9


def fixture_map(fixtures_payload: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not fixtures_payload:
        return out
    for item in fixtures_payload.get("fixtures", []) or []:
        fid = str(item.get("fixture_id") or "")
        if fid:
            out[fid] = item
    return out


def match_name(raw_item: dict[str, Any], fixture: dict[str, Any] | None, fixture_id: str) -> str:
    if fixture and fixture.get("match"):
        return str(fixture["match"])

    teams = raw_item.get("teams", {}) or {}
    home = ((teams.get("home") or {}).get("name")) or "Home"
    away = ((teams.get("away") or {}).get("name")) or "Away"
    return f"{home} vs {away}" if home or away else f"Fixture {fixture_id}"


def score_state(raw_item: dict[str, Any], fixture: dict[str, Any] | None) -> tuple[int, int]:
    if fixture:
        h = safe_int(fixture.get("score_home"), None)
        a = safe_int(fixture.get("score_away"), None)
        if h is not None and a is not None:
            return h, a

    teams = raw_item.get("teams", {}) or {}
    home = teams.get("home") or {}
    away = teams.get("away") or {}
    h = safe_int(home.get("goals"), 0) or 0
    a = safe_int(away.get("goals"), 0) or 0
    return h, a


def elapsed_minute(raw_item: dict[str, Any], fixture: dict[str, Any] | None) -> int:
    if fixture:
        minute = safe_int(fixture.get("elapsed"), None)
        if minute is not None:
            return minute

    status = ((raw_item.get("fixture") or {}).get("status")) or {}
    minute = safe_int(status.get("elapsed"), 0) or 0
    return minute


def is_blocked(raw_item: dict[str, Any]) -> bool:
    status = raw_item.get("status", {}) or {}
    return bool(status.get("blocked") or status.get("stopped") or status.get("finished"))


def parse_line(value_item: dict[str, Any]) -> float | None:
    line = safe_float(value_item.get("handicap"), None)
    if line is not None:
        return line

    text = str(value_item.get("value") or value_item.get("selection") or "")
    parts = text.replace(",", ".").split()
    for part in reversed(parts):
        x = safe_float(part, None)
        if x is not None:
            return x

    return None


def canonical_side(value_item: dict[str, Any]) -> str | None:
    text = str(value_item.get("value") or value_item.get("selection") or "").strip().upper()
    if text.startswith("OVER"):
        return "OVER"
    if text.startswith("UNDER"):
        return "UNDER"
    return None


def poisson_distribution(lam: float, max_k: int = 12) -> dict[int, float]:
    lam = clamp(lam, 0.01, 8.0)
    probs: dict[int, float] = {}
    total = 0.0

    for k in range(max_k + 1):
        p = math.exp(-lam) * (lam ** k) / math.factorial(k)
        probs[k] = p
        total += p

    if total > 0:
        for k in probs:
            probs[k] /= total

    return probs


def remaining_goal_expectancy(minute: int, home_score: int, away_score: int) -> float:
    minute = clamp(float(minute), 0.0, 90.0)
    remaining = max(0.0, 90.0 - minute)

    baseline_total_goals_per_90 = 2.58
    base_remaining = baseline_total_goals_per_90 * remaining / 90.0

    current_total = home_score + away_score
    expected_so_far = baseline_total_goals_per_90 * minute / 90.0

    pace_ratio = (current_total + 0.35) / max(0.55, expected_so_far + 0.20)
    pace_multiplier = clamp(pace_ratio, 0.72, 1.32)

    score_gap = abs(home_score - away_score)
    if score_gap == 0:
        state_multiplier = 1.03
    elif score_gap == 1:
        state_multiplier = 1.08
    elif score_gap == 2:
        state_multiplier = 0.96
    else:
        state_multiplier = 0.86

    late_multiplier = 1.0
    if minute >= 75:
        late_multiplier = 0.92
    elif minute >= 60:
        late_multiplier = 0.97

    lam = base_remaining * pace_multiplier * state_multiplier * late_multiplier
    return clamp(lam, 0.05, 4.25)


def final_total_distribution(current_total: int, lam_remaining: float) -> dict[int, float]:
    rem = poisson_distribution(lam_remaining)
    return {current_total + k: p for k, p in rem.items()}


def probability_for_total_side(dist: dict[int, float], line: float, side: str) -> float:
    if side == "OVER":
        return sum(p for total, p in dist.items() if total > line)
    if side == "UNDER":
        return sum(p for total, p in dist.items() if total < line)
    return 0.0


def no_vig_pair(over_odds: float, under_odds: float) -> tuple[float, float, float]:
    raw_over = 1.0 / over_odds
    raw_under = 1.0 / under_odds
    overround = raw_over + raw_under
    if overround <= 0:
        return 0.0, 0.0, 0.0
    return raw_over / overround, raw_under / overround, overround


def extract_total_goal_groups(
    raw_payload: Any,
    fixtures_payload: Any,
    *,
    min_odds: float,
    max_odds: float,
    standard_lines_only: bool,
) -> list[dict[str, Any]]:
    fmap = fixture_map(fixtures_payload)
    groups: dict[str, dict[str, Any]] = {}

    for raw_item in (raw_payload or {}).get("response", []) or []:
        fixture = raw_item.get("fixture") or {}
        fixture_id = str(fixture.get("id") or "")
        if not fixture_id:
            continue

        if is_blocked(raw_item):
            continue

        fixture_meta = fmap.get(fixture_id)
        home_score, away_score = score_state(raw_item, fixture_meta)
        minute = elapsed_minute(raw_item, fixture_meta)
        name = match_name(raw_item, fixture_meta, fixture_id)

        for market in raw_item.get("odds", []) or []:
            market_name = str(market.get("name") or "").strip()

            if market_name not in SUPPORTED_RAW_MARKETS:
                continue

            if "1st Half" in market_name or "2nd Half" in market_name:
                continue

            for value_item in market.get("values", []) or []:
                if bool(value_item.get("suspended")):
                    continue

                side = canonical_side(value_item)
                if side not in {"OVER", "UNDER"}:
                    continue

                line = parse_line(value_item)
                if line is None:
                    continue

                if standard_lines_only and not line_is_standard_half_goal(line):
                    continue

                odds = safe_float(value_item.get("odd") or value_item.get("odds"), None)
                if odds is None or odds < min_odds or odds > max_odds:
                    continue

                key = f"{fixture_id}|{market_name}|{line:.2f}"
                group = groups.setdefault(
                    key,
                    {
                        "fixture_id": fixture_id,
                        "match": name,
                        "minute": minute,
                        "home_score": home_score,
                        "away_score": away_score,
                        "market_name": market_name,
                        "line": line,
                        "offers": {},
                    },
                )

                prev = group["offers"].get(side)
                if prev is None or odds > prev["odds_decimal"]:
                    group["offers"][side] = {
                        "side": side,
                        "odds_decimal": odds,
                        "raw_value": value_item,
                    }

    out = []
    for group in groups.values():
        if "OVER" in group["offers"] and "UNDER" in group["offers"]:
            out.append(group)

    return out


def projection_to_record(projection: MarketProjection, base: dict[str, Any]) -> dict[str, Any]:
    payload = dict(getattr(projection, "payload", {}) or {})

    return {
        "mode": MODE,
        "model_version": MODEL_VERSION,
        "generated_at_utc": utc_now(),
        "fixture_id": base["fixture_id"],
        "match": base["match"],
        "score": f"{base['home_score']}-{base['away_score']}",
        "minute": base["minute"],
        "market": "Total Goals FT",
        "market_key": projection.market_key,
        "side": projection.side,
        "line": projection.line,
        "selection": f"{projection.side.title()} {projection.line:g}",
        "odds_decimal": projection.odds_decimal,
        "bookmaker": projection.bookmaker,
        "raw_probability": projection.raw_probability,
        "calibrated_probability": projection.calibrated_probability,
        "market_no_vig_probability": projection.market_no_vig_probability,
        "edge": projection.edge,
        "expected_value": projection.expected_value,
        "price_state": projection.price_state,
        "documentary_status": projection.documentary_status,
        "real_status": projection.real_status,
        "top_bet_flag": projection.top_bet_flag,
        "executable": projection.executable,
        "reasons": list(projection.reasons),
        "vetoes": list(projection.vetoes),
        "payload": payload,
    }



def bridge_blocker_family(veto: str) -> str:
    v = str(veto or "")

    if v.startswith("level3_"):
        return "LEVEL3_DATA"

    if v == "non_positive_edge":
        return "NEGATIVE_VALUE"

    if (
        "under_0_5" in v
        or "under_1_5" in v
        or "under_2_5" in v
        or "under_one_goal" in v
        or "under_two_goal" in v
    ):
        return "MARKET_STRUCTURE"

    if "edge" in v:
        return "EDGE"

    if "ev" in v:
        return "EV"

    if "regime" in v or "confidence" in v or "fragility" in v:
        return "EXECUTION_RISK"

    return "OTHER"



def classify_group(group: dict[str, Any], level3_states: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    calibration = CalibrationLayer()
    execution = ExecutionLayer()

    state = (level3_states or {}).get(str(group["fixture_id"])) or {}

    regime_label = str(state.get("regime_label") or "NEUTRAL")
    regime_confidence = safe_float(state.get("regime_confidence"), 0.62) or 0.62
    feed_quality = safe_float(state.get("feed_quality"), 0.62) or 0.62
    chaos_index = safe_float(state.get("chaos_index"), 0.25) or 0.25
    pressure_index = safe_float(state.get("pressure_index"), 0.0) or 0.0

    state_ready = bool(state.get("state_ready", False))
    trade_ready = bool(state.get("trade_ready", False))
    events_available = bool(state.get("events_available", False))
    stats_available = bool(state.get("stats_available", False))
    data_mode = str(state.get("data_mode") or "UNKNOWN")

    level3_classification = classify_level3_state(
        events_available=events_available,
        stats_available=stats_available,
        promotion_allowed=False,
    )
    level3_route = route_level3_pipeline(
        state=level3_classification.state.value,
        promotion_allowed=False,
    )

    state_warnings = list(state.get("state_warnings") or [])
    state_vetoes = list(state.get("vetoes") or [])

    over_offer = group["offers"]["OVER"]
    under_offer = group["offers"]["UNDER"]

    p_mkt_over, p_mkt_under, overround = no_vig_pair(
        float(over_offer["odds_decimal"]),
        float(under_offer["odds_decimal"]),
    )

    home_score = int(group["home_score"])
    away_score = int(group["away_score"])
    current_total = home_score + away_score
    minute = int(group["minute"])
    line = float(group["line"])

    lam_remaining = remaining_goal_expectancy(minute, home_score, away_score)
    dist = final_total_distribution(current_total, lam_remaining)

    records = []

    for side, offer, p_mkt in (
        ("OVER", over_offer, p_mkt_over),
        ("UNDER", under_offer, p_mkt_under),
    ):
        structural_p = probability_for_total_side(dist, line, side)

        # Niveau 2: on évite de faire croire que le modèle est plus fort qu'il ne l'est.
        # On blend 70% structure live + 30% marché no-vig.
        raw_p = clamp(0.70 * structural_p + 0.30 * p_mkt, 0.001, 0.999)

        cal = calibration.calibrate(
            "OU_FT",
            raw_p,
            minute=minute,
            regime=regime_label,
            quality=feed_quality,
            market_probability=p_mkt,
            side=side,
            current_total=current_total,
            line=line,
            remaining_goal_expectancy=lam_remaining,
            score_home=home_score,
            score_away=away_score,
        )

        p_cal = float(cal.calibrated_probability)
        odds = float(offer["odds_decimal"])
        edge = p_cal - p_mkt
        ev = p_cal * odds - 1.0

        projection = MarketProjection(
            market_key="OU_FT",
            side=side,
            line=line,
            raw_probability=round(raw_p, 6),
            calibrated_probability=round(p_cal, 6),
            market_no_vig_probability=round(p_mkt, 6),
            edge=round(edge, 6),
            expected_value=round(ev, 6),
            bookmaker="API-Sports Live",
            odds_decimal=odds,
            executable=True,
            price_state="VIVANT",
            documentary_status="DOC_ONLY",
            real_status="NO_BET",
            reasons=[
                "level2_decision_bridge",
                "ou_ft_only",
                "standard_half_goal_line",
                "model_calibration_execution_layer_attached",
                "level3_state_attached",
                f"level3_mode_{data_mode}",
            ],
            vetoes=[],
            payload={
                "fixture_id": group["fixture_id"],
                "match": group["match"],
                "minute": minute,
                "score_home": home_score,
                "score_away": away_score,
                "current_total": current_total,
                "line": line,
                "regime_label": regime_label,
                "regime_confidence": round(regime_confidence, 6),
                "calibration_confidence": float(cal.confidence),
                "data_quality": round(feed_quality, 6),
                "feed_quality": round(feed_quality, 6),
                "chaos": round(chaos_index, 6),
                "pressure_index": round(pressure_index, 6),
                "level3_data_mode": data_mode,
                "level3_state_ready": state_ready,
                "level3_trade_ready": trade_ready,
                "level3_events_available": events_available,
                "level3_stats_available": stats_available,
                "level3_gate_state": level3_classification.state.value,
                "level3_pipeline": level3_route.pipeline,
                "level3_production_allowed": level3_route.production_allowed,
                "level3_research_allowed": level3_route.research_allowed,
                "level3_live_staking_allowed": False,
                "level3_route_reason": level3_route.reason,
                "final_pipeline": level3_route.pipeline,
                "final_pipeline_reason": level3_route.reason,
                "live_staking_allowed": False,
                "level3_state_warnings": state_warnings,
                "level3_state_vetoes": state_vetoes,
                "remaining_goal_expectancy": round(lam_remaining, 6),
                "lambda_total_remaining": round(lam_remaining, 6),
                "market_overround": round(overround, 6),
                "raw_market_name": group["market_name"],
                "structural_probability": round(structural_p, 6),
                "calibration_segment": cal.segment,
            },
        )

        classified = execution.classify(projection)

        # Preserve the execution-layer opinion before external/Level-3 vetoes.
        classified.payload = dict(getattr(classified, "payload", {}) or {})
        classified.payload["pre_external_veto_real_status"] = getattr(classified, "real_status", "NO_BET")
        classified.payload["pre_external_veto_top_bet_flag"] = bool(getattr(classified, "top_bet_flag", False))
        classified.payload["pre_external_veto_executable"] = bool(getattr(classified, "executable", False))

        # Level 3 cannot be bypassed.
        # State-ready means the bot can understand the match.
        # Trade-ready means the bot has enough live data to validate stronger decisions.
        if level3_route.reject:
            classified.vetoes.append("level3_pipeline_reject")

        if level3_route.pipeline == "research":
            classified.vetoes.append("level3_research_only")

        if not state_ready:
            classified.vetoes.append("level3_state_not_ready")

        if not trade_ready:
            classified.vetoes.append("level3_not_trade_ready")

        for veto in state_vetoes:
            veto_name = f"level3_{veto}"
            if veto_name not in classified.vetoes:
                classified.vetoes.append(veto_name)

        # Put Level 3 vetoes first so reports show the true blocking reason.
        classified.vetoes = sorted(
            list(dict.fromkeys(classified.vetoes)),
            key=lambda v: (0 if str(v).startswith("level3_") else 1, str(v)),
        )

        if classified.vetoes:
            primary_veto = str(classified.vetoes[0])
            classified.payload["post_veto_operational_status"] = "BLOCKED"
            classified.payload["primary_veto"] = primary_veto
            classified.payload["primary_blocker_family"] = bridge_blocker_family(primary_veto)

            # Critical clarity rule:
            # A decision may have passed the execution layer pre-veto, but if any final veto exists,
            # operational real_status must be NO_BET.
            classified.real_status = "NO_BET"
            classified.top_bet_flag = False
            classified.executable = False
        else:
            classified.payload["post_veto_operational_status"] = "PASS"
            classified.payload["primary_veto"] = ""
            classified.payload["primary_blocker_family"] = "NONE"

        records.append(projection_to_record(classified, group))

    return records


def publication_status(record: dict[str, Any], *, min_edge: float, min_ev: float, min_confidence: float) -> str:
    vetoes = set(record.get("vetoes") or [])

    # Niveau 2 strict:
    # - une d?cision avec veto reste bloqu?e
    # - on ne publie pas une watchlist qui contredit l'ExecutionLayer
    if vetoes:
        return "BLOCKED"

    if record["real_status"] in {"REAL_VALID", "TOP_BET"}:
        return "PAPER_DECISION"

    confidence = safe_float((record.get("payload") or {}).get("calibration_confidence"), 0.0) or 0.0

    if (
        float(record["edge"]) >= min_edge
        and float(record["expected_value"]) >= min_ev
        and confidence >= min_confidence
    ):
        return "WATCHLIST_DECISION"

    return "BLOCKED"



def send_discord(payload: dict[str, Any]) -> bool:
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        return False

    def _post(body: dict[str, Any]) -> bool:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "FQIS-Level2-DecisionBridge/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=20) as response:
            response.read()
        return True

    try:
        return _post(payload)
    except Exception as exc:
        print(f"DISCORD_SEND_FAILED_EMBED_OR_PAYLOAD: {exc}", file=sys.stderr)

    try:
        content = payload_to_plain_discord_text(payload)
        if not content.strip():
            content = "FQIS Discord notification"
        if len(content) > 1900:
            content = content[:1900] + chr(10) + "...[TRUNCATED]"
        return _post({"content": content})
    except Exception as exc:
        print(f"DISCORD_SEND_FAILED_FALLBACK_TEXT: {exc}", file=sys.stderr)
        return False


def payload_to_plain_discord_text(payload: dict[str, Any]) -> str:
    if payload.get("content"):
        return str(payload["content"])

    embeds = payload.get("embeds") or []
    if not embeds:
        return ""

    embed = embeds[0] or {}
    lines = []

    title = embed.get("title")
    description = embed.get("description")

    if title:
        lines.append(f"**{title}**")
    if description:
        lines.append(str(description))

    for field in embed.get("fields") or []:
        name = str(field.get("name", ""))
        value = str(field.get("value", ""))
        if name or value:
            lines.append(f"**{name}**: {value}")

    footer = embed.get("footer") or {}
    if footer.get("text"):
        lines.append("")
        lines.append(str(footer["text"]))

    return chr(10).join(lines)


def decision_embed(record: dict[str, Any], status: str, run_dir: Path) -> dict[str, Any]:
    color = 0x2ECC71 if status == "PAPER_DECISION" else 0xF1C40F

    vetoes = record.get("vetoes") or []
    reasons = record.get("reasons") or []
    payload = record.get("payload") or {}

    veto_text = "None" if not vetoes else ", ".join(vetoes[:5])
    reason_text = ", ".join(reasons[:4]) if reasons else "decision_bridge"

    return {
        "title": f"FQIS L2 {status}",
        "description": "Paper only. No real staking. Model + calibration + execution layer attached.",
        "color": color,
        "fields": [
            {"name": "Match", "value": str(record["match"])[:1024], "inline": False},
            {"name": "Score / minute", "value": f"{record['score']} | {record['minute']}'", "inline": True},
            {"name": "Selection", "value": f"{record['selection']} @ {record['odds_decimal']}", "inline": True},
            {"name": "Status", "value": f"{record['real_status']} / {record['price_state']}", "inline": True},
            {"name": "Model probability", "value": f"{float(record['calibrated_probability']) * 100:.2f}%", "inline": True},
            {"name": "Market no-vig", "value": f"{float(record['market_no_vig_probability']) * 100:.2f}%", "inline": True},
            {"name": "Edge / EV", "value": f"{float(record['edge']) * 100:.2f}% / {float(record['expected_value']) * 100:.2f}%", "inline": True},
            {"name": "Calibration confidence", "value": f"{float(payload.get('calibration_confidence', 0.0)) * 100:.1f}%", "inline": True},
            {"name": "Remaining xG proxy", "value": str(payload.get("remaining_goal_expectancy", "NA")), "inline": True},
            {"name": "Vetoes", "value": veto_text[:1024], "inline": False},
            {"name": "Reason", "value": reason_text[:1024], "inline": False},
            {"name": "Run folder", "value": str(run_dir)[:1024], "inline": False},
        ],
        "footer": {"text": "FQIS Level 2 | Paper only | Decision bridge"},
        "timestamp": utc_now(),
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    rows = payload.get("decisions", [])
    lines = [
        "# FQIS Level 2 Decision Bridge",
        "",
        "## Summary",
        "",
        f"- Status: **{payload['status']}**",
        f"- Mode: **{payload['mode']}**",
        f"- Groups total: **{payload['summary'].get('groups_total', 0)}**",
        f"- Groups priced: **{payload['summary'].get('groups_priced', 0)}**",
        f"- Groups skipped no Level 3: **{payload['summary'].get('groups_skipped_no_level3', 0)}**",
        f"- Official decisions: **{payload['summary']['official_decisions']}**",
        f"- Watchlist decisions: **{payload['summary']['watchlist_decisions']}**",
        f"- Blocked decisions: **{payload['summary']['blocked_decisions']}**",
        f"- Level 3 fixtures inspected: **{payload['summary'].get('level3_fixtures_inspected', 0)}**",
        f"- Level 3 state ready: **{payload['summary'].get('level3_state_ready', 0)}**",
        f"- Level 3 trade ready: **{payload['summary'].get('level3_trade_ready', 0)}**",
        f"- Level 3 stats available: **{payload['summary'].get('level3_stats_available', 0)}**",
        f"- Level 3 events available: **{payload['summary'].get('level3_events_available', 0)}**",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        "",
        "> PAPER ONLY. No real staking.",
        "",
        "## Decisions",
        "",
    ]

    if not rows:
        lines.append("No decisions.")
    else:
        lines.append("| Status | Match | Score | Min | Selection | Odds | Model | Market | Edge | EV | L3 mode | L3 state | L3 trade | Real | Vetoes |")
        lines.append("|---|---|---:|---:|---|---:|---:|---:|---:|---:|---|---|---|---|---|")
        for r in rows[:80]:
            vetoes = ", ".join((r.get("vetoes") or [])[:8])
            payload = r.get("payload") or {}
            lines.append(
                "| {publication_status} | {match} | {score} | {minute} | {selection} | {odds:.3f} | {model:.2f}% | {market:.2f}% | {edge:.2f}% | {ev:.2f}% | {l3_mode} | {l3_state} | {l3_trade} | {real} | {vetoes} |".format(
                    publication_status=r["publication_status"],
                    match=str(r["match"]).replace("|", "/"),
                    score=r["score"],
                    minute=r["minute"],
                    selection=r["selection"],
                    odds=float(r["odds_decimal"]),
                    model=float(r["calibrated_probability"]) * 100,
                    market=float(r["market_no_vig_probability"]) * 100,
                    edge=float(r["edge"]) * 100,
                    ev=float(r["expected_value"]) * 100,
                    l3_mode=str(payload.get("level3_data_mode", "NA")).replace("|", "/"),
                    l3_state="yes" if payload.get("level3_state_ready") else "no",
                    l3_trade="yes" if payload.get("level3_trade_ready") else "no",
                    real=r["real_status"],
                    vetoes=vetoes.replace("|", "/"),
                )
            )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def append_ledger(path: Path, records: list[dict[str, Any]]) -> None:
    if not records:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()

    fields = [
        "decision_key",
        "sent_at_utc",
        "publication_status",
        "fixture_id",
        "match",
        "score",
        "minute",
        "selection",
        "odds_decimal",
        "calibrated_probability",
        "market_no_vig_probability",
        "edge",
        "expected_value",
        "real_status",
        "price_state",
        "vetoes",
    ]

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            writer.writeheader()

        for r in records:
            writer.writerow({
                "decision_key": r["decision_key"],
                "sent_at_utc": utc_now(),
                "publication_status": r["publication_status"],
                "fixture_id": r["fixture_id"],
                "match": r["match"],
                "score": r["score"],
                "minute": r["minute"],
                "selection": r["selection"],
                "odds_decimal": r["odds_decimal"],
                "calibrated_probability": r["calibrated_probability"],
                "market_no_vig_probability": r["market_no_vig_probability"],
                "edge": r["edge"],
                "expected_value": r["expected_value"],
                "real_status": r["real_status"],
                "price_state": r["price_state"],
                "vetoes": ",".join(r.get("vetoes") or []),
            })


def load_seen(path: Path) -> set[str]:
    if not path.exists():
        return set()

    seen: set[str] = set()
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                key = row.get("decision_key")
                if key:
                    seen.add(key)
    except Exception:
        return set()

    return seen


def run_orchestrator(cycle_dir: Path, *, max_candidates: int, min_bookmakers: int) -> int:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "fqis_api_sports_inplay_orchestrator.py"),
        "--output-dir",
        str(cycle_dir),
        "--require-ready",
        "--max-candidates",
        str(max_candidates),
        "--min-bookmakers",
        str(min_bookmakers),
    ]

    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    (cycle_dir / "decision_bridge_stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (cycle_dir / "decision_bridge_stderr.log").write_text(proc.stderr or "", encoding="utf-8")

    return proc.returncode



def run_level3_state_probe_for_cycle(args: argparse.Namespace) -> dict[str, Any] | None:
    if not getattr(args, "use_level3_state", False):
        return None

    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "fqis_level3_live_state_probe.py"),
        "--source-dir",
        str(args.output_dir),
        "--output-dir",
        str(args.level3_output_dir),
        "--max-fixtures",
        str(args.level3_max_fixtures),
        "--cache-ttl-seconds",
        str(args.level3_cache_ttl_seconds),
    ]

    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    level3_dir = Path(args.level3_output_dir)
    level3_dir.mkdir(parents=True, exist_ok=True)
    (level3_dir / "last_probe_stdout.log").write_text(proc.stdout or "", encoding="utf-8")
    (level3_dir / "last_probe_stderr.log").write_text(proc.stderr or "", encoding="utf-8")

    payload = read_json(level3_dir / "latest_level3_live_state.json")
    return payload if isinstance(payload, dict) else None


def level3_state_map(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    if not payload:
        return out

    for row in payload.get("fixtures", []) or []:
        fixture_id = str(row.get("fixture_id") or "")
        if fixture_id:
            out[fixture_id] = row

    return out


def level3_summary(payload: dict[str, Any] | None) -> dict[str, int]:
    summary = (payload or {}).get("summary") or {}

    return {
        "level3_fixtures_inspected": int(summary.get("fixtures_inspected") or 0),
        "level3_state_ready": int(summary.get("state_ready") or summary.get("ready_states") or 0),
        "level3_trade_ready": int(summary.get("trade_ready") or 0),
        "level3_stats_available": int(summary.get("stats_available") or 0),
        "level3_events_available": int(summary.get("events_available") or 0),
    }



def run_cycle(args: argparse.Namespace, seen: set[str]) -> dict[str, Any]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cycle_dir = Path(args.output_dir) / f"run_{timestamp}"
    cycle_dir.mkdir(parents=True, exist_ok=True)

    exit_code = run_orchestrator(
        cycle_dir,
        max_candidates=args.max_candidates,
        min_bookmakers=args.min_bookmakers,
    )

    fixtures_payload = read_json(cycle_dir / "inplay_fixtures.json")
    raw_payload = read_json(cycle_dir / "diagnostics" / "odds_live_raw.json")

    level3_payload = run_level3_state_probe_for_cycle(args)
    states_by_fixture = level3_state_map(level3_payload)

    groups = []
    priced_groups = []
    skipped_no_level3_groups = []
    decisions = []

    if raw_payload:
        groups = extract_total_goal_groups(
            raw_payload,
            fixtures_payload,
            min_odds=args.min_odds,
            max_odds=args.max_odds,
            standard_lines_only=not args.include_asian_lines,
        )

        for group in groups:
            if getattr(args, "use_level3_state", False):
                state = states_by_fixture.get(str(group["fixture_id"]))
                if not state:
                    skipped_no_level3_groups.append(group)
                    continue

            priced_groups.append(group)
            decisions.extend(classify_group(group, states_by_fixture))

    enriched = []
    for record in decisions:
        status = publication_status(
            record,
            min_edge=args.min_edge,
            min_ev=args.min_ev,
            min_confidence=args.min_confidence,
        )

        key = "{fixture_id}|{market_key}|{side}|{line}|{odds}".format(
            fixture_id=record["fixture_id"],
            market_key=record["market_key"],
            side=record["side"],
            line=record["line"],
            odds=round(float(record["odds_decimal"]), 3),
        )

        record["publication_status"] = status
        record["decision_key"] = key
        enriched.append(record)

    enriched.sort(
        key=lambda r: (
            r["publication_status"] != "PAPER_DECISION",
            r["publication_status"] != "WATCHLIST_DECISION",
            -float(r["expected_value"]),
            -float(r["edge"]),
        )
    )

    official = [r for r in enriched if r["publication_status"] == "PAPER_DECISION"]
    watchlist = [r for r in enriched if r["publication_status"] == "WATCHLIST_DECISION"]
    blocked = [r for r in enriched if r["publication_status"] == "BLOCKED"]

    publishable = (official + watchlist)[: args.max_alerts]
    new_publishable = [r for r in publishable if r["decision_key"] not in seen]

    payload = {
        "mode": MODE,
        "status": "READY" if exit_code == 0 else "FAILED",
        "generated_at_utc": utc_now(),
        "cycle_dir": str(cycle_dir),
        "orchestrator_exit_code": exit_code,
        "config": {
            "min_edge": args.min_edge,
            "min_ev": args.min_ev,
            "min_confidence": args.min_confidence,
            "min_odds": args.min_odds,
            "max_odds": args.max_odds,
            "include_asian_lines": args.include_asian_lines,
        },
        "summary": {
            "groups_total": len(groups),
            "groups_priced": len(priced_groups),
            "groups_skipped_no_level3": len(skipped_no_level3_groups),
            "decisions_total": len(enriched),
            "official_decisions": len(official),
            "watchlist_decisions": len(watchlist),
            "blocked_decisions": len(blocked),
            "new_publishable_decisions": len(new_publishable),
            **level3_summary(level3_payload),
        },
        "decisions": enriched,
    }

    write_json(cycle_dir / "live_decisions.json", payload)
    write_json(Path(args.output_dir) / "latest_live_decisions.json", payload)
    write_markdown(cycle_dir / "live_decisions.md", payload)
    write_markdown(Path(args.output_dir) / "latest_live_decisions.md", payload)

    sent_publishable = []

    if args.discord:
        for record in new_publishable:
            sent = send_discord({"embeds": [decision_embed(record, record["publication_status"], cycle_dir)]})
            if sent:
                seen.add(record["decision_key"])
                sent_publishable.append(record)

        append_ledger(Path(args.output_dir) / "decision_ledger.csv", sent_publishable)

    print(json.dumps({
        "mode": MODE,
        "status": payload["status"],
        "cycle_dir": str(cycle_dir),
        "summary": payload["summary"],
    }, indent=2, ensure_ascii=True))

    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/pipeline/api_sports/decision_bridge_live")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=45)
    parser.add_argument("--discord", action="store_true")
    parser.add_argument("--max-candidates", type=int, default=100)
    parser.add_argument("--min-bookmakers", type=int, default=1)
    parser.add_argument("--min-odds", type=float, default=1.25)
    parser.add_argument("--max-odds", type=float, default=8.0)
    parser.add_argument("--min-edge", type=float, default=0.025)
    parser.add_argument("--min-ev", type=float, default=0.020)
    parser.add_argument("--min-confidence", type=float, default=0.45)
    parser.add_argument("--max-alerts", type=int, default=5)
    parser.add_argument("--include-asian-lines", action="store_true")
    parser.add_argument("--use-level3-state", action="store_true")
    parser.add_argument("--level3-output-dir", default="data/pipeline/api_sports/level3_live_state")
    parser.add_argument("--level3-max-fixtures", type=int, default=15)
    parser.add_argument("--level3-cache-ttl-seconds", type=int, default=120)

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    seen = load_seen(output_dir / "decision_ledger.csv")

    if args.discord:
        send_discord({
            "embeds": [{
                "title": "FQIS LEVEL 2 DECISION BRIDGE STARTED",
                "description": "Paper only. No real staking. Raw odds are now routed through model probability, calibration and execution gates.",
                "color": 0x3498DB,
                "fields": [
                    {"name": "Poll", "value": f"Every {args.poll_seconds}s" if args.loop else "Single run", "inline": True},
                    {"name": "Market", "value": "OU_FT only", "inline": True},
                    {"name": "Lines", "value": "Standard half-goal only" if not args.include_asian_lines else "Asian + standard", "inline": True},
                    {"name": "Level 3", "value": "ON" if args.use_level3_state else "OFF", "inline": True},
                    {"name": "Ledger", "value": str(output_dir / "decision_ledger.csv"), "inline": False},
                ],
                "footer": {"text": "FQIS Level 2 | Paper only | Decision bridge"},
                "timestamp": utc_now(),
            }]
        })

    if not args.loop:
        payload = run_cycle(args, seen)
        return 0 if payload["status"] == "READY" else 1

    last_heartbeat = 0.0

    while True:
        payload = run_cycle(args, seen)

        now = time.time()
        if args.discord and now - last_heartbeat >= 15 * 60:
            summary = payload["summary"]
            send_discord({
                "embeds": [{
                    "title": "FQIS LEVEL 2 DECISION HEARTBEAT",
                    "description": "Decision bridge running.",
                    "color": 0x95A5A6,
                    "fields": [
                        {"name": "Groups", "value": str(summary["groups_total"]), "inline": True},
                        {"name": "Decisions", "value": str(summary["decisions_total"]), "inline": True},
                        {"name": "Paper decisions", "value": str(summary["official_decisions"]), "inline": True},
                        {"name": "Watchlist", "value": str(summary["watchlist_decisions"]), "inline": True},
                        {"name": "Blocked", "value": str(summary["blocked_decisions"]), "inline": True},
                        {"name": "L3 state ready", "value": str(summary.get("level3_state_ready", 0)), "inline": True},
                        {"name": "L3 trade ready", "value": str(summary.get("level3_trade_ready", 0)), "inline": True},
                        {"name": "L3 stats/events", "value": f"{summary.get('level3_stats_available', 0)} / {summary.get('level3_events_available', 0)}", "inline": True},
                        {"name": "Latest run", "value": str(payload["cycle_dir"]), "inline": False},
                    ],
                    "footer": {"text": "FQIS Level 2 | Paper only | Decision bridge"},
                    "timestamp": utc_now(),
                }]
            })
            last_heartbeat = now

        time.sleep(max(10, int(args.poll_seconds)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
