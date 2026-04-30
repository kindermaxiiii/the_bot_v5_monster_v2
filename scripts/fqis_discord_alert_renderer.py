from __future__ import annotations

import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"

DISCORD_PAYLOAD_JSON = ORCH_DIR / "latest_discord_paper_payload.json"

OUT_JSON = ORCH_DIR / "latest_discord_alert_renderer.json"
OUT_MD = ORCH_DIR / "latest_discord_alert_renderer.md"
OUT_HTML = ORCH_DIR / "latest_discord_alert_preview.html"

ELITE_SEND_REASONS = {
    "NEW_CANONICAL_PAPER_ALERTS_READY",
    "MATERIAL_CANONICAL_UPDATES_READY",
}

ELITE_LIFECYCLES = {
    "NEW_CANONICAL",
    "UPDATED_CANONICAL",
}

MODEL_LOG_LIFECYCLES = {
    "REPEATED_CANONICAL",
    "SUPPRESSED_REPEAT",
    "BLOCKED",
    "REVIEW",
}

SAFETY_BLOCK = {
    "can_execute_real_bets": False,
    "can_enable_live_staking": False,
    "can_mutate_ledger": False,
    "live_staking_allowed": False,
    "promotion_allowed": False,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {"missing": True, "path": str(path)}
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {"non_object_json": True, "payload": payload}
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def safe_text(value: Any) -> str:
    return str(value or "").replace("\n", " ").replace("|", "/").strip()


def esc(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def truthy_flag(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


def first_present(record: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return default


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        cleaned = str(value).replace("%", "").replace(",", ".").strip()
        return float(cleaned)
    except Exception:
        return default


def fmt_float(value: Any, decimals: int = 2) -> str:
    return f"{as_float(value):.{decimals}f}"


def fmt_pct(value: Any, *, signed: bool = False, decimals: int = 1) -> str:
    raw = as_float(value)
    pct = raw * 100 if abs(raw) <= 1 else raw
    sign = "+" if signed and pct > 0 else ""
    return f"{sign}{pct:.{decimals}f}%"


def compact_reason(value: Any, max_len: int = 110) -> str:
    text = safe_text(value)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def split_match(match: str) -> tuple[str, str]:
    cleaned = safe_text(match)
    if not cleaned:
        return "Home", "Away"

    for pattern in [r"\s+vs\s+", r"\s+v\s+", r"\s+-\s+"]:
        parts = re.split(pattern, cleaned, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2:
            return safe_text(parts[0]) or "Home", safe_text(parts[1]) or "Away"

    return cleaned, "Opponent"


def initials(name: str) -> str:
    words = [part for part in re.split(r"\s+", safe_text(name)) if part]
    if not words:
        return "?"
    if len(words) == 1:
        return words[0][:2].upper()
    return (words[0][:1] + words[1][:1]).upper()


def lifecycle(record: dict[str, Any]) -> str:
    return safe_text(
        first_present(
            record,
            "alert_lifecycle_status",
            "dedupe_status",
            "lifecycle",
            "status",
            default="UNKNOWN",
        )
    ).upper()


def record_is_sendable(record: dict[str, Any]) -> bool:
    life = lifecycle(record)
    return (
        life in ELITE_LIFECYCLES
        or record.get("discord_sendable") is True
        or record.get("is_new_alert") is True
        or record.get("is_updated_alert") is True
    )


def unsafe_source_flags(payload: dict[str, Any]) -> list[str]:
    unsafe_names = set(SAFETY_BLOCK)
    hits: list[str] = []

    def walk(value: Any, prefix: str = "") -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                path = f"{prefix}.{key}" if prefix else str(key)
                if key in unsafe_names and truthy_flag(item):
                    hits.append(path)
                walk(item, path)
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{prefix}[{index}]")

    walk(payload)
    return hits


def route_record(
    *,
    payload_status: str,
    payload_sendable: bool,
    send_reason: str,
    record: dict[str, Any],
    unsafe_hits: list[str],
) -> str:
    life = lifecycle(record)

    if unsafe_hits or payload_status != "READY":
        return "model_logs"

    if (
        payload_sendable
        and send_reason in ELITE_SEND_REASONS
        and record_is_sendable(record)
    ):
        return "elite_alerts"

    if life in MODEL_LOG_LIFECYCLES or life == "REPEATED_CANONICAL":
        return "model_logs"

    return "no_send"


def normalize_record(record: dict[str, Any], route: str, index: int) -> dict[str, Any]:
    match = safe_text(first_present(record, "match", "fixture", "fixture_name", "fixture_id", default="UNKNOWN MATCH"))
    home_name = safe_text(first_present(record, "home_team", "home", default=""))
    away_name = safe_text(first_present(record, "away_team", "away", default=""))

    if not home_name or not away_name:
        split_home, split_away = split_match(match)
        home_name = home_name or split_home
        away_name = away_name or split_away

    score = safe_text(first_present(record, "score", "current_score", default="0-0")).replace(" ", "")
    if "-" not in score:
        score = "0-0"

    score_home, score_away = "0", "0"
    score_parts = score.split("-", maxsplit=1)
    if len(score_parts) == 2:
        score_home = safe_text(score_parts[0]) or "0"
        score_away = safe_text(score_parts[1]) or "0"

    market = safe_text(
        first_present(
            record,
            "selection",
            "market_name",
            "market",
            "bet_name",
            default="Market unavailable",
        )
    )

    odds = first_present(record, "odds_latest", "odds_taken", "odds", default="")
    edge = first_present(record, "edge_latest", "edge_prob", "edge", default="")
    ev = first_present(record, "ev_latest", "ev_real", "expected_value", default="")
    p_model = first_present(record, "p_model", "p_model_latest", "model_probability", default="")
    p_market = first_present(record, "p_market", "p_market_latest", "market_probability", default="")

    if p_market == "" and odds != "":
        # Pure display fallback only.
        # This does not affect model pricing, alert selection, thresholds, staking, or ledger.
        p_market = 1.0 / max(as_float(odds, 1.0), 1.0)

    display_decision = "PAPER VALUE" if route == "elite_alerts" else "PAPER REVIEW"

    reasons = [
        first_present(record, "operator_note", "reason", "primary_veto", default=""),
        first_present(record, "bucket_action", "bucket", default=""),
        lifecycle(record),
        first_present(record, "paper_action", default=""),
    ]
    risk_notes = [compact_reason(item, 90) for item in reasons if safe_text(item)]
    risk_notes = list(dict.fromkeys(risk_notes))[:4]
    if not risk_notes:
        risk_notes = ["Paper-only observation", "No real execution", "Ledger protected"]

    return {
        "route": route,
        "rank": safe_text(first_present(record, "rank", default=index)),
        "match": match,
        "home_name": home_name,
        "away_name": away_name,
        "home_logo_url": safe_text(first_present(record, "home_logo", "home_logo_url", default="")),
        "away_logo_url": safe_text(first_present(record, "away_logo", "away_logo_url", default="")),
        "league_name": safe_text(first_present(record, "league_name", "league", "competition", default="Football")),
        "league_logo_url": safe_text(first_present(record, "league_logo", "league_logo_url", default="")),
        "minute": safe_text(first_present(record, "minute", "elapsed", default="")),
        "score_home": score_home,
        "score_away": score_away,
        "market": market,
        "odds": fmt_float(odds, 2) if odds != "" else "N/A",
        "p_model": fmt_pct(p_model) if p_model != "" else "N/A",
        "p_market": fmt_pct(p_market) if p_market != "" else "N/A",
        "edge": fmt_pct(edge, signed=True) if edge != "" else "N/A",
        "ev": fmt_pct(ev, signed=True) if ev != "" else "N/A",
        "lifecycle": lifecycle(record),
        "display_decision": display_decision,
        "risk_notes": risk_notes,
        "raw_record": record,
    }


def logo_html(url: str, name: str) -> str:
    if url:
        return f'<img class="team-logo" src="{esc(url)}" alt="{esc(name)}" />'
    return f'<div class="team-logo-placeholder">{esc(initials(name))}</div>'


def league_logo_html(url: str, name: str) -> str:
    if url:
        return f'<img class="league-logo" src="{esc(url)}" alt="{esc(name)}" />'
    return '<div class="league-logo-placeholder">⚽</div>'


def card_html(alert: dict[str, Any]) -> str:
    route = alert["route"]
    accent = "#57f287" if route == "elite_alerts" else "#fee75c"
    soft = "rgba(87, 242, 135, .12)" if route == "elite_alerts" else "rgba(254, 231, 92, .12)"
    label = "PAPER VALUE DETECTED" if route == "elite_alerts" else "MODEL LOG / REVIEW"

    risk_html = "\n".join(
        f'<div class="risk-item"><span class="dot"></span><span>{esc(note)}</span></div>'
        for note in alert["risk_notes"]
    )

    minute = alert["minute"]
    minute_label = f"{minute}'" if minute else "live"

    return f"""
<article class="alert-card" style="--accent:{accent};--soft:{soft};">
  <div class="topline">
    <div class="bot-icon">◎</div>
    <div class="badge badge-blue">FQIS ALERT BOT</div>
    <div class="badge badge-decision">● {esc(label)}</div>
  </div>

  <section class="match-panel">
    <div class="scoreboard">
      <div class="team">
        {logo_html(alert["home_logo_url"], alert["home_name"])}
        <div class="team-name">{esc(alert["home_name"])}</div>
      </div>

      <div class="score">{esc(alert["score_home"])} - {esc(alert["score_away"])}</div>

      <div class="team">
        {logo_html(alert["away_logo_url"], alert["away_name"])}
        <div class="team-name">{esc(alert["away_name"])}</div>
      </div>
    </div>

    <div class="info-row">
      {league_logo_html(alert["league_logo_url"], alert["league_name"])}
      <span>{esc(alert["league_name"])}</span>
      <span>•</span>
      <span>{esc(minute_label)}</span>
      <span>•</span>
      <span class="ok">PAPER ONLY</span>
      <span>•</span>
      <span class="red">NO REAL BET</span>
    </div>
  </section>

  <section class="grid-odds">
    <div class="box">
      <div class="label">Marché</div>
      <div class="market">{esc(alert["market"])}</div>
    </div>
    <div class="box">
      <div class="odds-label">Cote observée</div>
      <div class="odds">{esc(alert["odds"])}</div>
    </div>
  </section>

  <section class="metric-grid">
    <div class="box">
      <div class="metric-label">P_modèle</div>
      <div class="metric-value blue">{esc(alert["p_model"])}</div>
    </div>
    <div class="box">
      <div class="metric-label">P_marché</div>
      <div class="metric-value">{esc(alert["p_market"])}</div>
    </div>
    <div class="box">
      <div class="metric-label">Edge</div>
      <div class="metric-value green">{esc(alert["edge"])}</div>
    </div>
  </section>

  <section class="decision-grid">
    <div class="box ev-stake">
      <div>
        <div class="metric-label">EV réel</div>
        <div class="big-value green">{esc(alert["ev"])}</div>
      </div>
      <div>
        <div class="metric-label">Stake réel</div>
        <div class="big-value red">OFF</div>
      </div>
    </div>
    <div class="decision-box">
      <div class="decision-small">✓ SIGNAL :</div>
      <div class="decision-main">{esc(alert["display_decision"])}</div>
    </div>
  </section>

  <section class="gate-strip">
    <div class="gate">
      <div class="gate-label">Lifecycle</div>
      <div class="gate-value blue">{esc(alert["lifecycle"])}</div>
    </div>
    <div class="gate">
      <div class="gate-label">Data Freshness</div>
      <div class="gate-value green">CHECKED</div>
    </div>
    <div class="gate">
      <div class="gate-label">Execution</div>
      <div class="gate-value red">DISABLED</div>
    </div>
    <div class="gate">
      <div class="gate-label">Ledger</div>
      <div class="gate-value red">PROTECTED</div>
    </div>
  </section>

  <section class="risk-box">
    <div class="risk-title">▣ NOTES DE RISQUE</div>
    <div class="risk-grid">
      {risk_html}
    </div>
  </section>

  <footer class="footer">
    <span>Mode paper trading • NO STAKE • NO EXECUTION</span>
    <span>Route: {esc(route)} • Rank #{esc(alert["rank"])}</span>
  </footer>
</article>
"""


def build_preview_html(payload: dict[str, Any], alerts: list[dict[str, Any]]) -> str:
    cards = "\n".join(card_html(alert) for alert in alerts[:5])
    if not cards:
        cards = """
<article class="empty-card">
  <h1>Aucune alerte elite sendable</h1>
  <p>PAPER ONLY • NO REAL BET • NO STAKE • NO EXECUTION</p>
</article>
"""

    generated_at = payload.get("generated_at_utc") or utc_now()

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>FQIS Discord Alert Preview</title>
<style>
* {{
  box-sizing: border-box;
}}
body {{
  margin: 0;
  min-height: 100vh;
  background:
    radial-gradient(circle at 15% 0%, rgba(88,101,242,.20), transparent 35%),
    radial-gradient(circle at 95% 18%, rgba(87,242,135,.12), transparent 30%),
    #080d15;
  color: #f3f6fb;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}}
.page {{
  width: 920px;
  margin: 0 auto;
  padding: 32px;
}}
.header {{
  margin-bottom: 22px;
}}
.header h1 {{
  margin: 0 0 8px;
  font-size: 28px;
  letter-spacing: .02em;
}}
.header p {{
  margin: 0;
  color: #9aa5b6;
}}
.alert-card {{
  width: 820px;
  margin-bottom: 24px;
  border-radius: 22px;
  overflow: hidden;
  background: linear-gradient(180deg, #121a28 0%, #0c1420 100%);
  border: 1px solid rgba(148, 163, 184, .22);
  box-shadow: 0 28px 70px rgba(0, 0, 0, .48), inset 4px 0 0 #6d93ff;
  padding: 20px;
}}
.empty-card {{
  width: 820px;
  border-radius: 22px;
  padding: 30px;
  background: #111827;
  border: 1px solid rgba(148, 163, 184, .22);
}}
.topline {{
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 16px;
}}
.bot-icon {{
  width: 48px;
  height: 48px;
  border-radius: 14px;
  background: rgba(109,147,255,.14);
  border: 1px solid rgba(109,147,255,.25);
  display: grid;
  place-items: center;
  color: #8fb0ff;
  font-size: 24px;
  font-weight: 900;
}}
.badge {{
  height: 34px;
  padding: 0 16px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  font-weight: 900;
  letter-spacing: .04em;
  font-size: 14px;
}}
.badge-blue {{
  color: #8fb0ff;
  background: rgba(109,147,255,.12);
  border: 1px solid rgba(109,147,255,.24);
}}
.badge-decision {{
  color: var(--accent);
  background: var(--soft);
  border: 1px solid var(--accent);
}}
.match-panel {{
  border-radius: 18px;
  border: 1px solid rgba(148, 163, 184, .20);
  background: rgba(7, 12, 20, .52);
  padding: 22px;
  margin-bottom: 16px;
}}
.scoreboard {{
  display: grid;
  grid-template-columns: 1fr 190px 1fr;
  align-items: center;
  gap: 20px;
}}
.team {{
  text-align: center;
}}
.team-logo,
.team-logo-placeholder {{
  width: 84px;
  height: 84px;
  margin: 0 auto;
  border-radius: 18px;
  object-fit: contain;
  filter: drop-shadow(0 8px 18px rgba(0,0,0,.35));
}}
.team-logo-placeholder {{
  display: grid;
  place-items: center;
  color: #8fb0ff;
  background: rgba(109,147,255,.13);
  border: 1px solid rgba(109,147,255,.25);
  font-size: 28px;
  font-weight: 950;
}}
.team-name {{
  margin-top: 8px;
  font-size: 22px;
  font-weight: 900;
}}
.score {{
  height: 96px;
  border-radius: 18px;
  background: linear-gradient(180deg, rgba(255,255,255,.055), rgba(255,255,255,.018));
  border: 1px solid rgba(226, 232, 240, .24);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 56px;
  line-height: 1;
  font-weight: 950;
  letter-spacing: .04em;
}}
.info-row {{
  margin-top: 18px;
  padding-top: 16px;
  border-top: 1px solid rgba(148, 163, 184, .16);
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 11px;
  color: #b8c0cc;
  font-size: 15px;
  white-space: nowrap;
}}
.league-logo,
.league-logo-placeholder {{
  width: 31px;
  height: 31px;
  object-fit: contain;
  border-radius: 8px;
}}
.league-logo-placeholder {{
  display: grid;
  place-items: center;
  background: rgba(255,255,255,.08);
}}
.ok {{
  color: #74f284;
  font-weight: 900;
}}
.red {{
  color: #ff5656;
  font-weight: 900;
}}
.green {{
  color: #74f284;
}}
.blue {{
  color: #7798ff;
}}
.grid-odds {{
  display: grid;
  grid-template-columns: 1fr 210px;
  gap: 14px;
  margin-bottom: 14px;
}}
.box {{
  border-radius: 16px;
  border: 1px solid rgba(148, 163, 184, .20);
  background: rgba(7, 12, 20, .48);
  padding: 18px;
}}
.label {{
  color: #8fb0ff;
  font-size: 16px;
  font-weight: 850;
  margin-bottom: 9px;
}}
.market {{
  font-size: 29px;
  font-weight: 950;
}}
.odds-label {{
  color: #cbd2dc;
  text-align: center;
  font-size: 17px;
  font-weight: 750;
}}
.odds {{
  color: #7798ff;
  text-align: center;
  font-size: 44px;
  font-weight: 950;
}}
.metric-grid {{
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 14px;
  margin-bottom: 14px;
}}
.metric-label {{
  color: #c7ceda;
  font-size: 15px;
  margin-bottom: 12px;
}}
.metric-value {{
  font-size: 29px;
  font-weight: 950;
}}
.decision-grid {{
  display: grid;
  grid-template-columns: 1fr 260px;
  gap: 14px;
  margin-bottom: 14px;
}}
.ev-stake {{
  display: grid;
  grid-template-columns: 1fr 1fr;
}}
.ev-stake > div:first-child {{
  border-right: 1px solid rgba(148, 163, 184, .16);
  padding-right: 18px;
}}
.ev-stake > div:last-child {{
  padding-left: 18px;
}}
.big-value {{
  font-size: 32px;
  font-weight: 950;
}}
.decision-box {{
  border-radius: 16px;
  border: 1px solid var(--accent);
  background: var(--soft);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-direction: column;
}}
.decision-small {{
  color: var(--accent);
  font-size: 18px;
  font-weight: 950;
}}
.decision-main {{
  color: var(--accent);
  font-size: 36px;
  font-weight: 1000;
  line-height: 1.02;
  text-align: center;
}}
.gate-strip {{
  border-radius: 16px;
  border: 1px solid rgba(148, 163, 184, .18);
  background: rgba(7, 12, 20, .45);
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  margin-bottom: 14px;
}}
.gate {{
  padding: 16px 14px;
  border-right: 1px solid rgba(148, 163, 184, .14);
}}
.gate:last-child {{
  border-right: 0;
}}
.gate-label {{
  color: #c7ceda;
  font-size: 13px;
  margin-bottom: 3px;
}}
.gate-value {{
  font-size: 16px;
  font-weight: 950;
}}
.risk-box {{
  border-radius: 16px;
  border: 1px solid rgba(148, 163, 184, .18);
  background: rgba(7, 12, 20, .45);
  padding: 18px;
}}
.risk-title {{
  color: #8fb0ff;
  font-size: 17px;
  font-weight: 950;
  letter-spacing: .03em;
  margin-bottom: 14px;
}}
.risk-grid {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  column-gap: 22px;
  row-gap: 10px;
}}
.risk-item {{
  display: flex;
  align-items: center;
  gap: 10px;
  color: #d9dee7;
  font-size: 14px;
}}
.dot {{
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: #74f284;
  box-shadow: 0 0 14px rgba(116,242,132,.35);
  flex: 0 0 auto;
}}
.footer {{
  margin-top: 17px;
  color: #929bad;
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}}
</style>
</head>
<body>
  <main class="page">
    <section class="header">
      <h1>FQIS Discord Alert Preview</h1>
      <p>Generated at {esc(generated_at)} • PAPER ONLY • NO REAL BET • NO STAKE • NO EXECUTION</p>
    </section>
    {cards}
  </main>
</body>
</html>
"""


def build_markdown(payload: dict[str, Any]) -> str:
    elite = payload.get("elite_alerts") or []
    logs = payload.get("model_logs") or []
    no_send = payload.get("no_send") or []

    lines = [
        "# FQIS Discord Alert Renderer",
        "",
        f"- status: **{payload.get('status')}**",
        f"- generated_at_utc: `{payload.get('generated_at_utc')}`",
        f"- elite_alerts_count: **{len(elite)}**",
        f"- model_logs_count: **{len(logs)}**",
        f"- no_send_count: **{len(no_send)}**",
        f"- preview_html: `{payload.get('preview_html')}`",
        "",
        "PAPER ONLY / NO REAL BET / NO STAKE / NO EXECUTION",
        "",
        "## Elite Alerts",
        "",
    ]

    if not elite:
        lines.append("- NONE")
    else:
        for alert in elite:
            lines.append(
                "- #{rank} {match} | {market} @ {odds} | edge {edge} | EV {ev} | {decision}".format(
                    rank=safe_text(alert.get("rank")),
                    match=safe_text(alert.get("match")),
                    market=safe_text(alert.get("market")),
                    odds=safe_text(alert.get("odds")),
                    edge=safe_text(alert.get("edge")),
                    ev=safe_text(alert.get("ev")),
                    decision=safe_text(alert.get("display_decision")),
                )
            )

    lines += [
        "",
        "## Model Logs / Muted Channel",
        "",
    ]

    if not logs:
        lines.append("- NONE")
    else:
        for alert in logs[:10]:
            lines.append(
                "- #{rank} {match} | {market} | lifecycle {life}".format(
                    rank=safe_text(alert.get("rank")),
                    match=safe_text(alert.get("match")),
                    market=safe_text(alert.get("market")),
                    life=safe_text(alert.get("lifecycle")),
                )
            )

    if no_send:
        lines += [
            "",
            "## No Send",
            "",
        ]
        for alert in no_send[:10]:
            lines.append(
                "- #{rank} {match} | {market} | lifecycle {life}".format(
                    rank=safe_text(alert.get("rank")),
                    match=safe_text(alert.get("match")),
                    market=safe_text(alert.get("market")),
                    life=safe_text(alert.get("lifecycle")),
                )
            )

    if payload.get("reasons"):
        lines += [
            "",
            "## Reasons",
            "",
        ]
        for reason in payload.get("reasons") or []:
            lines.append(f"- {safe_text(reason)}")

    return "\n".join(lines) + "\n"


def build_payload() -> dict[str, Any]:
    generated_at_utc = utc_now()
    discord_payload = read_json(DISCORD_PAYLOAD_JSON)

    reasons: list[str] = []
    if discord_payload.get("missing") or discord_payload.get("error"):
        reasons.append("DISCORD_PAYLOAD_MISSING_OR_ERROR")

    payload_status = safe_text(discord_payload.get("status"))
    payload_sendable = bool(discord_payload.get("sendable") is True)
    send_reason = safe_text(discord_payload.get("send_reason"))

    unsafe_hits = unsafe_source_flags(discord_payload)
    if unsafe_hits:
        reasons.append("UNSAFE_SOURCE_FLAGS:" + ",".join(unsafe_hits[:20]))

    raw_records = discord_payload.get("alert_records") or discord_payload.get("ranked_alert_records") or []
    if not isinstance(raw_records, list):
        raw_records = []
        reasons.append("ALERT_RECORDS_NOT_LIST")

    elite_alerts: list[dict[str, Any]] = []
    model_logs: list[dict[str, Any]] = []
    no_send: list[dict[str, Any]] = []

    for index, record in enumerate(raw_records, start=1):
        if not isinstance(record, dict):
            continue

        route = route_record(
            payload_status=payload_status,
            payload_sendable=payload_sendable,
            send_reason=send_reason,
            record=record,
            unsafe_hits=unsafe_hits,
        )
        normalized = normalize_record(record, route, index)

        if route == "elite_alerts":
            elite_alerts.append(normalized)
        elif route == "model_logs":
            model_logs.append(normalized)
        else:
            no_send.append(normalized)

    if not raw_records:
        reasons.append("NO_ALERT_RECORDS_TO_RENDER")

    status = "READY" if not unsafe_hits and not discord_payload.get("missing") and not discord_payload.get("error") else "REVIEW"

    preview_alerts = elite_alerts or model_logs or no_send

    payload = {
        "mode": "FQIS_DISCORD_ALERT_RENDERER",
        "status": status,
        "generated_at_utc": generated_at_utc,
        "source_payload": str(DISCORD_PAYLOAD_JSON),
        "source_payload_status": payload_status,
        "source_payload_sendable": payload_sendable,
        "source_send_reason": send_reason,
        "reasons": reasons,
        "elite_alerts_count": len(elite_alerts),
        "model_logs_count": len(model_logs),
        "no_send_count": len(no_send),
        "preview_html": str(OUT_HTML),
        "elite_alerts": elite_alerts,
        "model_logs": model_logs,
        "no_send": no_send,
        "unsafe_source_flag_paths": unsafe_hits,
        "safety": dict(SAFETY_BLOCK),
        **SAFETY_BLOCK,
        "read": {
            "purpose": "PRESENTATION_ONLY",
            "decision_path_mutated": False,
            "thresholds_changed": False,
            "stake_sizing_performed": False,
            "ledger_mutation_performed": False,
            "bookmaker_execution_performed": False,
            "discord_send_performed": False,
        },
    }

    html_preview = build_preview_html(payload, preview_alerts)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_text(html_preview, encoding="utf-8")

    return payload


def write_outputs(payload: dict[str, Any]) -> None:
    write_json(OUT_JSON, payload)
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(build_markdown(payload), encoding="utf-8")


def main() -> int:
    payload = build_payload()
    write_outputs(payload)
    print(json.dumps({
        "status": payload["status"],
        "elite_alerts_count": payload["elite_alerts_count"],
        "model_logs_count": payload["model_logs_count"],
        "no_send_count": payload["no_send_count"],
        "preview_html": payload["preview_html"],
        "can_execute_real_bets": payload["can_execute_real_bets"],
        "can_enable_live_staking": payload["can_enable_live_staking"],
        "can_mutate_ledger": payload["can_mutate_ledger"],
        "discord_send_performed": payload["read"]["discord_send_performed"],
    }, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
