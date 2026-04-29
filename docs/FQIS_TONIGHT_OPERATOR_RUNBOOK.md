# FQIS Tonight Operator Runbook

## Current Mode

- Paper-only / dry-run / simulation-only.
- Live staking is disabled.
- Real betting execution is forbidden.
- Research ledger mutation is forbidden outside the orchestrator snapshot/restore guard.

## Commands

```powershell
python scripts/fqis_run_full_audit_cycle.py
python scripts/fqis_go_no_go_report.py
python scripts/fqis_shadow_readiness_report.py
```

One-cycle sanity command:

```powershell
python scripts/fqis_tonight_shadow_monitor.py --cycles 1 --sleep-seconds 0 --discord --quiet --tail-lines 20
```

## 10-Minute Monitoring Loop

```powershell
python scripts/fqis_tonight_shadow_monitor.py --cycles 5 --sleep-seconds 120
python scripts/fqis_tonight_shadow_monitor.py --cycles 5 --sleep-seconds 120 --discord
```

Recommended long evening command:

```powershell
python scripts/fqis_tonight_shadow_monitor.py --cycles 180 --sleep-seconds 120 --discord --quiet --tail-lines 20
```

- Use `--discord` only for paper-only alerts.
- Use `--quiet` to capture full-cycle child output in per-cycle logs under `data/pipeline/api_sports/orchestrator/monitor_run_*`.
- Stop if monitor status is `STOPPED`.
- Inspect `data/pipeline/api_sports/orchestrator/latest_tonight_shadow_monitor.md`.

## How To Stop Safely

Press `Ctrl+C` once. Expected monitor status: `MANUALLY_INTERRUPTED`. This is acceptable if `stopped_reason` is `KEYBOARD_INTERRUPT` and all unsafe flags are `false`.

## End-Of-Session Digest

```powershell
python scripts/fqis_tonight_shadow_digest.py
Get-Content data\pipeline\api_sports\orchestrator\latest_tonight_shadow_digest.md
```

## Operator Console

```powershell
python scripts/fqis_operator_shadow_console.py
Get-Content data\pipeline\api_sports\orchestrator\latest_operator_shadow_console.md
```

## Paper Signal Export

```powershell
python scripts/fqis_paper_signal_export.py
Get-Content data\pipeline\api_sports\orchestrator\latest_paper_signal_export.md
```

## Freshness Audit

```powershell
python scripts/fqis_live_freshness_report.py
Get-Content data\pipeline\api_sports\orchestrator\latest_live_freshness_report.md
```

`STALE_REVIEW` is a research-quality warning, not permission to place real bets. Constant PnL is not automatically fatal; inspect freshness flags before interpreting performance.

## Inspect

- `data/pipeline/api_sports/orchestrator/latest_full_cycle_report.md`
- `data/pipeline/api_sports/orchestrator/latest_go_no_go_report.json`
- `data/pipeline/api_sports/orchestrator/latest_shadow_readiness_report.json`
- `data/pipeline/api_sports/orchestrator/latest_tonight_shadow_monitor.md`
- `data/pipeline/api_sports/orchestrator/latest_live_freshness_report.md`
- `data/pipeline/api_sports/orchestrator/latest_operator_shadow_console.md`
- `data/pipeline/api_sports/orchestrator/latest_paper_signal_export.md`
- `data/pipeline/api_sports/orchestrator/latest_discord_paper_payload.md`
- `data/pipeline/api_sports/decision_bridge_live/latest_live_decisions.json`

## Red Lines

- Do not bet real money.
- Do not flip `live_staking_allowed`.
- Do not set `enforce_quarantine` true.
- Do not mutate `research_candidates_ledger.csv`.
- Do not interpret paper alerts as betting instructions.
- Do not run any bookmaker execution.

## Green Means

- Operator state is `PAPER_READY` or `PAPER_REVIEW`.
- Full cycle status is `READY`.
- Go/no-go is `NO_GO_DRY_RUN_ONLY`.
- Shadow readiness is `SHADOW_READY`.
- Research candidates ledger preserved is `true`.
- `can_execute_real_bets` is `false`.
- `can_enable_live_staking` is `false`.
- `live_staking_allowed` is `false`.

## Stop Means

- Operator state is `PAPER_BLOCKED`.
- Monitor status is `STOPPED`.
- Full cycle status is `PARTIAL_FAILURE`.
- Research candidates ledger preserved is `false`.
- `live_staking_allowed` is `true`.
- `can_execute_real_bets` is `true`.
- `go_no_go_state` is `LIVE_READY`.
- `promotion_allowed` is `true`.
