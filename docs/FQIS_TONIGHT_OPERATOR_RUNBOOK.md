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

## Inspect

- `data/pipeline/api_sports/orchestrator/latest_full_cycle_report.md`
- `data/pipeline/api_sports/orchestrator/latest_go_no_go_report.json`
- `data/pipeline/api_sports/orchestrator/latest_shadow_readiness_report.json`
- `data/pipeline/api_sports/decision_bridge_live/latest_live_decisions.json`

## Red Lines

- Do not bet real money.
- Do not flip `live_staking_allowed`.
- Do not set `enforce_quarantine` true tonight.
- Do not mutate `research_candidates_ledger.csv`.

## Green Means

- Full cycle status is `READY`.
- Go/no-go is dry-run only.
- Shadow readiness is `SHADOW_READY`.
- Post-quarantine ROI is positive.
- Research candidates ledger preserved is `true`.

## Stop Means

- Full cycle status is `PARTIAL_FAILURE`.
- Research candidates ledger preserved is `false`.
- `live_staking_allowed` is `true`.
- `can_execute_real_bets` is `true`.
- `go_no_go_state` is `LIVE_READY`.
- Final pipeline fields are missing.
