# FQIS Niveau 1 — Operator Runbook

## Mission

Niveau 1 is a shadow-production layer.

It is designed to run the FQIS pipeline on real or fixture-like inputs, create audit artifacts, track readiness, and expose operational status.

Niveau 1 is not authorized for real-money staking, automated betting, or unattended execution.

## Primary command

```powershell
.\.venv\Scripts\python.exe scripts\fqis_shadow.py --profile demo
Operator checklist command: .\.venv\Scripts\python.exe scripts\fqis_level1_checklist.py --profile demo

No real-money staking
