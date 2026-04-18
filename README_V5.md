# THE BOT V5 — Multi-Market Institutional Engine

Bundle prêt à l'emploi :
1. créer `.venv`
2. installer `requirements.txt`
3. copier `.env.example` vers `.env`
4. remplir la clé API et les webhooks
5. lancer `python main.py --live`

## Marchés couverts
- Over / Under FT
- Over / Under 1H
- BTTS / No BTTS
- Team Totals FT
- 1X2 / ML / Draw
- Correct Score (désactivé en réel par défaut)

## Philosophie
- structure avant prix
- régime avant marché
- calibration avant edge
- meilleur véhicule avant multiplication des tickets
- board réel séparé du board documentaire

## Notes
- `ALLOW_DOCUMENTARY_DISPATCH=false` par défaut
- `CORRECT_SCORE_REAL_ENABLED=false` par défaut
- les payloads sont sérialisés en JSON texte pour éviter les crashes SQLite
