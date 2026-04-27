# Project Status

## Legacy
- `app/vnext/` = moteur live legacy gelé
- il reste exécutable, testable, observable
- on ne modifie plus sa logique métier sauf bug bloquant

## Target
- `app/fqis/` = nouveau moteur cible
- doctrine:
  1. thesis
  2. tradable universe
  3. pricing
  4. p_real / p_implied / edge / EV
  5. risk
  6. publication

## Rules
- pas de patch opportuniste sur `vnext`
- pas de nouvelle logique de sélection dans `vnext`
- toute nouvelle logique produit/stat/ranking va dans `app/fqis/`
- Discord / ledger / export restent des sorties, pas le centre du moteur

