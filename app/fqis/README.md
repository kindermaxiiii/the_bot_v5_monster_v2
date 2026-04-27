# FQIS Engine

## Doctrine
Le moteur FQIS ne choisit pas un template.
Il :
1. lit le match,
2. produit une thèse statistique,
3. recense les marchés réellement tradables,
4. price plusieurs véhicules,
5. calcule `p_real`, `p_implied`, `edge`, `EV`,
6. applique le risque,
7. publie ou ne publie pas.

## Contraste avec le legacy
Le legacy `app/vnext/` reste gelé.
Il continue de servir de référence comportementale, mais il n'est plus la cible d'évolution.