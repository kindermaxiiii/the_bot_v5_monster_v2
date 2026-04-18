from __future__ import annotations

from dataclasses import replace

from app.core.match_state import MarketQuote, MatchState


class QuoteAliasAdapter:
    """
    Canonicalize V1 quote aliases into the stricter V2 market vocabulary
    without touching V1 normalizers or MatchState builders.
    """

    _result_aliases = {"1X2", "ML", "MONEYLINE", "RESULT", "RESULT_FT"}
    _btts_aliases = {"BTTS", "BTTS_FT"}
    _team_total_aliases = {"TEAM_TOTAL", "TEAM_TOTAL_FT"}

    _result_side_aliases = {
        "1": "HOME",
        "HOME": "HOME",
        "H": "HOME",
        "X": "DRAW",
        "DRAW": "DRAW",
        "D": "DRAW",
        "2": "AWAY",
        "AWAY": "AWAY",
        "A": "AWAY",
    }

    _btts_side_aliases = {
        "YES": "YES",
        "NO": "NO",
        "BTTS_YES": "YES",
        "BTTS_NO": "NO",
    }

    def _canonical_market_key(self, market_key: str, scope: str) -> str:
        normalized_key = str(market_key or "").upper().strip()
        normalized_scope = str(scope or "FT").upper().strip()

        if normalized_key == "OU":
            if normalized_scope == "1H":
                return "OU_1H"
            if normalized_scope == "FT":
                return "OU_FT"
            return f"OU_{normalized_scope or 'FT'}"

        if normalized_key in self._result_aliases:
            return "RESULT" if normalized_scope == "FT" else f"RESULT_{normalized_scope}"

        if normalized_key in self._btts_aliases:
            return "BTTS" if normalized_scope == "FT" else f"BTTS_{normalized_scope}"

        if normalized_key in self._team_total_aliases:
            return "TEAM_TOTAL" if normalized_scope == "FT" else f"TEAM_TOTAL_{normalized_scope}"

        return normalized_key

    def _canonical_side(self, market_key: str, side: str) -> str:
        normalized_key = str(market_key or "").upper().strip()
        normalized_side = str(side or "").upper().strip()

        if normalized_key == "RESULT":
            return self._result_side_aliases.get(normalized_side, normalized_side)

        if normalized_key == "BTTS":
            return self._btts_side_aliases.get(normalized_side, normalized_side)

        return normalized_side

    def adapt_quote(self, quote: MarketQuote) -> MarketQuote:
        canonical_key = self._canonical_market_key(quote.market_key, quote.scope)
        canonical_side = self._canonical_side(canonical_key, quote.side)
        canonical_scope = str(quote.scope or "FT").upper().strip() or "FT"
        return replace(
            quote,
            market_key=canonical_key,
            side=canonical_side,
            scope=canonical_scope,
        )

    def adapt_state(self, state: MatchState) -> MatchState:
        adapted_quotes = [self.adapt_quote(quote) for quote in (state.quotes or [])]
        return replace(state, quotes=adapted_quotes)
