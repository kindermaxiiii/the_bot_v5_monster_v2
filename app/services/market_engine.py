from __future__ import annotations

from math import isfinite
from typing import Iterable

from app.config import settings
from app.core.match_state import MarketQuote, MatchState


class MarketEngine:
    """
    Couche marché robuste V2 :
    - filtre les quotes mortes / suspendues / incohérentes
    - reconstruit les paires 2-way et triplets 3-way
    - calcule les probabilités no-vig
    - distingue clairement analyse cross-book vs exécution same-book
    """

    # ------------------------------------------------------------------
    # Public selectors
    # ------------------------------------------------------------------
    def quotes_for(self, state: MatchState, market_key: str, scope: str = "FT") -> list[MarketQuote]:
        mk = self._norm(market_key)
        sc = self._norm(scope)

        out: list[MarketQuote] = []
        for q in state.quotes:
            if self._norm(getattr(q, "market_key", None)) != mk:
                continue
            if self._norm(getattr(q, "scope", None)) != sc:
                continue
            if not self._is_live_quote(q):
                continue
            out.append(q)
        return out

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _norm(value: str | None) -> str:
        return " ".join((value or "").lower().replace("-", " ").replace("_", " ").split())

    def _safe_odds(self, quote: MarketQuote) -> float:
        try:
            value = float(quote.odds_decimal or 0.0)
        except (TypeError, ValueError):
            return 0.0
        if not isfinite(value):
            return 0.0
        return value

    def _safe_line(self, quote: MarketQuote) -> float | None:
        try:
            if quote.line is None:
                return None
            value = float(quote.line)
        except (TypeError, ValueError):
            return None
        if not isfinite(value):
            return None
        return value

    def _same_bookmaker(self, *quotes: MarketQuote) -> bool:
        books = {self._norm(getattr(q, "bookmaker", None)) for q in quotes if getattr(q, "bookmaker", None)}
        return len(books) == 1 and bool(books)

    def _bookmaker_label(self, *quotes: MarketQuote) -> str:
        labels = []
        for q in quotes:
            book = str(getattr(q, "bookmaker", "") or "").strip()
            if book and book not in labels:
                labels.append(book)
        if not labels:
            return ""
        if len(labels) == 1:
            return labels[0]
        return " / ".join(labels)

    def _is_live_quote(self, quote: MarketQuote) -> bool:
        odds = self._safe_odds(quote)
        if odds <= 1.0:
            return False

        raw = quote.raw or {}

        direct_flags = (
            "is_finished",
            "is_stopped",
            "is_suspended",
            "is_blocked",
            "finished",
            "stopped",
            "suspended",
            "blocked",
            "closed",
        )
        for key in direct_flags:
            if raw.get(key):
                return False

        raw_status = self._norm(
            str(
                raw.get("status")
                or raw.get("market_status")
                or raw.get("selection_status")
                or ""
            )
        )
        if raw_status in {"finished", "stopped", "suspended", "blocked", "closed", "settled"}:
            return False

        return True

    def _side_matches(self, side_value: str | None, aliases: set[str]) -> bool:
        side = self._norm(side_value)
        alias_norm = {self._norm(a) for a in aliases}

        if side in alias_norm:
            return True

        for alias in alias_norm:
            if side.startswith(alias + " "):
                return True
            if (" " + alias + " ") in (" " + side + " "):
                return True
        return False

    def _bookmaker_rank(self, bookmaker: str | None) -> int:
        book = self._norm(bookmaker)

        primary = self._norm(getattr(settings, "primary_bookmaker_name", ""))
        fallback = self._norm(getattr(settings, "fallback_bookmaker_name", ""))
        documentary = self._norm(getattr(settings, "documentary_bookmaker_name", ""))

        if book and book == primary:
            return 3
        if book and book == fallback:
            return 2
        if book and book == documentary:
            return 1
        return 0

    def _main_rank(self, quote: MarketQuote) -> int:
        return 1 if bool(getattr(quote, "is_main", False)) else 0

    # ------------------------------------------------------------------
    # Price state
    # ------------------------------------------------------------------
    def _price_state_two_way(self, overround: float, same_bookmaker: bool, *odds: float) -> str:
        valid_odds = [o for o in odds if isfinite(o) and o > 1.0]
        if not valid_odds or not isfinite(overround) or overround <= 0:
            return "MORT"

        max_odds = max(valid_odds)
        min_odds = min(valid_odds)

        # Same-book = vérité exécutable
        if same_bookmaker:
            if overround <= 1.05 and max_odds >= 1.45 and min_odds >= 1.08:
                return "VIVANT"
            if overround <= 1.08 and max_odds >= 1.28 and min_odds >= 1.06:
                return "VIVANT"
            if overround <= 1.12 and max_odds >= 1.12:
                return "DEGRADE_MAIS_VIVANT"
            if overround <= 1.16:
                return "DEGRADE_MAIS_VIVANT"
            return "MORT"

        # Cross-book = analytique seulement, jamais vraiment "VIVANT"
        if overround <= 1.10 and max_odds >= 1.18:
            return "DEGRADE_MAIS_VIVANT"
        if overround <= 1.14:
            return "DEGRADE_MAIS_VIVANT"
        return "MORT"

    def _price_state_three_way(self, overround: float, same_bookmaker: bool, *odds: float) -> str:
        valid_odds = [o for o in odds if isfinite(o) and o > 1.0]
        if not valid_odds or not isfinite(overround) or overround <= 0:
            return "MORT"

        if same_bookmaker:
            if overround <= 1.10:
                return "VIVANT"
            if overround <= 1.16:
                return "DEGRADE_MAIS_VIVANT"
            return "MORT"

        if overround <= 1.12:
            return "DEGRADE_MAIS_VIVANT"
        return "MORT"

    # ------------------------------------------------------------------
    # Ranking
    # ------------------------------------------------------------------
    def _pair_rank_two_way(self, p: MarketQuote, n: MarketQuote) -> tuple:
        p_odds = self._safe_odds(p)
        n_odds = self._safe_odds(n)

        overround = (1.0 / p_odds) + (1.0 / n_odds)
        same_book = 1 if self._same_bookmaker(p, n) else 0
        book_rank = max(self._bookmaker_rank(p.bookmaker), self._bookmaker_rank(n.bookmaker))
        both_main = 1 if self._main_rank(p) and self._main_rank(n) else 0
        one_main = self._main_rank(p) + self._main_rank(n)

        price_state = self._price_state_two_way(overround, bool(same_book), p_odds, n_odds)
        price_rank = 2 if price_state == "VIVANT" else 1 if price_state == "DEGRADE_MAIS_VIVANT" else 0

        balance_penalty = abs(p_odds - n_odds)

        return (
            same_book,
            price_rank,
            book_rank,
            both_main,
            one_main,
            -overround,
            -balance_penalty,
            max(p_odds, n_odds),
            min(p_odds, n_odds),
        )

    def _triplet_rank_three_way(self, h: MarketQuote, d: MarketQuote, a: MarketQuote) -> tuple:
        h_odds = self._safe_odds(h)
        d_odds = self._safe_odds(d)
        a_odds = self._safe_odds(a)

        overround = (1.0 / h_odds) + (1.0 / d_odds) + (1.0 / a_odds)
        same_book = 1 if self._same_bookmaker(h, d, a) else 0
        book_rank = max(
            self._bookmaker_rank(h.bookmaker),
            self._bookmaker_rank(d.bookmaker),
            self._bookmaker_rank(a.bookmaker),
        )
        main_count = self._main_rank(h) + self._main_rank(d) + self._main_rank(a)

        price_state = self._price_state_three_way(overround, bool(same_book), h_odds, d_odds, a_odds)
        price_rank = 2 if price_state == "VIVANT" else 1 if price_state == "DEGRADE_MAIS_VIVANT" else 0

        return (
            same_book,
            price_rank,
            book_rank,
            main_count,
            -overround,
            max(h_odds, d_odds, a_odds),
            min(h_odds, d_odds, a_odds),
        )

    # ------------------------------------------------------------------
    # Two-way pairing
    # ------------------------------------------------------------------
    def pair_two_way(
        self,
        quotes: Iterable[MarketQuote],
        positive_side_names: set[str],
        negative_side_names: set[str],
        line: float | None = None,
    ) -> dict | None:
        pos_names = {self._norm(s) for s in positive_side_names}
        neg_names = {self._norm(s) for s in negative_side_names}

        rows: list[MarketQuote] = []
        for q in quotes:
            if not self._is_live_quote(q):
                continue

            q_line = self._safe_line(q)
            if line is not None:
                if q_line is None:
                    continue
                if abs(q_line - line) > 1e-6:
                    continue

            rows.append(q)

        pos = [q for q in rows if self._side_matches(q.side, pos_names)]
        neg = [q for q in rows if self._side_matches(q.side, neg_names)]

        if not pos or not neg:
            return None

        best_rank: tuple | None = None
        best_pair: tuple[MarketQuote, MarketQuote] | None = None

        for p in pos:
            p_odds = self._safe_odds(p)
            if p_odds <= 1.0:
                continue

            for n in neg:
                n_odds = self._safe_odds(n)
                if n_odds <= 1.0:
                    continue

                overround = (1.0 / p_odds) + (1.0 / n_odds)
                if not isfinite(overround) or overround <= 0:
                    continue

                rank = self._pair_rank_two_way(p, n)
                if best_rank is None or rank > best_rank:
                    best_rank = rank
                    best_pair = (p, n)

        if best_pair is None:
            return None

        p, n = best_pair
        p_odds = self._safe_odds(p)
        n_odds = self._safe_odds(n)

        inv_p = 1.0 / p_odds
        inv_n = 1.0 / n_odds
        total = inv_p + inv_n
        if not isfinite(total) or total <= 0:
            return None

        same_bookmaker = self._same_bookmaker(p, n)
        line_value = self._safe_line(p)
        if line_value is None:
            line_value = self._safe_line(n)

        price_state = self._price_state_two_way(total, same_bookmaker, p_odds, n_odds)

        return {
            "bookmaker": self._bookmaker_label(p, n),
            "positive_quote": p,
            "negative_quote": n,
            "positive_no_vig": inv_p / total,
            "negative_no_vig": inv_n / total,
            "positive_fair_odds": total / inv_p,
            "negative_fair_odds": total / inv_n,
            "overround": total,
            "margin": total - 1.0,
            "line": line_value,
            "same_bookmaker": same_bookmaker,
            "synthetic_cross_book": not same_bookmaker,
            "bookmaker_rank": max(self._bookmaker_rank(p.bookmaker), self._bookmaker_rank(n.bookmaker)),
            "price_state": price_state,
            "is_executable": same_bookmaker and price_state != "MORT",
        }

    # ------------------------------------------------------------------
    # Three-way pairing
    # ------------------------------------------------------------------
    def pair_three_way(self, quotes: Iterable[MarketQuote]) -> dict | None:
        rows = [q for q in quotes if self._is_live_quote(q)]

        homes = [q for q in rows if self._side_matches(q.side, {"home", "1", "home win"})]
        draws = [q for q in rows if self._side_matches(q.side, {"draw", "x"})]
        aways = [q for q in rows if self._side_matches(q.side, {"away", "2", "away win"})]

        if not homes or not draws or not aways:
            return None

        best_rank: tuple | None = None
        best_triplet: tuple[MarketQuote, MarketQuote, MarketQuote] | None = None

        for h in homes:
            h_odds = self._safe_odds(h)
            if h_odds <= 1.0:
                continue

            for d in draws:
                d_odds = self._safe_odds(d)
                if d_odds <= 1.0:
                    continue

                for a in aways:
                    a_odds = self._safe_odds(a)
                    if a_odds <= 1.0:
                        continue

                    overround = (1.0 / h_odds) + (1.0 / d_odds) + (1.0 / a_odds)
                    if not isfinite(overround) or overround <= 0:
                        continue

                    rank = self._triplet_rank_three_way(h, d, a)
                    if best_rank is None or rank > best_rank:
                        best_rank = rank
                        best_triplet = (h, d, a)

        if best_triplet is None:
            return None

        h, d, a = best_triplet
        h_odds = self._safe_odds(h)
        d_odds = self._safe_odds(d)
        a_odds = self._safe_odds(a)

        inv_h = 1.0 / h_odds
        inv_d = 1.0 / d_odds
        inv_a = 1.0 / a_odds
        total = inv_h + inv_d + inv_a
        if not isfinite(total) or total <= 0:
            return None

        same_bookmaker = self._same_bookmaker(h, d, a)
        price_state = self._price_state_three_way(total, same_bookmaker, h_odds, d_odds, a_odds)

        return {
            "bookmaker": self._bookmaker_label(h, d, a),
            "home_quote": h,
            "draw_quote": d,
            "away_quote": a,
            "home_no_vig": inv_h / total,
            "draw_no_vig": inv_d / total,
            "away_no_vig": inv_a / total,
            "home_fair_odds": total / inv_h,
            "draw_fair_odds": total / inv_d,
            "away_fair_odds": total / inv_a,
            "overround": total,
            "margin": total - 1.0,
            "same_bookmaker": same_bookmaker,
            "synthetic_cross_book": not same_bookmaker,
            "bookmaker_rank": max(
                self._bookmaker_rank(h.bookmaker),
                self._bookmaker_rank(d.bookmaker),
                self._bookmaker_rank(a.bookmaker),
            ),
            "price_state": price_state,
            "is_executable": same_bookmaker and price_state != "MORT",
        }

    # ------------------------------------------------------------------
    # EV
    # ------------------------------------------------------------------
    def expected_value(self, probability: float, odds_decimal: float) -> float:
        if not isfinite(probability) or not isfinite(odds_decimal):
            return 0.0
        if probability <= 0 or odds_decimal <= 1.0:
            return 0.0
        return probability * (odds_decimal - 1.0) - (1.0 - probability)


market_engine = MarketEngine()