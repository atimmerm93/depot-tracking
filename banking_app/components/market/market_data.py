from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import requests


class MarketDataError(RuntimeError):
    pass


@dataclass
class QuoteResult:
    symbol: str
    value: float
    currency: str = "EUR"


@dataclass
class HistoricalPriceResult:
    symbol: str
    value: float
    currency: str
    price_date: date


class YahooMarketDataClient:
    search_url = "https://query2.finance.yahoo.com/v1/finance/search"
    chart_url_template = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

    def __init__(self, timeout_seconds: float = 12.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "application/json",
            }
        )

    def resolve_symbol(self, *, wkn: str, isin: str | None, name: str | None, ticker: str | None) -> str:
        if ticker:
            return ticker

        candidates = [item for item in [isin, wkn, name] if item]
        for query in candidates:
            payload = self._search(query)
            symbol = self._pick_symbol(payload)
            if symbol:
                return symbol

        joined = ", ".join([candidate for candidate in candidates if candidate])
        raise MarketDataError(f"No Yahoo symbol found for {joined}")

    def fetch_price(self, symbol: str) -> QuoteResult:
        url = self.chart_url_template.format(symbol=symbol)
        response = self.session.get(
            url,
            params={"range": "1d", "interval": "1d"},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()

        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if not result:
            raise MarketDataError(f"No chart result for symbol {symbol}")

        meta = result.get("meta") or {}
        currency = self._normalize_currency(meta.get("currency"))
        regular_price = meta.get("regularMarketPrice")
        if regular_price is not None:
            value = float(regular_price)
            currency, value = self._apply_minor_unit_scaling(currency, value)
            return QuoteResult(symbol=symbol, value=value, currency=currency)

        indicators = result.get("indicators") or {}
        quote = (indicators.get("quote") or [{}])[0]
        closes = quote.get("close") or []
        for close in reversed(closes):
            if close is not None:
                value = float(close)
                currency, value = self._apply_minor_unit_scaling(currency, value)
                return QuoteResult(symbol=symbol, value=value, currency=currency)

        raise MarketDataError(f"No price available for symbol {symbol}")

    def fetch_quote(self, *, wkn: str, isin: str | None, name: str | None, ticker: str | None) -> QuoteResult:
        symbol = self.resolve_symbol(wkn=wkn, isin=isin, name=name, ticker=ticker)
        return self.fetch_price(symbol)

    def fetch_fx_rate(self, *, base_currency: str, quote_currency: str = "EUR") -> float:
        base = self._normalize_currency(base_currency)
        quote = self._normalize_currency(quote_currency)
        if base == quote:
            return 1.0

        direct_symbol = f"{base}{quote}=X"
        try:
            direct = self.fetch_price(direct_symbol)
            if direct.value <= 0:
                raise MarketDataError(f"Invalid FX quote for {direct_symbol}: {direct.value}")
            return direct.value
        except (MarketDataError, requests.RequestException):
            inverse_symbol = f"{quote}{base}=X"
            inverse = self.fetch_price(inverse_symbol)
            if inverse.value <= 0:
                raise MarketDataError(f"Invalid FX quote for {inverse_symbol}: {inverse.value}")
            return 1.0 / inverse.value

    def fetch_historical_fx_rate(
        self, *, base_currency: str, on_date: date, quote_currency: str = "EUR", lookback_days: int = 14, lookahead_days: int = 5
    ) -> float:
        base = self._normalize_currency(base_currency)
        quote = self._normalize_currency(quote_currency)
        if base == quote:
            return 1.0

        direct_symbol = f"{base}{quote}=X"
        try:
            direct = self.fetch_historical_quote(
                direct_symbol,
                on_date,
                lookback_days=lookback_days,
                lookahead_days=lookahead_days,
            )
            if direct.value <= 0:
                raise MarketDataError(f"Invalid historical FX quote for {direct_symbol}: {direct.value}")
            return direct.value
        except (MarketDataError, requests.RequestException):
            inverse_symbol = f"{quote}{base}=X"
            inverse = self.fetch_historical_quote(
                inverse_symbol,
                on_date,
                lookback_days=lookback_days,
                lookahead_days=lookahead_days,
            )
            if inverse.value <= 0:
                raise MarketDataError(f"Invalid historical FX quote for {inverse_symbol}: {inverse.value}")
            return 1.0 / inverse.value

    def fetch_historical_quote(
        self, symbol: str, on_date: date, *, lookback_days: int = 14, lookahead_days: int = 5
    ) -> HistoricalPriceResult:
        points, currency = self._fetch_historical_points(
            symbol,
            on_date,
            lookback_days=lookback_days,
            lookahead_days=lookahead_days,
        )

        before_or_equal = [item for item in points if item[0] <= on_date]
        if before_or_equal:
            before_or_equal.sort(key=lambda item: item[0], reverse=True)
            selected_date, selected_value = before_or_equal[0]
        else:
            points.sort(key=lambda item: item[0])
            selected_date, selected_value = points[0]

        currency, selected_value = self._apply_minor_unit_scaling(currency, selected_value)
        return HistoricalPriceResult(
            symbol=symbol,
            value=selected_value,
            currency=currency,
            price_date=selected_date,
        )

    def fetch_historical_price(self, symbol: str, on_date: date, *, lookback_days: int = 14, lookahead_days: int = 5) -> float:
        result = self.fetch_historical_quote(
            symbol,
            on_date,
            lookback_days=lookback_days,
            lookahead_days=lookahead_days,
        )
        return result.value

    def _fetch_historical_points(
        self, symbol: str, on_date: date, *, lookback_days: int = 14, lookahead_days: int = 5
    ) -> tuple[list[tuple[date, float]], str]:
        start_dt = datetime.combine(on_date - timedelta(days=lookback_days), datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(on_date + timedelta(days=lookahead_days), datetime.max.time(), tzinfo=timezone.utc)

        response = self.session.get(
            self.chart_url_template.format(symbol=symbol),
            params={
                "period1": int(start_dt.timestamp()),
                "period2": int(end_dt.timestamp()),
                "interval": "1d",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()

        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if not result:
            raise MarketDataError(f"No chart result for historical symbol {symbol}")

        meta = result.get("meta") or {}
        currency = self._normalize_currency(meta.get("currency"))
        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        quote = (indicators.get("quote") or [{}])[0]
        closes = quote.get("close") or []
        if not timestamps or not closes:
            raise MarketDataError(f"No historical prices for symbol {symbol}")

        points: list[tuple[date, float]] = []
        for ts, close in zip(timestamps, closes):
            if close is None:
                continue
            d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
            points.append((d, float(close)))
        if not points:
            raise MarketDataError(f"No historical close values for symbol {symbol}")
        return points, currency

    def _search(self, query: str) -> dict:
        response = self.session.get(
            self.search_url,
            params={"q": query, "quotesCount": 10, "newsCount": 0},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _pick_symbol(payload: dict) -> str | None:
        valid_types = {"EQUITY", "ETF", "MUTUALFUND", "INDEX", "CRYPTOCURRENCY"}
        quotes = payload.get("quotes") or []
        for item in quotes:
            symbol = item.get("symbol")
            quote_type = item.get("quoteType")
            if symbol and quote_type in valid_types:
                return str(symbol)
        return None

    @staticmethod
    def _normalize_currency(currency: str | None) -> str:
        if not currency:
            return "EUR"
        raw = str(currency).strip()
        if raw in {"GBp", "GBX", "GBx", "GBp.", "GBPx"}:
            return "GBX"
        upper = raw.upper()
        if upper in {"GB PENCE", "GBPENCE", "GB P"}:
            return "GBX"
        return upper

    @staticmethod
    def _apply_minor_unit_scaling(currency: str, value: float) -> tuple[str, float]:
        # Some Yahoo instruments are quoted in pence (GBp/GBX) instead of GBP.
        if currency in {"EUR", "USD", "CHF", "JPY", "CAD", "AUD", "NOK", "SEK", "DKK", "GBP"}:
            return currency, value
        if currency == "GBX":
            return "GBP", value / 100.0
        if currency == "GBP.":
            return "GBP", value
        return currency, value
