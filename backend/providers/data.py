"""
Data Provider Registry
======================
Same pattern as AI providers. Switch live data source from Admin Panel.
"""
import math
import httpx
from typing import Optional


class DataProvider:
    name: str = "base"
    display_name: str = "Base"
    api_key_field: str = "api_key"
    key_url: str = ""
    free_tier: bool = False
    coverage: list[str] = []  # market codes covered

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    async def quote(self, ticker: str, market: str) -> dict:
        """Return: {price, change_pct, volume, currency, source}"""
        raise NotImplementedError


class MansaProvider(DataProvider):
    name = "mansa"
    display_name = "Mansa Markets"
    api_key_field = "mansa"
    key_url = "https://mansamarkets.com (email hello@mansamarkets.com)"
    free_tier = True
    coverage = ["DSE", "NSE", "NGX", "GSE", "BRVM", "JSE"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("Mansa API key required")
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://www.mansaapi.com/api/v1/stocks/{ticker}",
                params={"exchange": market},
                headers={"X-API-Key": self.api_key},
            )
        if r.status_code != 200:
            raise RuntimeError(f"Mansa error {r.status_code}: {r.text[:200]}")
        data = r.json()
        return {
            "ticker": ticker, "market": market,
            "price": data.get("price", 0),
            "change_pct": data.get("change_pct", 0),
            "volume": data.get("volume", 0),
            "source": "mansa",
        }


class EODHDProvider(DataProvider):
    name = "eodhd"
    display_name = "EOD Historical Data"
    api_key_field = "eodhd"
    key_url = "https://eodhistoricaldata.com"
    free_tier = False
    coverage = ["DSE", "NSE", "NYSE", "LSE", "JSE", "NGX", "EGX", "MISC"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("EODHD API key required")
        # EODHD uses TICKER.EXCHANGE format (e.g. CRDB.DSE, AAPL.US)
        ex_map = {"NYSE": "US", "NASDAQ": "US", "LSE": "LSE", "DSE": "DSE", "NSE": "NR", "JSE": "JSE", "NGX": "NGX"}
        ex = ex_map.get(market, market)
        symbol = f"{ticker}.{ex}"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://eodhd.com/api/real-time/{symbol}",
                params={"api_token": self.api_key, "fmt": "json"},
            )
        if r.status_code != 200:
            raise RuntimeError(f"EODHD error {r.status_code}: {r.text[:200]}")
        data = r.json()
        return {
            "ticker": ticker, "market": market,
            "price": data.get("close", 0),
            "change_pct": data.get("change_p", 0),
            "volume": data.get("volume", 0),
            "source": "eodhd",
        }


class AlphaVantageProvider(DataProvider):
    name = "alpha_vantage"
    display_name = "Alpha Vantage"
    api_key_field = "alpha_vantage"
    key_url = "https://www.alphavantage.co/support/#api-key"
    free_tier = True
    coverage = ["NYSE", "NASDAQ", "LSE", "FOREX"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("Alpha Vantage key required")
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://www.alphavantage.co/query",
                params={
                    "function": "GLOBAL_QUOTE",
                    "symbol": ticker,
                    "apikey": self.api_key,
                },
            )
        if r.status_code != 200:
            raise RuntimeError(f"AlphaVantage error {r.status_code}")
        data = r.json().get("Global Quote", {})
        if not data:
            raise RuntimeError("AlphaVantage: no data (rate limited or symbol not found)")
        price = float(data.get("05. price", 0))
        change_pct = float(str(data.get("10. change percent", "0")).replace("%", ""))
        volume = float(data.get("06. volume", 0))
        return {
            "ticker": ticker, "market": market,
            "price": price, "change_pct": change_pct, "volume": volume,
            "source": "alpha_vantage",
        }


class YahooProvider(DataProvider):
    """Yahoo Finance — unofficial, free, no key. Best-effort coverage."""
    name = "yahoo"
    display_name = "Yahoo Finance (no key)"
    api_key_field = "none"
    key_url = ""
    free_tier = True
    coverage = ["NYSE", "NASDAQ", "LSE", "JSE", "MISC"]

    async def quote(self, ticker: str, market: str) -> dict:
        # Yahoo symbol format quirks
        suffix_map = {"LSE": ".L", "JSE": ".JO", "NGX": ".LG", "NSE": ".NR"}
        symbol = ticker + suffix_map.get(market, "")
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            r = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "5d"},
            )
        if r.status_code != 200:
            raise RuntimeError(f"Yahoo error {r.status_code}")
        data = r.json()
        try:
            result = data["chart"]["result"][0]
            meta = result["meta"]
            price = meta.get("regularMarketPrice", 0)
            prev = meta.get("chartPreviousClose", price)
            change_pct = ((price - prev) / prev * 100) if prev else 0
            return {
                "ticker": ticker, "market": market,
                "price": price, "change_pct": change_pct,
                "volume": meta.get("regularMarketVolume", 0),
                "source": "yahoo",
            }
        except (KeyError, IndexError, TypeError):
            raise RuntimeError("Yahoo: invalid response")


class StubProvider(DataProvider):
    """Deterministic offline fallback. No real data — for development/testing only."""
    name = "stub"
    display_name = "Stub (offline, deterministic)"
    api_key_field = "none"
    key_url = ""
    free_tier = True
    coverage = ["*"]

    async def quote(self, ticker: str, market: str) -> dict:
        h = sum(ord(c) for c in ticker)
        price = round(100 + (math.sin(h * 9301 + 49297) * 0.5 + 0.5) * 200, 2)
        change = round((math.sin(h * 137) - 0.5) * 4, 2)
        return {
            "ticker": ticker, "market": market,
            "price": price, "change_pct": change, "volume": 0,
            "source": "stub",
        }


# ══════════════════════════════════════════════════════════════════
PROVIDERS = {
    "mansa":         MansaProvider,
    "eodhd":         EODHDProvider,
    "alpha_vantage": AlphaVantageProvider,
    "yahoo":         YahooProvider,
    "stub":          StubProvider,
}


def list_providers() -> list[dict]:
    return [
        {
            "id": cls.name,
            "name": cls.display_name,
            "key_field": cls.api_key_field,
            "key_url": cls.key_url,
            "free_tier": cls.free_tier,
            "coverage": cls.coverage,
        }
        for cls in PROVIDERS.values()
    ]


def get_provider(name: str, api_key: str = "") -> DataProvider:
    if name not in PROVIDERS:
        raise ValueError(f"Unknown data provider '{name}'")
    return PROVIDERS[name](api_key=api_key)


class DSEScraperProvider(DataProvider):
    """DSE web scraper — scrapes dse.co.tz for live DSE prices.
    No API key needed. Fallback when Mansa key isn't available."""
    name = "dse_scraper"
    display_name = "DSE Scraper (no key, free)"
    api_key_field = "none"
    key_url = ""
    free_tier = True
    coverage = ["DSE"]

    async def quote(self, ticker: str, market: str) -> dict:
        if market != "DSE":
            raise RuntimeError(f"DSE scraper only supports DSE market, got {market}")
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}, verify=False) as client:
            try:
                # DSE publishes daily price data on this endpoint
                r = await client.get(f"https://www.dse.co.tz/equities-statistics/{ticker}/")
                if r.status_code != 200:
                    # Try alternative DSE URL
                    r = await client.get(f"https://dse.co.tz/equities-statistics/{ticker}/")
                if r.status_code != 200:
                    raise RuntimeError(f"DSE returned {r.status_code}")
                # Extract price from HTML — DSE puts current price in known places
                html = r.text
                import re as rex
                # Look for current price patterns
                m = rex.search(r'class="current[\s\S]*?>([\d,]+\.\d+)', html)
                if not m:
                    m = rex.search(r'Closing Price[\s\S]*?>([\d,]+\.\d+)', html)
                if not m:
                    m = rex.search(r'Price[\s:]*([\d,]+)', html)
                if not m:
                    raise RuntimeError("No price found in DSE response")
                price = float(m.group(1).replace(",", ""))
                # Look for change %
                ch_m = rex.search(r'change[\s\S]*?([-+]?\d+\.\d+)\s*%', html)
                change_pct = float(ch_m.group(1)) if ch_m else 0.0
                return {
                    "ticker": ticker, "market": market,
                    "price": price, "change_pct": change_pct,
                    "volume": 0, "source": "dse_scraper",
                }
            except Exception as e:
                raise RuntimeError(f"DSE scraper: {e}")


# Register the scraper
PROVIDERS["dse_scraper"] = DSEScraperProvider


# ──────────────────────────────────────────────────────────────────
class FinnhubProvider(DataProvider):
    """Finnhub — real-time NYSE/NASDAQ prices + global coverage. Free tier: 60 calls/min."""
    name = "finnhub"
    display_name = "Finnhub (free, real-time NYSE)"
    api_key_field = "finnhub"
    key_url = "https://finnhub.io/register"
    free_tier = True
    coverage = ["NYSE", "NASDAQ", "LSE", "FOREX", "CRYPTO"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("Finnhub API key required — get free key at finnhub.io")
        # Finnhub symbol format
        symbol = ticker
        if market == "FOREX":
            # Finnhub FX format: OANDA:EUR_USD
            base = ticker[:3]
            quote = ticker[3:]
            symbol = f"OANDA:{base}_{quote}"
        elif market == "CRYPTO":
            symbol = f"BINANCE:{ticker.replace('USD','_USDT')}"

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": symbol, "token": self.api_key},
            )
        if r.status_code != 200:
            raise RuntimeError(f"Finnhub error {r.status_code}: {r.text[:200]}")
        data = r.json()
        if data.get("c", 0) == 0:
            raise RuntimeError(f"Finnhub: no data for {symbol}")
        price = data["c"]       # current price
        prev  = data.get("pc", price)  # previous close
        change_pct = ((price - prev) / prev * 100) if prev else 0
        return {
            "ticker": ticker, "market": market,
            "price": price,
            "change_pct": round(change_pct, 4),
            "high": data.get("h", 0),
            "low": data.get("l", 0),
            "open": data.get("o", 0),
            "prev_close": prev,
            "volume": data.get("v", 0),
            "source": "finnhub",
        }


PROVIDERS["finnhub"] = FinnhubProvider


# ──────────────────────────────────────────────────────────────────
class ExchangeRateProvider(DataProvider):
    """ExchangeRate-API — live FX rates including USD/TZS, USD/KES, all major pairs. Free: 1500 calls/month."""
    name = "exchangerate"
    display_name = "ExchangeRate-API (free FX, incl USD/TZS)"
    api_key_field = "exchangerate"
    key_url = "https://www.exchangerate-api.com"
    free_tier = True
    coverage = ["FOREX"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("ExchangeRate-API key required — free at exchangerate-api.com")
        # ticker format: EURUSD, USDTZS, USDKES etc
        base  = ticker[:3].upper()
        quote = ticker[3:].upper()
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://v6.exchangerate-api.com/v6/{self.api_key}/pair/{base}/{quote}"
            )
        if r.status_code != 200:
            raise RuntimeError(f"ExchangeRate-API error {r.status_code}")
        data = r.json()
        if data.get("result") != "success":
            raise RuntimeError(f"ExchangeRate-API: {data.get('error-type', 'unknown error')}")
        rate = data["conversion_rate"]
        return {
            "ticker": ticker, "market": market,
            "price": rate,
            "change_pct": 0,  # free tier doesn't include historical for change calc
            "volume": 0,
            "source": "exchangerate",
        }


PROVIDERS["exchangerate"] = ExchangeRateProvider


# ──────────────────────────────────────────────────────────────────
class CoinGeckoProvider(DataProvider):
    """CoinGecko — real-time crypto prices. Free tier works without key (limited), key extends limits."""
    name = "coingecko"
    display_name = "CoinGecko (free crypto, real-time)"
    api_key_field = "coingecko"
    key_url = "https://www.coingecko.com/en/api"
    free_tier = True
    coverage = ["CRYPTO"]

    # Map common symbols to CoinGecko IDs
    SYMBOL_MAP = {
        "BTCUSD": "bitcoin", "ETHUSD": "ethereum", "SOLUSD": "solana",
        "BNBUSD": "binancecoin", "XRPUSD": "ripple", "ADAUSD": "cardano",
        "DOGEUSD": "dogecoin", "AVAXUSD": "avalanche-2", "DOTUSD": "polkadot",
        "LINKUSD": "chainlink", "MATICUSD": "matic-network", "UNIUSD": "uniswap",
    }

    async def quote(self, ticker: str, market: str) -> dict:
        coin_id = self.SYMBOL_MAP.get(ticker.upper())
        if not coin_id:
            # Try to derive from ticker (e.g. BTCUSD → bitcoin)
            coin_id = ticker.replace("USD", "").lower()

        headers = {}
        if self.api_key:
            headers["x-cg-demo-api-key"] = self.api_key

        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            r = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={
                    "ids": coin_id,
                    "vs_currencies": "usd",
                    "include_24hr_change": "true",
                    "include_24hr_vol": "true",
                },
            )
        if r.status_code != 200:
            raise RuntimeError(f"CoinGecko error {r.status_code}: {r.text[:200]}")
        data = r.json()
        if coin_id not in data:
            raise RuntimeError(f"CoinGecko: no data for {coin_id}")
        d = data[coin_id]
        return {
            "ticker": ticker, "market": market,
            "price": d.get("usd", 0),
            "change_pct": round(d.get("usd_24h_change", 0), 4),
            "volume": d.get("usd_24h_vol", 0),
            "source": "coingecko",
        }


PROVIDERS["coingecko"] = CoinGeckoProvider


# ══════════════════════════════════════════════════════════════════
# SMART ROUTER — automatically picks the best available provider
# per instrument type, falls back gracefully
# ══════════════════════════════════════════════════════════════════
async def smart_quote(ticker: str, market: str, api_keys: dict) -> dict:
    """
    Intelligent data routing:
      DSE  → DSE Scraper (free, no key) → Mansa (if key) → Stub
      NSE  → Mansa (if key) → Yahoo → Stub
      NYSE → Finnhub (if key) → Yahoo → Stub
      FOREX→ ExchangeRate-API (if key) → Stub
      CRYPTO→ CoinGecko (free) → Stub
      COMEX→ Finnhub (if key) → Stub
    """
    kind_routes = {
        "DSE":     [
            ("dse_scraper", ""),
            ("mansa",        api_keys.get("mansa", "")),
            ("yahoo",        ""),
            ("stub",         ""),
        ],
        "NSE":     [
            ("mansa",        api_keys.get("mansa", "")),
            ("yahoo",        ""),
            ("stub",         ""),
        ],
        "NYSE":    [
            ("finnhub",      api_keys.get("finnhub", "")),
            ("alpha_vantage",api_keys.get("alpha_vantage", "")),
            ("yahoo",        ""),
            ("stub",         ""),
        ],
        "NASDAQ":  [
            ("finnhub",      api_keys.get("finnhub", "")),
            ("yahoo",        ""),
            ("stub",         ""),
        ],
        "FOREX":   [
            ("exchangerate", api_keys.get("exchangerate", "")),
            ("finnhub",      api_keys.get("finnhub", "")),
            ("stub",         ""),
        ],
        "CRYPTO":  [
            ("coingecko",    api_keys.get("coingecko", "")),
            ("stub",         ""),
        ],
        "COMEX":   [
            ("finnhub",      api_keys.get("finnhub", "")),
            ("stub",         ""),
        ],
    }

    routes = kind_routes.get(market, [("yahoo", ""), ("stub", "")])

    for provider_name, key in routes:
        # Skip if this provider needs a key and we don't have one
        if provider_name not in ("stub", "yahoo", "dse_scraper", "coingecko") and not key:
            continue
        try:
            provider = get_provider(provider_name, api_key=key)
            result = await provider.quote(ticker, market)
            return result
        except Exception as e:
            import logging
            logging.getLogger("resolut").warning(
                "Smart router: %s failed for %s/%s: %s — trying next",
                provider_name, market, ticker, e
            )
            continue

    # Final fallback — deterministic stub
    return await StubProvider().quote(ticker, market)

