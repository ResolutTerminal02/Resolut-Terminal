"""
Data Provider Registry — Resolut Asset Management
All providers fixed: Mansa (correct API), Finnhub, ExchangeRate, CoinGecko, Yahoo, Stub.
Smart router picks best per market automatically.
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
    coverage: list = []

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    async def quote(self, ticker: str, market: str) -> dict:
        raise NotImplementedError


class MansaProvider(DataProvider):
    name = "mansa"
    display_name = "Mansa Markets"
    api_key_field = "mansa"
    key_url = "https://mansaapi.com"
    free_tier = True
    coverage = ["DSE", "NSE", "NGX", "GSE", "BRVM", "JSE"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("Mansa key required — email hello@mansamarkets.com")
        async with httpx.AsyncClient(timeout=15, follow_redirects=True, verify=False) as client:
            r = await client.get(
                f"https://mansaapi.com/api/v1/markets/exchanges/{market}/stocks",
                params={"api_key": self.api_key},
            )
        if r.status_code != 200:
            raise RuntimeError(f"Mansa error {r.status_code}: {r.text[:200]}")
        stocks = r.json().get("data", {}).get("stocks", [])
        stock = next((s for s in stocks if s["ticker"].upper() == ticker.upper()), None)
        if not stock:
            raise RuntimeError(f"Mansa: {ticker} not found on {market}")
        return {
            "ticker": ticker, "market": market,
            "price": stock.get("price", 0),
            "change_pct": stock.get("change_pct", 0),
            "volume": stock.get("volume", 0),
            "source": "mansa",
        }


class FinnhubProvider(DataProvider):
    name = "finnhub"
    display_name = "Finnhub (free, real-time NYSE)"
    api_key_field = "finnhub"
    key_url = "https://finnhub.io/register"
    free_tier = True
    coverage = ["NYSE", "NASDAQ", "LSE", "FOREX", "COMEX"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("Finnhub key required — free at finnhub.io")
        symbol = ticker
        if market == "FOREX":
            symbol = f"OANDA:{ticker[:3]}_{ticker[3:]}"
        elif market == "COMEX":
            comex = {"XAUUSD":"OANDA:XAU_USD","XAGUSD":"OANDA:XAG_USD","WTI":"OANDA:BCO_USD","BRENT":"OANDA:BCO_USD"}
            symbol = comex.get(ticker, ticker)
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://finnhub.io/api/v1/quote",
                                 params={"symbol": symbol, "token": self.api_key})
        if r.status_code != 200:
            raise RuntimeError(f"Finnhub {r.status_code}: {r.text[:200]}")
        data = r.json()
        if data.get("c", 0) == 0:
            raise RuntimeError(f"Finnhub: no data for {symbol}")
        price = data["c"]
        prev = data.get("pc", price)
        return {
            "ticker": ticker, "market": market,
            "price": price, "change_pct": round(((price-prev)/prev*100) if prev else 0, 4),
            "high": data.get("h",0), "low": data.get("l",0),
            "open": data.get("o",0), "prev_close": prev,
            "volume": data.get("v",0), "source": "finnhub",
        }


class ExchangeRateProvider(DataProvider):
    name = "exchangerate"
    display_name = "ExchangeRate-API (free FX, incl USD/TZS)"
    api_key_field = "exchangerate"
    key_url = "https://www.exchangerate-api.com"
    free_tier = True
    coverage = ["FOREX"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key:
            raise RuntimeError("ExchangeRate-API key required — free at exchangerate-api.com")
        base, quote_cur = ticker[:3].upper(), ticker[3:].upper()
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://v6.exchangerate-api.com/v6/{self.api_key}/pair/{base}/{quote_cur}")
        if r.status_code != 200:
            raise RuntimeError(f"ExchangeRate-API {r.status_code}")
        data = r.json()
        if data.get("result") != "success":
            raise RuntimeError(f"ExchangeRate-API: {data.get('error-type','unknown')}")
        return {"ticker": ticker, "market": market,
                "price": data["conversion_rate"], "change_pct": 0, "volume": 0,
                "source": "exchangerate"}


class CoinGeckoProvider(DataProvider):
    name = "coingecko"
    display_name = "CoinGecko (free crypto, real-time)"
    api_key_field = "coingecko"
    key_url = "https://www.coingecko.com/en/api"
    free_tier = True
    coverage = ["CRYPTO"]

    SYMBOL_MAP = {
        "BTCUSD":"bitcoin","ETHUSD":"ethereum","SOLUSD":"solana",
        "BNBUSD":"binancecoin","XRPUSD":"ripple","ADAUSD":"cardano",
        "DOGEUSD":"dogecoin","AVAXUSD":"avalanche-2","DOTUSD":"polkadot",
        "LINKUSD":"chainlink",
    }

    async def quote(self, ticker: str, market: str) -> dict:
        coin_id = self.SYMBOL_MAP.get(ticker.upper(), ticker.replace("USD","").lower())
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            r = await client.get("https://api.coingecko.com/api/v3/simple/price",
                params={"ids":coin_id,"vs_currencies":"usd",
                        "include_24hr_change":"true","include_24hr_vol":"true"})
        if r.status_code != 200:
            raise RuntimeError(f"CoinGecko {r.status_code}: {r.text[:200]}")
        data = r.json()
        if coin_id not in data:
            raise RuntimeError(f"CoinGecko: no data for {coin_id}")
        d = data[coin_id]
        return {"ticker": ticker, "market": market,
                "price": d.get("usd",0),
                "change_pct": round(d.get("usd_24h_change",0),4),
                "volume": d.get("usd_24h_vol",0), "source": "coingecko"}


class EODHDProvider(DataProvider):
    name = "eodhd"
    display_name = "EOD Historical Data"
    api_key_field = "eodhd"
    key_url = "https://eodhistoricaldata.com"
    free_tier = False
    coverage = ["DSE","NSE","NYSE","LSE","JSE","NGX"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key: raise RuntimeError("EODHD key required")
        ex_map = {"NYSE":"US","NASDAQ":"US","LSE":"LSE","DSE":"DSE","NSE":"NR","JSE":"JSE","NGX":"NGX"}
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://eodhd.com/api/real-time/{ticker}.{ex_map.get(market,market)}",
                params={"api_token":self.api_key,"fmt":"json"})
        if r.status_code != 200: raise RuntimeError(f"EODHD {r.status_code}")
        data = r.json()
        return {"ticker":ticker,"market":market,"price":data.get("close",0),
                "change_pct":data.get("change_p",0),"volume":data.get("volume",0),"source":"eodhd"}


class AlphaVantageProvider(DataProvider):
    name = "alpha_vantage"
    display_name = "Alpha Vantage"
    api_key_field = "alpha_vantage"
    key_url = "https://www.alphavantage.co/support/#api-key"
    free_tier = True
    coverage = ["NYSE","NASDAQ","LSE","FOREX"]

    async def quote(self, ticker: str, market: str) -> dict:
        if not self.api_key: raise RuntimeError("Alpha Vantage key required")
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://www.alphavantage.co/query",
                params={"function":"GLOBAL_QUOTE","symbol":ticker,"apikey":self.api_key})
        if r.status_code != 200: raise RuntimeError(f"AlphaVantage {r.status_code}")
        data = r.json().get("Global Quote",{})
        if not data: raise RuntimeError("AlphaVantage: no data")
        return {"ticker":ticker,"market":market,
                "price":float(data.get("05. price",0)),
                "change_pct":float(str(data.get("10. change percent","0")).replace("%","")),
                "volume":float(data.get("06. volume",0)),"source":"alpha_vantage"}


class YahooProvider(DataProvider):
    name = "yahoo"
    display_name = "Yahoo Finance (no key)"
    api_key_field = "none"
    key_url = ""
    free_tier = True
    coverage = ["NYSE","NASDAQ","LSE","JSE","MISC"]

    async def quote(self, ticker: str, market: str) -> dict:
        suffix = {"LSE":".L","JSE":".JO","NGX":".LG","NSE":".NR"}.get(market,"")
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent":"Mozilla/5.0"}) as client:
            r = await client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}{suffix}",
                params={"interval":"1d","range":"5d"})
        if r.status_code != 200: raise RuntimeError(f"Yahoo {r.status_code}")
        try:
            meta = r.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice",0)
            prev = meta.get("chartPreviousClose",price)
            return {"ticker":ticker,"market":market,"price":price,
                    "change_pct":((price-prev)/prev*100) if prev else 0,
                    "volume":meta.get("regularMarketVolume",0),"source":"yahoo"}
        except (KeyError,IndexError,TypeError):
            raise RuntimeError("Yahoo: invalid response")


class DSEScraperProvider(DataProvider):
    name = "dse_scraper"
    display_name = "DSE Scraper (no key, free)"
    api_key_field = "none"
    key_url = ""
    free_tier = True
    coverage = ["DSE"]

    async def quote(self, ticker: str, market: str) -> dict:
        if market != "DSE": raise RuntimeError(f"DSE scraper only supports DSE")
        async with httpx.AsyncClient(timeout=15, headers={"User-Agent":"Mozilla/5.0"},
                                     verify=False, follow_redirects=True) as client:
            r = await client.get(f"https://dse.co.tz/equities-statistics/{ticker}/")
        if r.status_code != 200: raise RuntimeError(f"DSE scraper {r.status_code}")
        import re as rex
        html = r.text
        m = (rex.search(r'class="current[\s\S]*?>([\d,]+\.?\d*)',html) or
             rex.search(r'Closing Price[\s\S]*?>([\d,]+\.?\d*)',html) or
             rex.search(r'Price[\s:]*([\d,]+)',html))
        if not m: raise RuntimeError("No price found in DSE response")
        price = float(m.group(1).replace(",",""))
        ch = rex.search(r'([-+]?\d+\.?\d*)\s*%',html)
        return {"ticker":ticker,"market":market,"price":price,
                "change_pct":float(ch.group(1)) if ch else 0.0,"volume":0,"source":"dse_scraper"}


class StubProvider(DataProvider):
    name = "stub"
    display_name = "Stub (offline, deterministic)"
    api_key_field = "none"
    key_url = ""
    free_tier = True
    coverage = ["*"]

    async def quote(self, ticker: str, market: str) -> dict:
        h = sum(ord(c) for c in ticker)
        return {"ticker":ticker,"market":market,
                "price":round(100+(math.sin(h*9301+49297)*0.5+0.5)*200,2),
                "change_pct":round((math.sin(h*137)-0.5)*4,2),"volume":0,"source":"stub"}


PROVIDERS = {
    "mansa":MansaProvider,"finnhub":FinnhubProvider,
    "exchangerate":ExchangeRateProvider,"coingecko":CoinGeckoProvider,
    "eodhd":EODHDProvider,"alpha_vantage":AlphaVantageProvider,
    "yahoo":YahooProvider,"dse_scraper":DSEScraperProvider,"stub":StubProvider,
}


def list_providers():
    return [{"id":cls.name,"name":cls.display_name,"key_field":cls.api_key_field,
             "key_url":cls.key_url,"free_tier":cls.free_tier,"coverage":cls.coverage}
            for cls in PROVIDERS.values()]


def get_provider(name, api_key=""):
    if name not in PROVIDERS: raise ValueError(f"Unknown data provider '{name}'")
    return PROVIDERS[name](api_key=api_key)


async def smart_quote(ticker, market, api_keys):
    import logging
    log = logging.getLogger("resolut")
    routes = {
        "DSE":   [("mansa",api_keys.get("mansa","")),("dse_scraper",""),("stub","")],
        "NSE":   [("mansa",api_keys.get("mansa","")),("yahoo",""),("stub","")],
        "NYSE":  [("finnhub",api_keys.get("finnhub","")),("alpha_vantage",api_keys.get("alpha_vantage","")),("yahoo",""),("stub","")],
        "NASDAQ":[("finnhub",api_keys.get("finnhub","")),("yahoo",""),("stub","")],
        "FOREX": [("exchangerate",api_keys.get("exchangerate","")),("finnhub",api_keys.get("finnhub","")),("stub","")],
        "CRYPTO":[("coingecko",api_keys.get("coingecko","")),("stub","")],
        "COMEX": [("finnhub",api_keys.get("finnhub","")),("stub","")],
    }.get(market,[("yahoo",""),("stub","")])
    for pname, key in routes:
        if pname not in ("stub","yahoo","dse_scraper","coingecko") and not key: continue
        try: return await get_provider(pname,api_key=key).quote(ticker,market)
        except Exception as e:
            log.warning("Router: %s failed %s/%s: %s",pname,market,ticker,e)
    return await StubProvider().quote(ticker,market)
