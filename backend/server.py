"""
RESOLUT ASSET MANAGEMENT — Terminal Backend
============================================
Self-contained FastAPI server. No external data/ or instruments_seed modules.
All instrument data is inline. All providers are in providers/ subfolder.
"""
import os
import json
import sqlite3
import logging
import uuid as uuidlib
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from typing import Optional
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from providers import ai as ai_registry
from providers import data as data_registry
from providers import broker as broker_registry

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "resolut.db"
CONFIG_PATH = BASE_DIR / "config.json"
FRONTEND_DIR = BASE_DIR / "frontend"
EAT = timezone(timedelta(hours=3))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("resolut")

# ══════════════════════════════════════════════════════════════════
# INSTRUMENTS — fully inline, no external module
# ══════════════════════════════════════════════════════════════════
INSTRUMENTS = [
    # DSE
    {"symbol":"CRDB","kind":"equity","market":"DSE","name":"CRDB Bank","sector":"Financials"},
    {"symbol":"NMB","kind":"equity","market":"DSE","name":"NMB Bank","sector":"Financials"},
    {"symbol":"VODA","kind":"equity","market":"DSE","name":"Vodacom Tanzania","sector":"Telecom"},
    {"symbol":"TBL","kind":"equity","market":"DSE","name":"Tanzania Breweries","sector":"Consumer Goods"},
    {"symbol":"TPCC","kind":"equity","market":"DSE","name":"Tanzania Portland Cement","sector":"Industrials"},
    {"symbol":"DCB","kind":"equity","market":"DSE","name":"DCB Commercial Bank","sector":"Financials"},
    {"symbol":"MKCB","kind":"equity","market":"DSE","name":"Mkombozi Commercial Bank","sector":"Financials"},
    {"symbol":"JHL","kind":"equity","market":"DSE","name":"Jubilee Holdings","sector":"Financials"},
    {"symbol":"SWIS","kind":"equity","market":"DSE","name":"Swissport Tanzania","sector":"Industrials"},
    {"symbol":"TOL","kind":"equity","market":"DSE","name":"TOL Gases","sector":"Materials"},
    {"symbol":"TCC","kind":"equity","market":"DSE","name":"Tanzania Cigarette Company","sector":"Consumer Goods"},
    {"symbol":"TCCL","kind":"equity","market":"DSE","name":"Tanga Cement","sector":"Industrials"},
    {"symbol":"PAL","kind":"equity","market":"DSE","name":"Precision Air","sector":"Industrials"},
    {"symbol":"EABL","kind":"equity","market":"DSE","name":"East African Breweries","sector":"Consumer Goods"},
    {"symbol":"KCB","kind":"equity","market":"DSE","name":"KCB Group (cross-listed)","sector":"Financials"},
    {"symbol":"NMG","kind":"equity","market":"DSE","name":"Nation Media Group","sector":"Media"},
    {"symbol":"SWALA","kind":"equity","market":"DSE","name":"Swala Oil and Gas","sector":"Energy"},
    {"symbol":"YETU","kind":"equity","market":"DSE","name":"Yetu Microfinance","sector":"Financials"},
    {"symbol":"MUCOBA","kind":"equity","market":"DSE","name":"Mucoba Bank","sector":"Financials"},
    {"symbol":"MCB","kind":"equity","market":"DSE","name":"Mwalimu Commercial Bank","sector":"Financials"},
    {"symbol":"DSEI","kind":"index","market":"DSE","name":"DSE All Share Index","sector":"Index"},
    {"symbol":"TSI","kind":"index","market":"DSE","name":"Tanzania Share Index","sector":"Index"},
    # NSE
    {"symbol":"EQTY","kind":"equity","market":"NSE","name":"Equity Group Holdings","sector":"Financials"},
    {"symbol":"SCOM","kind":"equity","market":"NSE","name":"Safaricom","sector":"Telecom"},
    {"symbol":"KCB","kind":"equity","market":"NSE","name":"KCB Group","sector":"Financials"},
    {"symbol":"COOP","kind":"equity","market":"NSE","name":"Co-operative Bank","sector":"Financials"},
    {"symbol":"ABSA","kind":"equity","market":"NSE","name":"Absa Bank Kenya","sector":"Financials"},
    {"symbol":"NCBA","kind":"equity","market":"NSE","name":"NCBA Group","sector":"Financials"},
    {"symbol":"DTBK","kind":"equity","market":"NSE","name":"Diamond Trust Bank","sector":"Financials"},
    {"symbol":"BRIT","kind":"equity","market":"NSE","name":"Britam Holdings","sector":"Financials"},
    {"symbol":"JUB","kind":"equity","market":"NSE","name":"Jubilee Holdings","sector":"Financials"},
    {"symbol":"EABL","kind":"equity","market":"NSE","name":"East African Breweries","sector":"Consumer Goods"},
    {"symbol":"BAT","kind":"equity","market":"NSE","name":"BAT Kenya","sector":"Consumer Goods"},
    {"symbol":"BAMB","kind":"equity","market":"NSE","name":"Bamburi Cement","sector":"Industrials"},
    {"symbol":"KPLC","kind":"equity","market":"NSE","name":"Kenya Power","sector":"Utilities"},
    {"symbol":"KQ","kind":"equity","market":"NSE","name":"Kenya Airways","sector":"Industrials"},
    {"symbol":"NMG","kind":"equity","market":"NSE","name":"Nation Media Group","sector":"Media"},
    {"symbol":"NASI","kind":"index","market":"NSE","name":"NSE All Share Index","sector":"Index"},
    {"symbol":"NSE20","kind":"index","market":"NSE","name":"NSE 20 Share Index","sector":"Index"},
    # NYSE / NASDAQ
    {"symbol":"AAPL","kind":"equity","market":"NYSE","name":"Apple Inc.","sector":"Technology"},
    {"symbol":"MSFT","kind":"equity","market":"NYSE","name":"Microsoft Corp.","sector":"Technology"},
    {"symbol":"GOOGL","kind":"equity","market":"NYSE","name":"Alphabet Inc. A","sector":"Technology"},
    {"symbol":"GOOG","kind":"equity","market":"NYSE","name":"Alphabet Inc. C","sector":"Technology"},
    {"symbol":"AMZN","kind":"equity","market":"NYSE","name":"Amazon.com","sector":"Consumer Discretionary"},
    {"symbol":"META","kind":"equity","market":"NYSE","name":"Meta Platforms","sector":"Technology"},
    {"symbol":"NVDA","kind":"equity","market":"NYSE","name":"NVIDIA Corp.","sector":"Technology"},
    {"symbol":"TSLA","kind":"equity","market":"NYSE","name":"Tesla Inc.","sector":"Consumer Discretionary"},
    {"symbol":"AVGO","kind":"equity","market":"NYSE","name":"Broadcom Inc.","sector":"Technology"},
    {"symbol":"ORCL","kind":"equity","market":"NYSE","name":"Oracle Corp.","sector":"Technology"},
    {"symbol":"CRM","kind":"equity","market":"NYSE","name":"Salesforce Inc.","sector":"Technology"},
    {"symbol":"ADBE","kind":"equity","market":"NYSE","name":"Adobe Inc.","sector":"Technology"},
    {"symbol":"AMD","kind":"equity","market":"NYSE","name":"Advanced Micro Devices","sector":"Technology"},
    {"symbol":"INTC","kind":"equity","market":"NYSE","name":"Intel Corp.","sector":"Technology"},
    {"symbol":"CSCO","kind":"equity","market":"NYSE","name":"Cisco Systems","sector":"Technology"},
    {"symbol":"NFLX","kind":"equity","market":"NYSE","name":"Netflix Inc.","sector":"Communication"},
    {"symbol":"DIS","kind":"equity","market":"NYSE","name":"Walt Disney Co.","sector":"Communication"},
    {"symbol":"JPM","kind":"equity","market":"NYSE","name":"JPMorgan Chase","sector":"Financials"},
    {"symbol":"BAC","kind":"equity","market":"NYSE","name":"Bank of America","sector":"Financials"},
    {"symbol":"WFC","kind":"equity","market":"NYSE","name":"Wells Fargo","sector":"Financials"},
    {"symbol":"GS","kind":"equity","market":"NYSE","name":"Goldman Sachs","sector":"Financials"},
    {"symbol":"MS","kind":"equity","market":"NYSE","name":"Morgan Stanley","sector":"Financials"},
    {"symbol":"V","kind":"equity","market":"NYSE","name":"Visa Inc.","sector":"Financials"},
    {"symbol":"MA","kind":"equity","market":"NYSE","name":"Mastercard","sector":"Financials"},
    {"symbol":"UNH","kind":"equity","market":"NYSE","name":"UnitedHealth Group","sector":"Healthcare"},
    {"symbol":"JNJ","kind":"equity","market":"NYSE","name":"Johnson & Johnson","sector":"Healthcare"},
    {"symbol":"PFE","kind":"equity","market":"NYSE","name":"Pfizer Inc.","sector":"Healthcare"},
    {"symbol":"LLY","kind":"equity","market":"NYSE","name":"Eli Lilly","sector":"Healthcare"},
    {"symbol":"ABBV","kind":"equity","market":"NYSE","name":"AbbVie","sector":"Healthcare"},
    {"symbol":"WMT","kind":"equity","market":"NYSE","name":"Walmart","sector":"Consumer Staples"},
    {"symbol":"PG","kind":"equity","market":"NYSE","name":"Procter & Gamble","sector":"Consumer Staples"},
    {"symbol":"KO","kind":"equity","market":"NYSE","name":"Coca-Cola","sector":"Consumer Staples"},
    {"symbol":"MCD","kind":"equity","market":"NYSE","name":"McDonalds","sector":"Consumer Discretionary"},
    {"symbol":"NKE","kind":"equity","market":"NYSE","name":"Nike Inc.","sector":"Consumer Discretionary"},
    {"symbol":"XOM","kind":"equity","market":"NYSE","name":"Exxon Mobil","sector":"Energy"},
    {"symbol":"CVX","kind":"equity","market":"NYSE","name":"Chevron","sector":"Energy"},
    {"symbol":"BA","kind":"equity","market":"NYSE","name":"Boeing","sector":"Industrials"},
    {"symbol":"CAT","kind":"equity","market":"NYSE","name":"Caterpillar","sector":"Industrials"},
    {"symbol":"SPY","kind":"etf","market":"NYSE","name":"SPDR S&P 500 ETF","sector":"Index ETF"},
    {"symbol":"QQQ","kind":"etf","market":"NYSE","name":"Invesco QQQ","sector":"Index ETF"},
    {"symbol":"IWM","kind":"etf","market":"NYSE","name":"iShares Russell 2000","sector":"Index ETF"},
    {"symbol":"GLD","kind":"etf","market":"NYSE","name":"SPDR Gold Shares","sector":"Commodity ETF"},
    {"symbol":"TLT","kind":"etf","market":"NYSE","name":"iShares 20+ Year Treasury","sector":"Bond ETF"},
    {"symbol":"XLF","kind":"etf","market":"NYSE","name":"Financial Select Sector","sector":"Sector ETF"},
    {"symbol":"XLK","kind":"etf","market":"NYSE","name":"Technology Select Sector","sector":"Sector ETF"},
    {"symbol":"XLE","kind":"etf","market":"NYSE","name":"Energy Select Sector","sector":"Sector ETF"},
    {"symbol":"SPX","kind":"index","market":"NYSE","name":"S&P 500 Index","sector":"Index"},
    {"symbol":"DJI","kind":"index","market":"NYSE","name":"Dow Jones Industrial","sector":"Index"},
    {"symbol":"IXIC","kind":"index","market":"NYSE","name":"NASDAQ Composite","sector":"Index"},
    # FX
    {"symbol":"EURUSD","kind":"fx","market":"FOREX","name":"Euro / US Dollar","sector":"Major"},
    {"symbol":"GBPUSD","kind":"fx","market":"FOREX","name":"GBP / US Dollar","sector":"Major"},
    {"symbol":"USDJPY","kind":"fx","market":"FOREX","name":"USD / Japanese Yen","sector":"Major"},
    {"symbol":"USDTZS","kind":"fx","market":"FOREX","name":"USD / Tanzanian Shilling","sector":"EM"},
    {"symbol":"USDKES","kind":"fx","market":"FOREX","name":"USD / Kenyan Shilling","sector":"EM"},
    {"symbol":"USDZAR","kind":"fx","market":"FOREX","name":"USD / South African Rand","sector":"EM"},
    # Commodities
    {"symbol":"XAUUSD","kind":"commodity","market":"COMEX","name":"Gold","sector":"Precious Metals"},
    {"symbol":"XAGUSD","kind":"commodity","market":"COMEX","name":"Silver","sector":"Precious Metals"},
    {"symbol":"WTI","kind":"commodity","market":"COMEX","name":"WTI Crude Oil","sector":"Energy"},
    {"symbol":"BRENT","kind":"commodity","market":"COMEX","name":"Brent Crude Oil","sector":"Energy"},
    {"symbol":"COPPER","kind":"commodity","market":"COMEX","name":"Copper","sector":"Industrial Metals"},
    # Crypto
    {"symbol":"BTCUSD","kind":"crypto","market":"CRYPTO","name":"Bitcoin","sector":"Major Crypto"},
    {"symbol":"ETHUSD","kind":"crypto","market":"CRYPTO","name":"Ethereum","sector":"Major Crypto"},
    {"symbol":"SOLUSD","kind":"crypto","market":"CRYPTO","name":"Solana","sector":"Major Crypto"},
    {"symbol":"BNBUSD","kind":"crypto","market":"CRYPTO","name":"Binance Coin","sector":"Major Crypto"},
    {"symbol":"XRPUSD","kind":"crypto","market":"CRYPTO","name":"XRP","sector":"Major Crypto"},
]


# ══════════════════════════════════════════════════════════════════
# DEFAULT CONFIG
# ══════════════════════════════════════════════════════════════════
DEFAULT_CONFIG = {
    "active_providers": {"ai": "groq", "data": "stub", "broker": "paper"},
    "api_keys": {
        "gemini": "", "anthropic": "", "openai": "", "groq": "",
        "deepseek": "", "mistral": "", "cohere": "",
        "mansa": "", "eodhd": "", "alpha_vantage": "",
        "finnhub": "", "exchangerate": "", "coingecko": "",
        "alpaca_key": "", "alpaca_secret": "",
        "newsapi": "",
    },
    "ai_models": {
        "gemini": "", "anthropic": "", "openai": "", "groq": "",
        "deepseek": "", "mistral": "", "cohere": "",
    },
    "ibkr": {"host": "127.0.0.1", "port": 7497, "client_id": 17},
    "alpaca": {"paper": True},
    "markets": [
        {"code":"DSE","name":"Dar es Salaam SE","country":"Tanzania","currency":"TZS","open":"10:00","close":"16:00","tz":"+03:00","active":True},
        {"code":"NSE","name":"Nairobi SE","country":"Kenya","currency":"KES","open":"09:30","close":"15:00","tz":"+03:00","active":True},
        {"code":"NYSE","name":"New York SE","country":"USA","currency":"USD","open":"09:30","close":"16:00","tz":"-05:00","active":True},
        {"code":"LSE","name":"London SE","country":"UK","currency":"GBP","open":"08:00","close":"16:30","tz":"+00:00","active":True},
        {"code":"JSE","name":"Johannesburg SE","country":"South Africa","currency":"ZAR","open":"09:00","close":"17:00","tz":"+02:00","active":True},
        {"code":"NGX","name":"Nigerian Exchange","country":"Nigeria","currency":"NGN","open":"10:00","close":"14:30","tz":"+01:00","active":True},
        {"code":"FOREX","name":"Foreign Exchange","country":"Global","currency":"USD","open":"00:00","close":"23:59","tz":"+00:00","active":True},
        {"code":"COMEX","name":"COMEX (Commodities)","country":"USA","currency":"USD","open":"06:00","close":"17:00","tz":"-05:00","active":True},
        {"code":"CRYPTO","name":"Crypto (24/7)","country":"Global","currency":"USD","open":"00:00","close":"23:59","tz":"+00:00","active":True},
    ],
    "instruments": INSTRUMENTS,
    "watchlist": [
        {"symbol":"CRDB","market":"DSE"},
        {"symbol":"NMB","market":"DSE"},
        {"symbol":"VODA","market":"DSE"},
        {"symbol":"NVDA","market":"NYSE"},
        {"symbol":"BTCUSD","market":"CRYPTO"},
        {"symbol":"XAUUSD","market":"COMEX"},
        {"symbol":"USDTZS","market":"FOREX"},
    ],
    "strategies": [
        {"id":"momentum","name":"Momentum","description":"EMA 12/26 + RSI 14","weight":0.40,"enabled":True,"applies_to":["equity","crypto"]},
        {"id":"breakout","name":"Breakout","description":"Donchian + ATR volume","weight":0.35,"enabled":True,"applies_to":["equity","fx","commodity"]},
        {"id":"mean_rev","name":"Mean Reversion","description":"Bollinger + Z-score","weight":0.25,"enabled":True,"applies_to":["equity","fx"]},
        {"id":"value","name":"Value","description":"P/E, P/B, EV/EBITDA composite","weight":0,"enabled":False,"applies_to":["equity"]},
        {"id":"carry","name":"Carry Trade","description":"FX rate differential","weight":0,"enabled":False,"applies_to":["fx"]},
        {"id":"trend","name":"Trend Following","description":"Multi-timeframe MA","weight":0,"enabled":False,"applies_to":["fx","commodity","crypto"]},
    ],
    "prompts": {
        "market":"You are a Managing Director-level technical analyst at Resolut Asset Management, trained to Goldman Sachs and Point72 standards. Produce institutional-grade technical analysis for {kind} on {market}. Cover trend structure, exact support/resistance levels, RSI, MACD, volume, EMA crossovers, and a precise directional call with entry level. 160-200 words.",
        "fundamental":"You are a Fundamental Research Analyst at Resolut Asset Management (Goldman Sachs/Point72 standard). For equities: P/E vs history, revenue growth, margins, ROIC vs WACC, moat, price target. For FX/commodity/crypto: macro drivers, supply/demand, structural trends. 160-200 words.",
        "social":"You are a Market Intelligence analyst at Resolut (Goldman prime brokerage standard). Assess institutional vs retail sentiment, short interest, options positioning, social signals, fund flows. Identify positioning extremes. 100-130 words.",
        "news":"You are a Macro/Event-Driven analyst at Resolut (Point72 standard). Identify company catalysts, sector developments, regulatory risks, macro impact on {kind} on {market}. Rate sentiment: Strongly Positive/Positive/Neutral/Negative/Strongly Negative. 100-130 words.",
        "bull":"You are the Senior Bull Analyst at Resolut Asset Management (Goldman Conviction Buy standard). Build 3-4 specific, quantified bull arguments with price targets, catalyst timeline, and margin of safety. No hedging. 160-200 words.",
        "bear":"You are the Senior Bear Analyst at Resolut Asset Management (Point72 short thesis standard). Build 3-4 specific bear arguments with overvaluation metrics, breakdown levels, and short catalyst timeline. Be ruthless. 160-200 words.",
        "trader":"You are Head of Trading at Resolut Asset Management (Goldman desk standard). Synthesise the full debate into one decisive signal. State entry, stop, target, holding period. End with: SIGNAL: BUY/HOLD/SELL | CONVICTION: High/Medium/Low | SIZING: X%.",
        "risk_agg":"Aggressive Risk Analyst at Resolut (Point72 standard). Identify asymmetric upside, maximum justified position size. 100 words.",
        "risk_con":"Conservative Risk Analyst at Resolut (Goldman risk committee standard). Identify tail risks, max drawdown, recommend conservative size. 100 words.",
        "risk_neu":"Balanced Risk Analyst at Resolut. Assign probabilities to bull/base/bear scenarios. State expected value. 80 words.",
        "portfolio":"You are CIO of Resolut Asset Management (Goldman CIO + Point72 PM standard). Issue final investment decision after reviewing all research. State: VERDICT (BUY/HOLD/SELL), Conviction, Entry, Stop, Target, Holding Period, Position Size %, Key Risk. 220-270 words.",
        "live_brief":"You are CIO of Resolut Asset Management. Write a live institutional brief (Goldman morning note standard) covering global macro, key markets, East African markets (DSE/NSE), watchlist commentary with directional bias, top trade ideas, key risks.",
    },
    "risk": {
        "max_position_pct": 10.0, "daily_loss_cap_pct": 2.0,
        "max_open_positions": 8, "default_stop_pct": 5.0, "default_take_profit_pct": 15.0,
    },
}


# ══════════════════════════════════════════════════════════════════
# CONFIG — SQLite-backed for persistence on cloud deployments
# ══════════════════════════════════════════════════════════════════
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with _db() as conn:
        conn.executescript("""
CREATE TABLE IF NOT EXISTS config_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT, uuid TEXT UNIQUE NOT NULL,
    created_at TEXT NOT NULL, closed_at TEXT, status TEXT NOT NULL, source TEXT NOT NULL,
    symbol TEXT NOT NULL, market TEXT NOT NULL, kind TEXT, direction TEXT NOT NULL,
    shares REAL NOT NULL, entry_price REAL NOT NULL, exit_price REAL,
    stop_price REAL, tp_price REAL, current_price REAL,
    realized_pnl REAL, unrealized_pnl REAL, outcome TEXT, exit_reason TEXT,
    agent_signal TEXT, notes TEXT, ai_provider TEXT, broker_provider TEXT
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, event TEXT NOT NULL, details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL, symbol TEXT NOT NULL, market TEXT NOT NULL,
    kind TEXT, depth TEXT NOT NULL, ai_provider TEXT,
    verdict TEXT, conviction TEXT, full_results TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);
        """)
        conn.commit()
    log.info("Database ready at %s", DB_PATH)


def load_config() -> dict:
    """Load config from SQLite — falls back to DEFAULT_CONFIG on first run."""
    try:
        with _db() as conn:
            rows = conn.execute("SELECT key, value FROM config_kv").fetchall()
        if not rows:
            # First run — seed defaults
            save_config(DEFAULT_CONFIG)
            return json.loads(json.dumps(DEFAULT_CONFIG))
        cfg = {}
        for row in rows:
            cfg[row["key"]] = json.loads(row["value"])
        # Merge missing defaults
        def merge(default, current):
            for k, v in default.items():
                if k not in current:
                    current[k] = v
                elif isinstance(v, dict) and isinstance(current.get(k), dict):
                    merge(v, current[k])
            return current
        return merge(json.loads(json.dumps(DEFAULT_CONFIG)), cfg)
    except Exception:
        return json.loads(json.dumps(DEFAULT_CONFIG))


def save_config(cfg: dict):
    """Save entire config to SQLite as key-value pairs."""
    with _db() as conn:
        for key, value in cfg.items():
            conn.execute(
                "INSERT OR REPLACE INTO config_kv (key, value) VALUES (?, ?)",
                (key, json.dumps(value, default=str))
            )
        conn.commit()


def save_config_key(key: str, value):
    """Save a single config key."""
    with _db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO config_kv (key, value) VALUES (?, ?)",
            (key, json.dumps(value, default=str))
        )
        conn.commit()


def log_event(event: str, details: dict):
    with _db() as conn:
        conn.execute(
            "INSERT INTO audit_log (ts, event, details) VALUES (?,?,?)",
            (datetime.now(EAT).isoformat(), event, json.dumps(details, default=str))
        )
        conn.commit()


# ══════════════════════════════════════════════════════════════════
# PROVIDER RESOLUTION
# ══════════════════════════════════════════════════════════════════
def get_active_ai():
    cfg = load_config()
    name = cfg["active_providers"]["ai"]
    key = cfg["api_keys"].get(name, "")
    model = cfg["ai_models"].get(name) or None
    return ai_registry.get_provider(name, api_key=key, model=model)


async def fetch_price(symbol: str, market: str) -> dict:
    cfg = load_config()
    keys = cfg.get("api_keys", {})
    try:
        from providers.data import smart_quote
        return await smart_quote(symbol, market, keys)
    except Exception as e:
        log.warning("Price fetch failed %s/%s: %s", symbol, market, e)
        stub = data_registry.get_provider("stub")
        return await stub.quote(symbol, market)


_broker_instance = None
_broker_name = None


async def get_active_broker():
    global _broker_instance, _broker_name
    cfg = load_config()
    name = cfg["active_providers"]["broker"]
    if _broker_instance and _broker_name == name:
        return _broker_instance
    if _broker_instance:
        try: await _broker_instance.disconnect()
        except: pass
    broker_cfg = {
        **cfg.get("ibkr", {}),
        "alpaca_key": cfg["api_keys"].get("alpaca_key", ""),
        "alpaca_secret": cfg["api_keys"].get("alpaca_secret", ""),
        "alpaca_paper": cfg.get("alpaca", {}).get("paper", True),
    }
    _broker_instance = broker_registry.get_provider(name, config=broker_cfg)
    _broker_name = name
    if name == "paper":
        await _broker_instance.connect()
    return _broker_instance


def get_instrument(symbol: str, market: str) -> dict:
    cfg = load_config()
    inst = next((i for i in cfg.get("instruments", INSTRUMENTS)
                 if i["symbol"] == symbol and i["market"] == market), None)
    return inst or {"symbol": symbol, "market": market, "kind": "equity", "name": symbol, "sector": ""}


def quant_signals(symbol: str) -> dict:
    import math
    h = sum(ord(c) for c in symbol)
    r = lambda s: math.sin(s * 9301 + 49297) * 0.5 + 0.5
    momentum = round(r(h)*2-1, 2)
    breakout = round(r(h+1)*2-1, 2)
    meanrev  = round(r(h+2)*2-1, 2)
    composite = round(momentum*0.4 + breakout*0.35 + meanrev*0.25, 2)
    rsi = round(30 + r(h+3)*40)
    return {"momentum": momentum, "breakout": breakout, "mean_rev": meanrev, "composite": composite, "rsi": rsi}


# ══════════════════════════════════════════════════════════════════
# FASTAPI APP
# ══════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    load_config()  # seeds defaults on first run
    log.info("Resolut Asset Management Terminal ready")
    yield
    if _broker_instance:
        try: await _broker_instance.disconnect()
        except: pass


app = FastAPI(title="Resolut Asset Management Terminal", version="3.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── HEALTH ───────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    cfg = load_config()
    broker = await get_active_broker()
    return {
        "status": "ok",
        "time_eat": datetime.now(EAT).isoformat(),
        "active": cfg["active_providers"],
        "broker_connected": broker.connected,
    }


# ─── PROVIDERS ────────────────────────────────────────────────────
@app.get("/api/providers/ai")
def providers_ai(): return ai_registry.list_providers()

@app.get("/api/providers/data")
def providers_data(): return data_registry.list_providers()

@app.get("/api/providers/broker")
def providers_broker(): return broker_registry.list_providers()

@app.post("/api/providers/test/{kind}/{name}")
async def test_provider(kind: str, name: str):
    cfg = load_config()
    try:
        if kind == "ai":
            key = cfg["api_keys"].get(name, "")
            if not key: return {"ok": False, "error": "API key not set"}
            p = ai_registry.get_provider(name, api_key=key, model=cfg["ai_models"].get(name) or None)
            txt = await p.generate("Test.", "Reply: TEST OK", max_tokens=20)
            return {"ok": True, "response": txt[:100]}
        elif kind == "data":
            test_map = {
                "finnhub": ("AAPL", "NYSE"), "exchangerate": ("USDTZS", "FOREX"),
                "coingecko": ("BTCUSD", "CRYPTO"), "mansa": ("CRDB", "DSE"),
                "dse_scraper": ("CRDB", "DSE"), "yahoo": ("AAPL", "NYSE"),
                "stub": ("CRDB", "DSE"), "eodhd": ("AAPL", "NYSE"),
                "alpha_vantage": ("AAPL", "NYSE"),
            }
            sym, mkt = test_map.get(name, ("AAPL", "NYSE"))
            key = cfg["api_keys"].get(name, "")
            p = data_registry.get_provider(name, api_key=key)
            q = await p.quote(sym, mkt)
            return {"ok": True, "quote": {"symbol": q.get("ticker",""), "price": q.get("price",0), "source": q.get("source",""), "change_pct": q.get("change_pct",0)}}
        elif kind == "broker":
            broker_cfg = {**cfg.get("ibkr", {}),
                          "alpaca_key": cfg["api_keys"].get("alpaca_key", ""),
                          "alpaca_secret": cfg["api_keys"].get("alpaca_secret", ""),
                          "alpaca_paper": cfg.get("alpaca", {}).get("paper", True)}
            p = broker_registry.get_provider(name, config=broker_cfg)
            ok = await p.connect()
            try: await p.disconnect()
            except: pass
            return {"ok": ok, "error": p.error}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── CONFIG ───────────────────────────────────────────────────────
@app.get("/api/config")
def get_cfg():
    cfg = load_config()
    masked = json.loads(json.dumps(cfg))
    for k, v in masked.get("api_keys", {}).items():
        if v and len(str(v)) > 8:
            masked["api_keys"][k] = v[:4] + "••••••••" + v[-4:]
    return masked


@app.post("/api/config")
def update_cfg(updates: dict = Body(...)):
    cfg = load_config()
    def merge(a, b):
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict): merge(a[k], v)
            elif v == "" and isinstance(a.get(k), str) and len(a.get(k, "")) > 8: pass
            else: a[k] = v
    merge(cfg, updates)
    save_config(cfg)
    log_event("CONFIG_UPDATE", {"keys": list(updates.keys())})
    return {"ok": True}


@app.post("/api/config/api_key")
def set_api_key(payload: dict = Body(...)):
    name = payload.get("name", "").strip()
    value = payload.get("value", "").strip()
    if not name or not value:
        raise HTTPException(400, "name and value required")
    cfg = load_config()
    cfg["api_keys"][name] = value
    save_config(cfg)
    log_event("API_KEY_SET", {"name": name})
    return {"ok": True}


@app.delete("/api/config/api_key/{name}")
def delete_api_key(name: str):
    cfg = load_config()
    cfg["api_keys"].pop(name, None)
    save_config(cfg)
    log_event("API_KEY_DELETE", {"name": name})
    return {"ok": True}


# ─── MARKETS / INSTRUMENTS / STRATEGIES / WATCHLIST / PROMPTS ────
@app.get("/api/markets")
def list_markets(): return load_config()["markets"]

@app.post("/api/markets")
def upsert_market(m: dict = Body(...)):
    if not m.get("code"): raise HTTPException(400, "code required")
    cfg = load_config()
    ex = next((x for x in cfg["markets"] if x["code"] == m["code"]), None)
    if ex: ex.update(m)
    else: cfg["markets"].append(m)
    save_config(cfg); log_event("MARKET_UPSERT", m)
    return {"ok": True}

@app.delete("/api/markets/{code}")
def del_market(code: str):
    cfg = load_config()
    cfg["markets"] = [m for m in cfg["markets"] if m["code"] != code]
    save_config(cfg); return {"ok": True}

@app.get("/api/instruments")
def list_instruments(market: Optional[str] = None):
    cfg = load_config()
    insts = cfg.get("instruments", INSTRUMENTS)
    if market: insts = [i for i in insts if i["market"] == market]
    return insts

@app.post("/api/instruments")
def upsert_instrument(i: dict = Body(...)):
    if not i.get("symbol"): raise HTTPException(400, "symbol required")
    cfg = load_config()
    ex = next((x for x in cfg["instruments"] if x["symbol"] == i["symbol"] and x["market"] == i["market"]), None)
    if ex: ex.update(i)
    else: cfg["instruments"].append(i)
    save_config(cfg); log_event("INSTRUMENT_UPSERT", i)
    return {"ok": True}

@app.delete("/api/instruments/{market}/{symbol}")
def del_instrument(market: str, symbol: str):
    cfg = load_config()
    cfg["instruments"] = [x for x in cfg["instruments"] if not (x["symbol"] == symbol and x["market"] == market)]
    save_config(cfg); return {"ok": True}

@app.get("/api/strategies")
def list_strategies(): return load_config()["strategies"]

@app.post("/api/strategies")
def upsert_strategy(s: dict = Body(...)):
    if not s.get("id"): raise HTTPException(400, "id required")
    cfg = load_config()
    ex = next((x for x in cfg["strategies"] if x["id"] == s["id"]), None)
    if ex: ex.update(s)
    else: cfg["strategies"].append(s)
    save_config(cfg); return {"ok": True}

@app.delete("/api/strategies/{sid}")
def del_strategy(sid: str):
    cfg = load_config()
    cfg["strategies"] = [s for s in cfg["strategies"] if s["id"] != sid]
    save_config(cfg); return {"ok": True}

@app.get("/api/watchlist")
def list_watchlist(): return load_config()["watchlist"]

@app.post("/api/watchlist")
def add_watchlist(item: dict = Body(...)):
    cfg = load_config()
    if not any(w["symbol"] == item["symbol"] and w["market"] == item["market"] for w in cfg["watchlist"]):
        cfg["watchlist"].append(item)
        save_config(cfg)
    return {"ok": True}

@app.delete("/api/watchlist/{market}/{symbol}")
def del_watchlist(market: str, symbol: str):
    cfg = load_config()
    cfg["watchlist"] = [w for w in cfg["watchlist"] if not (w["symbol"] == symbol and w["market"] == market)]
    save_config(cfg); return {"ok": True}

@app.get("/api/prompts")
def get_prompts(): return load_config()["prompts"]

@app.post("/api/prompts")
def update_prompts(p: dict = Body(...)):
    cfg = load_config()
    cfg["prompts"].update(p)
    save_config(cfg); log_event("PROMPTS_UPDATE", {"keys": list(p.keys())})
    return {"ok": True}


# ─── PRICES ───────────────────────────────────────────────────────
@app.get("/api/price/{market}/{symbol}")
async def price(market: str, symbol: str):
    return await fetch_price(symbol, market)

@app.get("/api/prices")
async def prices_bulk(symbols: str):
    out = []
    for pair in symbols.split(","):
        if ":" not in pair: continue
        s, m = pair.split(":")
        try: out.append(await fetch_price(s.strip(), m.strip()))
        except Exception as e: out.append({"symbol": s, "market": m, "error": str(e)})
    return out


# ─── TRADES ───────────────────────────────────────────────────────
class TradeIn(BaseModel):
    symbol: str
    market: str
    direction: str = Field(pattern="^(LONG|SHORT)$")
    shares: float
    entry_price: float
    stop_price: Optional[float] = None
    tp_price: Optional[float] = None
    source: str = "PAPER"
    notes: Optional[str] = None
    agent_signal: Optional[dict] = None


@app.post("/api/trades")
def open_trade(t: TradeIn):
    cfg = load_config()
    risk = cfg["risk"]
    inst = get_instrument(t.symbol, t.market)
    if t.stop_price is None:
        t.stop_price = round(t.entry_price * (1 - risk["default_stop_pct"]/100) if t.direction == "LONG"
                             else t.entry_price * (1 + risk["default_stop_pct"]/100), 6)
    if t.tp_price is None:
        t.tp_price = round(t.entry_price * (1 + risk["default_take_profit_pct"]/100) if t.direction == "LONG"
                           else t.entry_price * (1 - risk["default_take_profit_pct"]/100), 6)
    uid = str(uuidlib.uuid4())
    now = datetime.now(EAT).isoformat()
    with _db() as conn:
        conn.execute("""INSERT INTO trades (uuid, created_at, status, source, symbol, market, kind,
                        direction, shares, entry_price, stop_price, tp_price, current_price,
                        agent_signal, notes, ai_provider, broker_provider)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                     (uid, now, "OPEN", t.source, t.symbol, t.market,
                      inst.get("kind", "equity"), t.direction, t.shares,
                      t.entry_price, t.stop_price, t.tp_price, t.entry_price,
                      json.dumps(t.agent_signal) if t.agent_signal else None, t.notes,
                      cfg["active_providers"]["ai"], cfg["active_providers"]["broker"]))
        conn.commit()
    log_event("TRADE_OPEN", t.dict())
    return {"ok": True, "uuid": uid}


@app.get("/api/trades")
def list_trades(status: Optional[str] = None, limit: int = 200):
    q = "SELECT * FROM trades"
    args = []
    if status: q += " WHERE status = ?"; args.append(status.upper())
    q += " ORDER BY created_at DESC LIMIT ?"; args.append(limit)
    with _db() as conn:
        return [dict(r) for r in conn.execute(q, args).fetchall()]


@app.post("/api/trades/refresh_all")
async def refresh_all():
    out = []
    with _db() as conn:
        opens = conn.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()
    for row in opens:
        try:
            p = await fetch_price(row["symbol"], row["market"])
            cur = p["price"]
            upnl = (cur - row["entry_price"]) * row["shares"] * (1 if row["direction"] == "LONG" else -1)
            with _db() as conn:
                conn.execute("UPDATE trades SET current_price=?, unrealized_pnl=? WHERE uuid=?",
                             (cur, upnl, row["uuid"]))
                conn.commit()
            out.append({"uuid": row["uuid"], "symbol": row["symbol"], "current_price": cur, "unrealized_pnl": upnl})
        except Exception as e:
            log.warning("refresh failed %s: %s", row["symbol"], e)
    return out


@app.post("/api/trades/{uid}/close")
def close_trade(uid: str, payload: dict = Body(default={})):
    with _db() as conn:
        row = conn.execute("SELECT * FROM trades WHERE uuid=?", (uid,)).fetchone()
        if not row: raise HTTPException(404, "trade not found")
        exit_price = payload.get("exit_price") or row["current_price"] or row["entry_price"]
        rpnl = (exit_price - row["entry_price"]) * row["shares"] * (1 if row["direction"] == "LONG" else -1)
        outcome = "WIN" if rpnl > 0 else "LOSS" if rpnl < 0 else "BREAKEVEN"
        conn.execute("""UPDATE trades SET status='CLOSED', closed_at=?, exit_price=?, exit_reason=?,
                        realized_pnl=?, unrealized_pnl=NULL, outcome=? WHERE uuid=?""",
                     (datetime.now(EAT).isoformat(), exit_price,
                      payload.get("exit_reason", "MANUAL"), rpnl, outcome, uid))
        conn.commit()
    log_event("TRADE_CLOSE", {"uuid": uid, "pnl": rpnl, "outcome": outcome})
    return {"ok": True, "realized_pnl": rpnl, "outcome": outcome}


# ─── PORTFOLIO ────────────────────────────────────────────────────
@app.get("/api/portfolio")
async def portfolio():
    with _db() as conn:
        opens = [dict(r) for r in conn.execute("SELECT * FROM trades WHERE status='OPEN'").fetchall()]
        closed = [dict(r) for r in conn.execute("SELECT * FROM trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 100").fetchall()]
    realized = sum((t.get("realized_pnl") or 0) for t in closed)
    unrealized = sum((t.get("unrealized_pnl") or 0) for t in opens)
    wins = sum(1 for t in closed if t.get("outcome") == "WIN")
    losses = sum(1 for t in closed if t.get("outcome") == "LOSS")
    broker = await get_active_broker()
    return {
        "open_positions": opens, "closed_trades": closed,
        "realized_pnl": realized, "unrealized_pnl": unrealized,
        "total_pnl": realized + unrealized,
        "win_rate": (wins / (wins + losses) * 100) if (wins + losses) else 0,
        "wins": wins, "losses": losses,
        "open_count": len(opens), "closed_count": len(closed),
        "broker": load_config()["active_providers"]["broker"],
        "broker_connected": broker.connected,
        "broker_summary": await broker.account_summary(),
        "broker_positions": await broker.positions(),
    }


# ─── AUDIT ────────────────────────────────────────────────────────
@app.get("/api/audit")
def audit_log(limit: int = 100, event: Optional[str] = None):
    q = "SELECT * FROM audit_log"
    args = []
    if event: q += " WHERE event = ?"; args.append(event)
    q += " ORDER BY ts DESC LIMIT ?"; args.append(limit)
    with _db() as conn:
        rows = conn.execute(q, args).fetchall()
    return [{"id": r["id"], "ts": r["ts"], "event": r["event"], "details": json.loads(r["details"])} for r in rows]


# ─── DATABASE STATS ───────────────────────────────────────────────
@app.get("/api/database/stats")
def db_stats():
    cfg = load_config()
    with _db() as conn:
        trades_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()[0]
        trades_closed = conn.execute("SELECT COUNT(*) FROM trades WHERE status='CLOSED'").fetchone()[0]
        agent_runs = conn.execute("SELECT COUNT(*) FROM agent_runs").fetchone()[0]
        audit_events = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    instruments = cfg.get("instruments", INSTRUMENTS)
    from collections import Counter
    by_market = [{"market": k, "count": v} for k, v in Counter(i["market"] for i in instruments).items()]
    by_kind = [{"kind": k, "count": v} for k, v in Counter(i["kind"] for i in instruments).items()]
    return {
        "instruments": len(instruments),
        "trades_open": trades_open, "trades_closed": trades_closed,
        "agent_runs": agent_runs, "audit_events": audit_events,
        "by_market": by_market, "by_kind": by_kind,
    }


@app.get("/api/agent_runs")
def list_agent_runs(symbol: Optional[str] = None, limit: int = 50):
    q = "SELECT id, ts, symbol, market, kind, depth, ai_provider, verdict, conviction FROM agent_runs"
    args = []
    if symbol: q += " WHERE symbol = ?"; args.append(symbol.upper())
    q += " ORDER BY ts DESC LIMIT ?"; args.append(limit)
    with _db() as conn:
        return [dict(r) for r in conn.execute(q, args).fetchall()]


# ─── POSITION SIZING ──────────────────────────────────────────────
@app.post("/api/sizing/calculate")
def calculate_sizing(payload: dict = Body(...)):
    account_size = float(payload.get("account_size", 0))
    risk_pct = float(payload.get("risk_pct", 2))
    entry = float(payload.get("entry_price", 0))
    stop = float(payload.get("stop_price", 0))
    if not account_size or not entry or not stop or entry == stop:
        raise HTTPException(400, "account_size, entry_price, stop_price required and must differ")
    risk_amount = account_size * risk_pct / 100
    risk_per_share = abs(entry - stop)
    shares = risk_amount / risk_per_share
    position_value = shares * entry
    position_pct = position_value / account_size * 100
    cfg = load_config()
    max_pos = cfg["risk"]["max_position_pct"]
    if position_pct > max_pos:
        shares = (account_size * max_pos / 100) / entry
        position_value = shares * entry
        position_pct = max_pos
    return {
        "shares": round(shares),
        "position_value": round(position_value, 2),
        "position_pct": round(position_pct, 2),
        "risk_amount": round(risk_amount, 2),
        "risk_per_share": round(risk_per_share, 4),
        "risk_reward": round(abs(payload.get("tp_price", entry) - entry) / risk_per_share, 2) if payload.get("tp_price") else None,
    }


# ─── AGENT PIPELINE ───────────────────────────────────────────────
class AgentRunRequest(BaseModel):
    symbol: str
    market: str
    date: Optional[str] = None
    depth: str = "full"


@app.post("/api/agents/run")
async def run_agents(req: AgentRunRequest):
    cfg = load_config()
    prompts = cfg["prompts"]
    inst = get_instrument(req.symbol, req.market)
    kind = inst.get("kind", "equity")
    name = inst.get("name", req.symbol)
    date_str = req.date or datetime.now(EAT).date().isoformat()
    qs = quant_signals(req.symbol)

    results = {
        "symbol": req.symbol, "market": req.market, "kind": kind, "name": name,
        "date": date_str, "depth": req.depth,
        "ai_provider": cfg["active_providers"]["ai"],
        "agents": {}, "quant": qs,
    }

    if req.depth == "quant":
        c = qs["composite"]
        verdict = "BUY" if c > 0.2 else "SELL" if c < -0.2 else "HOLD"
        results["verdict"] = verdict; results["conviction"] = "Medium"
        with _db() as conn:
            conn.execute("INSERT INTO agent_runs (ts, symbol, market, kind, depth, ai_provider, verdict, conviction, full_results) VALUES (?,?,?,?,?,?,?,?,?)",
                         (datetime.now(EAT).isoformat(), req.symbol, req.market, kind, "quant", None, verdict, "Medium", json.dumps(results)))
            conn.commit()
        log_event("AGENT_RUN", {"symbol": req.symbol, "depth": "quant", "verdict": verdict})
        return results

    ai = get_active_ai()
    if not ai.api_key:
        raise HTTPException(400, f"{ai.display_name} API key not set. Add it in Admin Panel.")

    fmt = lambda key: prompts.get(key, "").replace("{market}", req.market).replace("{ticker}", req.symbol).replace("{symbol}", req.symbol).replace("{kind}", kind)
    is_full = (req.depth == "full")

    async def call(system, user, mt=1000):
        try: return await ai.generate(system, user, mt)
        except Exception as e: raise HTTPException(500, f"{ai.display_name}: {e}")

    mkt_out = await call(fmt("market"), f"Technical analysis of {name} ({req.symbol}) on {req.market} as of {date_str}. Kind: {kind}. RSI reading: {qs['rsi']}.")
    results["agents"]["market"] = mkt_out

    fund_out = await call(fmt("fundamental"), f"Fundamental analysis of {name} ({req.symbol}) on {req.market} as of {date_str}. Kind: {kind}.")
    results["agents"]["fundamental"] = fund_out

    if is_full:
        results["agents"]["social"] = await call(fmt("social"), f"Sentiment analysis for {name} ({req.symbol}) as of {date_str}.")
        results["agents"]["news"] = await call(fmt("news"), f"News analysis for {name} ({req.symbol}) on {req.market} as of {date_str}. Kind: {kind}.")

    prev = mkt_out[:300] + " | " + fund_out[:200]
    bull = await call(fmt("bull"), f"Bull case for {name} ({req.symbol}). Context: {prev}")
    results["agents"]["bull"] = bull
    bear = await call(fmt("bear"), f"Bear case for {name} ({req.symbol}). Bull case: {bull[:350]}")
    results["agents"]["bear"] = bear
    trader = await call(fmt("trader"), f"Trading signal for {name} ({req.symbol}).\nBull: {bull[:400]}\nBear: {bear[:400]}")
    results["agents"]["trader"] = trader

    if is_full:
        results["agents"]["risk_agg"] = await call(fmt("risk_agg"), f"Aggressive risk view: {trader[:200]}")
        results["agents"]["risk_con"] = await call(fmt("risk_con"), f"Conservative risk view: {trader[:200]}")
        results["agents"]["risk_neu"] = await call(fmt("risk_neu"), f"Neutral risk view: {trader[:200]}")

    all_ctx = "\n".join(filter(None, [
        f"Market: {mkt_out[:250]}", f"Fundamental: {fund_out[:200]}",
        f"Bull: {bull[:200]}", f"Bear: {bear[:200]}", f"Trader: {trader[:200]}",
    ]))
    pm = await call(fmt("portfolio"), f"Final decision for {name} ({req.symbol}) on {req.market} as of {date_str}.\n\nResearch:\n{all_ctx}")
    results["agents"]["portfolio"] = pm

    import re as rex
    m = rex.search(r'\b(BUY|SELL|HOLD)\b', pm)
    verdict = m.group(1) if m else "HOLD"
    cm = rex.search(r'conviction[:\s]*(high|medium|low)', pm, rex.IGNORECASE)
    conviction = cm.group(1).capitalize() if cm else "Medium"
    results["verdict"] = verdict
    results["conviction"] = conviction

    with _db() as conn:
        conn.execute("INSERT INTO agent_runs (ts, symbol, market, kind, depth, ai_provider, verdict, conviction, full_results) VALUES (?,?,?,?,?,?,?,?,?)",
                     (datetime.now(EAT).isoformat(), req.symbol, req.market, kind, req.depth, ai.name, verdict, conviction, json.dumps(results)))
        conn.commit()
    log_event("AGENT_RUN", {"symbol": req.symbol, "market": req.market, "depth": req.depth, "verdict": verdict, "ai": ai.name})
    return results


# ─── LIVE BRIEF ───────────────────────────────────────────────────
@app.post("/api/brief")
async def live_brief():
    cfg = load_config()
    ai = get_active_ai()
    if not ai.api_key:
        raise HTTPException(400, f"{ai.display_name} API key not set in Admin Panel.")
    wl = cfg["watchlist"]
    prices = []
    for w in wl[:12]:
        try:
            p = await fetch_price(w["symbol"], w["market"])
            prices.append(f"{w['symbol']} ({w['market']}): {p['price']} ({p.get('change_pct',0):+.2f}%) [{p.get('source','?')}]")
        except Exception:
            prices.append(f"{w['symbol']} ({w['market']}): n/a")
    now = datetime.now(EAT)
    user = (f"Live institutional brief — {now.strftime('%A, %d %B %Y, %H:%M EAT')}.\n\n"
            f"Watchlist:\n" + "\n".join(prices) + "\n\n"
            "Cover: 1. Global Macro 2. East African Markets (DSE/NSE) 3. Watchlist Commentary 4. Top Trade Ideas 5. Key Risks")
    text = await ai.generate(cfg["prompts"]["live_brief"], user, max_tokens=1500)
    log_event("LIVE_BRIEF", {"watchlist_n": len(prices), "ai": ai.name})
    return {"generated_at": now.isoformat(), "watchlist_prices": prices, "brief": text, "ai_provider": ai.name}


# ─── CIO DECISION ENGINE ──────────────────────────────────────────
@app.post("/api/cio/scan")
async def cio_scan(payload: dict = Body(default={})):
    cfg = load_config()
    ai = get_active_ai()
    if not ai.api_key:
        raise HTTPException(400, f"{ai.display_name} API key not set in Admin Panel.")
    watchlist = cfg.get("watchlist", [])
    if not watchlist:
        raise HTTPException(400, "Watchlist is empty.")
    now = datetime.now(EAT)
    prices = {}
    price_lines = []
    for w in watchlist:
        sym, mkt = w["symbol"], w["market"]
        try:
            p = await fetch_price(sym, mkt)
            prices[f"{sym}:{mkt}"] = p
            price_lines.append(f"{sym} ({mkt}): {p['price']} {p.get('change_pct',0):+.2f}% [{p.get('source','?')}]")
        except Exception as e:
            price_lines.append(f"{sym} ({mkt}): n/a")
    quant_ranked = []
    for w in watchlist:
        sym, mkt = w["symbol"], w["market"]
        inst = get_instrument(sym, mkt)
        qs = quant_signals(sym)
        p = prices.get(f"{sym}:{mkt}", {})
        quant_ranked.append({"symbol": sym, "market": mkt, "kind": inst.get("kind","equity"),
                              "name": inst.get("name", sym), "quant": qs,
                              "price": p.get("price", 0), "change_pct": p.get("change_pct", 0)})
    quant_ranked.sort(key=lambda x: abs(x["quant"]["composite"]), reverse=True)
    with _db() as conn:
        open_trades = [dict(r) for r in conn.execute(
            "SELECT symbol, market, direction, shares, entry_price, unrealized_pnl FROM trades WHERE status='OPEN'"
        ).fetchall()]
    open_pos_str = "\n".join([f"  OPEN: {t['symbol']} ({t['market']}) {t['direction']} {t['shares']}sh @ {t['entry_price']}" for t in open_trades]) or "  No open positions"
    quant_summary = "\n".join([
        f"{r['symbol']} ({r['market']}, {r['kind']}) | Price: {r['price']} ({r['change_pct']:+.2f}%) | Quant: {'STRONG BUY' if r['quant']['composite']>0.4 else 'BUY' if r['quant']['composite']>0.2 else 'SELL' if r['quant']['composite']<-0.2 else 'STRONG SELL' if r['quant']['composite']<-0.4 else 'NEUTRAL'} (composite={r['quant']['composite']:+.2f}, RSI={r['quant']['rsi']})"
        for r in quant_ranked
    ])
    system = f"""You are the Chief Investment Officer of Resolut Asset Management, a global multi-asset hedge fund headquartered in Dar es Salaam, Tanzania. You operate to Goldman Sachs and Point72 standards.

MANDATE: Aggressive Growth — maximise absolute returns, accept higher risk, medium-term horizon (weeks to months).
MARKETS: DSE (Tanzania), NSE (Kenya), NYSE, Forex, Gold, Crypto.
DATE: {now.strftime('%A, %d %B %Y, %H:%M EAT')}

Issue precise BUY/SELL/HOLD/WATCH decisions for every instrument. Include specific entry, stop, target, and position size for each actionable trade. End with TOP 3 PRIORITY TRADES and FUND POSITIONING (% deployed vs cash)."""

    user = f"WATCHLIST — Live Prices & Quant Signals:\n{quant_summary}\n\nOPEN POSITIONS:\n{open_pos_str}\n\nIssue full CIO decision memo now."
    cio_memo = await ai.generate(system, user, max_tokens=2000)
    log_event("CIO_SCAN", {"watchlist_n": len(watchlist), "ai": ai.name})
    return {
        "generated_at": now.isoformat(), "ai_provider": ai.name,
        "watchlist_count": len(watchlist), "open_positions": len(open_trades),
        "price_snapshot": price_lines, "quant_ranked": quant_ranked[:10],
        "cio_memo": cio_memo, "mandate": "Aggressive Growth · Medium-Term · All Markets",
    }


# ─── RESEARCH PANEL ───────────────────────────────────────────────
@app.get("/api/research/{market}/{symbol}")
async def stock_research(market: str, symbol: str):
    inst = get_instrument(symbol, market)
    try: price_data = await fetch_price(symbol, market)
    except Exception as e: price_data = {"price": 0, "change_pct": 0, "source": "error", "error": str(e)}
    qs = quant_signals(symbol)
    signal = "STRONG BUY" if qs["composite"]>0.4 else "BUY" if qs["composite"]>0.2 else "STRONG SELL" if qs["composite"]<-0.4 else "SELL" if qs["composite"]<-0.2 else "NEUTRAL"
    links = _build_links(symbol, market, inst.get("kind","equity"), inst.get("name",symbol))
    with _db() as conn:
        recent_runs = [dict(r) for r in conn.execute(
            "SELECT ts, depth, verdict, conviction FROM agent_runs WHERE symbol=? AND market=? ORDER BY ts DESC LIMIT 5",
            (symbol, market)).fetchall()]
        positions = [dict(r) for r in conn.execute(
            "SELECT * FROM trades WHERE symbol=? AND market=? AND status='OPEN'",
            (symbol, market)).fetchall()]
    return {"symbol": symbol, "market": market, "kind": inst.get("kind","equity"),
            "name": inst.get("name",symbol), "sector": inst.get("sector",""),
            "price": price_data, "quant": qs, "quant_signal": signal,
            "links": links, "recent_agent_runs": recent_runs, "open_positions": positions,
            "fetched_at": datetime.now(EAT).isoformat()}


def _build_links(symbol, market, kind, name):
    enc = name.replace(" ", "+")
    links = {"price": [], "fundamentals": [], "news": [], "exchange": [], "charts": []}
    if market == "DSE":
        links["exchange"] = [{"label":"DSE Official","url":"https://dse.co.tz/"}]
        links["price"] = [{"label":"DSE Market Data","url":"https://dse.co.tz/market-data"},{"label":"Uwekezaji","url":"https://uwekezaji.online/"}]
        links["fundamentals"] = [{"label":"African Markets","url":f"https://www.african-markets.com/en/stock-markets/dse/listed-companies/company?code={symbol}"},{"label":"Tanzania Invest","url":f"https://www.tanzaniainvest.com/dse/{symbol.lower()}"}]
        links["news"] = [{"label":"DSE Announcements","url":"https://dse.co.tz/company-announcements"},{"label":"The Citizen","url":f"https://www.thecitizen.co.tz/tanzania/business?q={symbol}"}]
    elif market == "NSE":
        links["exchange"] = [{"label":"NSE Official","url":"https://www.nse.co.ke/"}]
        links["price"] = [{"label":"NSE Live","url":"https://afx.kwayisi.org/nse/"}]
        links["fundamentals"] = [{"label":"African Markets NSE","url":f"https://www.african-markets.com/en/stock-markets/nse/listed-companies/company?code={symbol}"}]
        links["news"] = [{"label":"Business Daily Kenya","url":f"https://www.businessdailyafrica.com/search?q={symbol}"}]
    elif market in ("NYSE","NASDAQ"):
        links["price"] = [{"label":"Yahoo Finance","url":f"https://finance.yahoo.com/quote/{symbol}"},{"label":"Google Finance","url":f"https://www.google.com/finance/quote/{symbol}"}]
        links["fundamentals"] = [{"label":"Stock Analysis","url":f"https://stockanalysis.com/stocks/{symbol.lower()}/financials/"},{"label":"Macrotrends","url":f"https://www.macrotrends.net/stocks/charts/{symbol}/{symbol.lower()}/revenue"},{"label":"SEC Filings","url":f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={enc}&type=10-K&dateb=&owner=include&count=10"}]
        links["news"] = [{"label":"Seeking Alpha","url":f"https://seekingalpha.com/symbol/{symbol}"},{"label":"Reuters","url":f"https://www.reuters.com/search/news?blob={symbol}"}]
        links["charts"] = [{"label":"TradingView","url":f"https://www.tradingview.com/chart/?symbol={symbol}"},{"label":"Finviz","url":f"https://finviz.com/quote.ashx?t={symbol}"}]
    elif market == "FOREX":
        links["price"] = [{"label":"XE.com","url":f"https://www.xe.com/currencyconverter/convert/?From={symbol[:3]}&To={symbol[3:]}"},{"label":"TradingView FX","url":f"https://www.tradingview.com/chart/?symbol=FX:{symbol}"}]
        links["news"] = [{"label":"FXStreet","url":"https://www.fxstreet.com/"},{"label":"Forex Factory","url":"https://www.forexfactory.com/calendar"}]
    elif market == "COMEX":
        links["price"] = [{"label":"Kitco","url":"https://www.kitco.com/"},{"label":"TradingView","url":f"https://www.tradingview.com/chart/?symbol={symbol}"}]
        links["news"] = [{"label":"Kitco News","url":"https://www.kitco.com/news/"},{"label":"Reuters Commodities","url":"https://www.reuters.com/markets/commodities/"}]
    elif market == "CRYPTO":
        links["price"] = [{"label":"CoinGecko","url":f"https://www.coingecko.com/en/coins/{name.lower().replace(' ','-')}"},{"label":"CoinMarketCap","url":f"https://coinmarketcap.com/currencies/{name.lower().replace(' ','-')}/"}]
        links["news"] = [{"label":"CoinDesk","url":f"https://www.coindesk.com/search?s={symbol}"},{"label":"CryptoPanic","url":f"https://cryptopanic.com/news/{symbol.lower()}/"}]
    links["news"].append({"label":"Google News","url":f"https://news.google.com/search?q={enc}"})
    return links


# ─── BROKER ───────────────────────────────────────────────────────
@app.post("/api/broker/connect")
async def broker_connect():
    broker = await get_active_broker()
    await broker.connect()
    return {"connected": broker.connected, "error": broker.error}

@app.post("/api/broker/disconnect")
async def broker_disconnect():
    global _broker_instance, _broker_name
    if _broker_instance: await _broker_instance.disconnect()
    _broker_instance = None; _broker_name = None
    return {"connected": False}

@app.get("/api/broker/status")
async def broker_status():
    broker = await get_active_broker()
    return {"name": broker.name, "connected": broker.connected, "error": broker.error,
            "positions": await broker.positions(), "summary": await broker.account_summary()}


# ─── STATIC FRONTEND ──────────────────────────────────────────────
if FRONTEND_DIR.exists():
    @app.get("/")
    def root():
        idx = FRONTEND_DIR / "index.html"
        return FileResponse(idx) if idx.exists() else {"error": "frontend missing"}
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("server:app", host=host, port=port, reload=False, log_level="info")
