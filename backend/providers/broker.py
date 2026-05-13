from typing import Optional

class BrokerProvider:
    name="base"; display_name="Base"; needs_local_gateway=False; is_paper_only=False
    def __init__(self, config):
        self.config=config; self.connected=False; self.error=None
    async def connect(self): raise NotImplementedError
    async def disconnect(self): self.connected=False
    async def positions(self): return []
    async def account_summary(self): return {}

class PaperBroker(BrokerProvider):
    name="paper"; display_name="Internal Paper Trading"; is_paper_only=True
    def __init__(self, config): super().__init__(config); self.connected=True
    async def connect(self): self.connected=True; return True

class IBKRBroker(BrokerProvider):
    name="ibkr"; display_name="Interactive Brokers"; needs_local_gateway=True
    def __init__(self, config): super().__init__(config); self.ib=None
    async def connect(self):
        try:
            from ib_insync import IB
            self.ib=IB()
            await self.ib.connectAsync(self.config.get("host","127.0.0.1"),int(self.config.get("port",7497)),clientId=int(self.config.get("client_id",17)),timeout=8)
            self.connected=self.ib.isConnected(); return True
        except Exception as e: self.error=str(e); return False
    async def disconnect(self):
        if self.ib and self.ib.isConnected(): self.ib.disconnect()
        self.connected=False

class AlpacaBroker(BrokerProvider):
    name="alpaca"; display_name="Alpaca Markets"
    def __init__(self, config):
        super().__init__(config)
        self.api_key=config.get("alpaca_key",""); self.api_secret=config.get("alpaca_secret","")
        self.base_url="https://paper-api.alpaca.markets" if config.get("alpaca_paper",True) else "https://api.alpaca.markets"
    async def connect(self):
        if not self.api_key: self.error="Alpaca key required"; return False
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as c:
                r=await c.get(f"{self.base_url}/v2/account",headers={"APCA-API-KEY-ID":self.api_key,"APCA-API-SECRET-KEY":self.api_secret})
            self.connected=r.status_code==200; return self.connected
        except Exception as e: self.error=str(e); return False

PROVIDERS={"paper":PaperBroker,"ibkr":IBKRBroker,"alpaca":AlpacaBroker}

def list_providers():
    return [{"id":cls.name,"name":cls.display_name,"needs_local_gateway":cls.needs_local_gateway,"is_paper_only":cls.is_paper_only} for cls in PROVIDERS.values()]

def get_provider(name, config):
    if name not in PROVIDERS: raise ValueError(f"Unknown broker '{name}'")
    return PROVIDERS[name](config=config)
