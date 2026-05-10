import httpx
from typing import Optional

class AIProvider:
    name = "base"; display_name = "Base"; api_key_field = "api_key"
    key_url = ""; free_tier = False; default_model = ""
    def __init__(self, api_key, model=None):
        self.api_key = api_key; self.model = model or self.default_model
    async def generate(self, system, user, max_tokens=1000): raise NotImplementedError

class GeminiProvider(AIProvider):
    name="gemini"; display_name="Google Gemini"; api_key_field="gemini"
    key_url="https://aistudio.google.com/apikey"; free_tier=True; default_model="gemini-2.0-flash"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}",
                json={"contents":[{"role":"user","parts":[{"text":f"SYSTEM:\n{system}\n\nUSER:\n{user}"}]}],"generationConfig":{"maxOutputTokens":max_tokens,"temperature":0.7}})
        if r.status_code!=200: raise RuntimeError(f"Gemini {r.status_code}: {r.text[:200]}")
        return r.json()["candidates"][0]["content"]["parts"][0]["text"]

class AnthropicProvider(AIProvider):
    name="anthropic"; display_name="Anthropic Claude"; api_key_field="anthropic"
    key_url="https://console.anthropic.com"; free_tier=False; default_model="claude-sonnet-4-20250514"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":self.api_key,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":self.model,"max_tokens":max_tokens,"system":system,"messages":[{"role":"user","content":user}]})
        if r.status_code!=200: raise RuntimeError(f"Anthropic {r.status_code}: {r.text[:200]}")
        return "".join(b.get("text","") for b in r.json().get("content",[]) if b.get("type")=="text")

class OpenAIProvider(AIProvider):
    name="openai"; display_name="OpenAI GPT-4o"; api_key_field="openai"
    key_url="https://platform.openai.com/api-keys"; free_tier=False; default_model="gpt-4o-mini"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},
                json={"model":self.model,"max_tokens":max_tokens,"messages":[{"role":"system","content":system},{"role":"user","content":user}]})
        if r.status_code!=200: raise RuntimeError(f"OpenAI {r.status_code}: {r.text[:200]}")
        return r.json()["choices"][0]["message"]["content"]

class GroqProvider(AIProvider):
    name="groq"; display_name="Groq (Llama 3.3)"; api_key_field="groq"
    key_url="https://console.groq.com/keys"; free_tier=True; default_model="llama-3.3-70b-versatile"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},
                json={"model":self.model,"max_tokens":max_tokens,"messages":[{"role":"system","content":system},{"role":"user","content":user}]})
        if r.status_code!=200: raise RuntimeError(f"Groq {r.status_code}: {r.text[:200]}")
        return r.json()["choices"][0]["message"]["content"]

class DeepSeekProvider(AIProvider):
    name="deepseek"; display_name="DeepSeek"; api_key_field="deepseek"
    key_url="https://platform.deepseek.com/api_keys"; free_tier=False; default_model="deepseek-chat"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.deepseek.com/chat/completions",
                headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},
                json={"model":self.model,"max_tokens":max_tokens,"messages":[{"role":"system","content":system},{"role":"user","content":user}]})
        if r.status_code!=200: raise RuntimeError(f"DeepSeek {r.status_code}: {r.text[:200]}")
        return r.json()["choices"][0]["message"]["content"]

class MistralProvider(AIProvider):
    name="mistral"; display_name="Mistral"; api_key_field="mistral"
    key_url="https://console.mistral.ai/api-keys/"; free_tier=True; default_model="mistral-small-latest"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.mistral.ai/v1/chat/completions",
                headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},
                json={"model":self.model,"max_tokens":max_tokens,"messages":[{"role":"system","content":system},{"role":"user","content":user}]})
        if r.status_code!=200: raise RuntimeError(f"Mistral {r.status_code}: {r.text[:200]}")
        return r.json()["choices"][0]["message"]["content"]

class CohereProvider(AIProvider):
    name="cohere"; display_name="Cohere Command"; api_key_field="cohere"
    key_url="https://dashboard.cohere.com/api-keys"; free_tier=True; default_model="command-r-plus"
    async def generate(self, system, user, max_tokens=1000):
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.cohere.com/v2/chat",
                headers={"Authorization":f"Bearer {self.api_key}","Content-Type":"application/json"},
                json={"model":self.model,"max_tokens":max_tokens,"messages":[{"role":"system","content":system},{"role":"user","content":user}]})
        if r.status_code!=200: raise RuntimeError(f"Cohere {r.status_code}: {r.text[:200]}")
        try: return r.json()["message"]["content"][0]["text"]
        except: return ""

PROVIDERS = {"gemini":GeminiProvider,"anthropic":AnthropicProvider,"openai":OpenAIProvider,
             "groq":GroqProvider,"deepseek":DeepSeekProvider,"mistral":MistralProvider,"cohere":CohereProvider}

def list_providers():
    return [{"id":cls.name,"name":cls.display_name,"key_field":cls.api_key_field,
             "key_url":cls.key_url,"free_tier":cls.free_tier,"default_model":cls.default_model}
            for cls in PROVIDERS.values()]

def get_provider(name, api_key, model=None):
    if name not in PROVIDERS: raise ValueError(f"Unknown AI provider '{name}'")
    return PROVIDERS[name](api_key=api_key, model=model)
