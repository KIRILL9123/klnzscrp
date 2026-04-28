from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class OllamaAnalyzer:
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "qwen2.5:7b") -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model

    def analyze(self, listing: dict, market_stats: dict) -> dict:
        """Run a synchronous Ollama analysis for a listing and market context."""
        system_prompt = (
            "Du bist ein Experte fur Schnappchenanalyse auf Kleinanzeigen.de.\n"
            "Analysiere das Inserat und antworte NUR mit einem JSON-Objekt.\n"
            "Kein Text auBerhalb von JSON. Kein Markdown. Nur reines JSON."
        )

        title = str(listing.get("title") or "Nicht angegeben")
        price = listing.get("price")
        price_text = f"{price}" if price is not None else "Nicht angegeben"
        negotiable_hint = "(VB - verhandelbar)" if bool(listing.get("price_negotiable")) else ""
        location = str(listing.get("location") or "Nicht angegeben")
        category = str(listing.get("category") or "Nicht angegeben")
        description = str(listing.get("description") or "Nicht angegeben")

        sample_count = int(market_stats.get("sample_count") or 0)
        query_name = str(market_stats.get("query_name") or "ahnlichen Inseraten")
        median_price = market_stats.get("median_price")
        min_price = market_stats.get("min_price")
        max_price = market_stats.get("max_price")

        median_text = f"{median_price}" if median_price is not None else "Nicht angegeben"
        min_text = f"{min_price}" if min_price is not None else "Nicht angegeben"
        max_text = f"{max_price}" if max_price is not None else "Nicht angegeben"

        user_prompt = (
            "Inserat:\n"
            f"Titel: {title}\n"
            f"Preis: {price_text} EUR {negotiable_hint}\n"
            f"Standort: {location}\n"
            f"Kategorie: {category}\n"
            f"Beschreibung: {description}\n\n"
            f"Marktdaten aus {sample_count} Inseraten zu {query_name}:\n"
            f"- Medianpreis: {median_text} EUR\n"
            f"- Minimum: {min_text} EUR\n"
            f"- Maximum: {max_text} EUR\n\n"
            "Antworte mit diesem JSON-Schema:\n"
            "{\n"
            "  \"score\": <int 1-10>,\n"
            "  \"verdict\": \"<kurzes Urteil auf Russisch>\",\n"
            "  \"price_assessment\": \"<'underpriced'|'fair'|'overpriced'>\",\n"
            "  \"risks\": [\"<Risiko auf Russisch>\"],\n"
            "  \"resale_margin\": \"<geschatzte Marge auf Russisch oder 'Nicht abschatzbar'>\",\n"
            "  \"recommendation\": \"<'buy'|'skip'|'negotiate'>\"\n"
            "}"
        )

        payload = {
            "model": self.model,
            "prompt": f"<system>\n{system_prompt}\n</system>\n\n{user_prompt}",
            "stream": False,
            "format": "json",
        }

        try:
            response = httpx.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=60.0,
            )
            response.raise_for_status()
            body = response.json()
            raw_json = body.get("response", "")
            if not isinstance(raw_json, str):
                raw_json = json.dumps(raw_json)

            try:
                parsed = json.loads(raw_json)
            except json.JSONDecodeError:
                logger.error("Ollama returned invalid JSON payload")
                return {"error": "Invalid JSON from model"}

            if isinstance(parsed, dict):
                return parsed
            return {"error": "Invalid JSON from model"}
        except Exception as exc:
            logger.error("Ollama analyze failed: %s", exc)
            return {"error": str(exc)}

    def is_available(self) -> bool:
        """Return True when Ollama tags endpoint responds with HTTP 200."""
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=10.0)
            return response.status_code == 200
        except Exception:
            return False
