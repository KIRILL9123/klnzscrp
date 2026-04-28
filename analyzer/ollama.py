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
            "Du bist ein Experte für Wiederverkauf von Elektronik auf Kleinanzeigen.de.\n"
            "Deine Aufgabe: analysiere das Inserat und bewerte es als\n"
            "Wiederverkaufsgelegenheit.\n"
            "Antworte NUR mit einem JSON-Objekt. Kein Text außerhalb von JSON.\n"
            "Kein Markdown. Nur reines JSON.\n"
            "Alle Textfelder (verdict, risks, resale_margin) MÜSSEN auf Russisch sein."
        )

        title = str(listing.get("title") or "Nicht angegeben")
        price = listing.get("price")
        price_negotiable = bool(listing.get("price_negotiable"))
        
        if price is not None:
            price_str = f"{price} €"
        elif price_negotiable:
            price_str = "VB (Verhandlungsbasis)"
        else:
            price_str = "Nicht angegeben"
        
        location = str(listing.get("location") or "Nicht angegeben")
        category = str(listing.get("category") or "Nicht angegeben")
        description = str(listing.get("description") or "Nicht angegeben")

        sample_count = int(market_stats.get("sample_count") or 0)
        median_price = market_stats.get("median_price")
        min_price = market_stats.get("min_price")
        max_price = market_stats.get("max_price")

        median_price_str = f"{median_price:.0f} €" if median_price is not None else "Keine Daten"
        min_price_str = f"{min_price} €" if min_price is not None else "—"
        max_price_str = f"{max_price} €" if max_price is not None else "—"
        
        price_comparison = ""
        if price is not None and median_price is not None:
            diff = price - median_price
            pct = (diff / median_price) * 100 if median_price != 0 else 0
            if diff < 0:
                price_comparison = f"Dieser Preis liegt {abs(pct):.0f}% UNTER dem Median."
            elif diff > 0:
                price_comparison = f"Dieser Preis liegt {pct:.0f}% ÜBER dem Median."
            else:
                price_comparison = "Dieser Preis entspricht genau dem Median."
        
        suggested_price = None
        if price is None and (price_negotiable or True):
            if median_price is not None:
                suggested_price = int(median_price * 0.8)

        user_prompt = (
            f"Inserat:\n"
            f"Titel: {title}\n"
            f"Preis: {price_str}\n"
            f"Standort: {location}\n"
            f"Kategorie: {category}\n"
            f"Beschreibung: {description}\n\n"
            f"Marktdaten ({sample_count} ähnliche Inserate auf Kleinanzeigen.de):\n"
            f"Medianpreis: {median_price_str}\n"
            f"Minimum: {min_price_str}\n"
            f"Maximum: {max_price_str}\n\n"
            f"{price_comparison}\n\n"
            f"Bewerte nach diesen Kriterien:\n"
            f"1. Preis im Vergleich zum Marktmedian\n"
            f"2. Wiederverkaufspotenzial (typische Marge für Elektronik: 15-30%)\n"
            f"3. Risiken: fehlende Beschreibung, unklarer Zustand, verdächtig niedriger Preis\n"
            f"4. Falls kein Preis angegeben (VB): empfehle konkreten Verhandlungspreis\n"
            f"   basierend auf dem Marktmedian minus 20%\n\n"
            f"Antworte mit diesem JSON-Schema:\n"
            "{\n"
            "  \"score\": <int 1-10>,\n"
            "  \"verdict\": \"<2-3 Sätze auf Russisch>\",\n"
            "  \"price_assessment\": \"<'underpriced'|'fair'|'overpriced'|'unknown'>\",\n"
            "  \"risks\": [\"<Risiko auf Russisch>\"],\n"
            "  \"resale_margin\": \"<konkrete Schätzung auf Russisch, z.B. '~50-80€ Gewinn möglich'>\",\n"
            "  \"recommendation\": \"<'buy'|'skip'|'negotiate'>\",\n"
            f"  \"suggested_price\": {suggested_price if suggested_price else 'null'}\n"
            "}\n\n"
            "suggested_price: falls VB oder Preis fehlt — empfohlener Kaufpreis in Euro.\n"
            "Falls Preis bekannt — null."
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
