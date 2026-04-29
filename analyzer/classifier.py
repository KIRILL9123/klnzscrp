from __future__ import annotations

import json
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ProductClassifier:
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "qwen2.5:7b") -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model

    def classify_batch(self, listings: list[dict]) -> list[dict]:
        """Classify listings in batches of up to five items."""
        if not listings:
            return []

        results: list[dict] = []
        for batch_start in range(0, len(listings), 5):
            batch = listings[batch_start:batch_start + 5]
            count = len(batch)
            try:
                payload = {
                    "model": self.model,
                    "prompt": self._build_prompt(batch),
                    "stream": False,
                }

                response = httpx.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    timeout=120.0,
                )
                response.raise_for_status()
                body = response.json()
                raw_text = body.get("response", "")
                if not isinstance(raw_text, str):
                    raw_text = json.dumps(raw_text, ensure_ascii=False)

                try:
                    parsed = json.loads(raw_text)
                except json.JSONDecodeError:
                    results.extend([{"error": "invalid_json"}] * count)
                    continue

                if not isinstance(parsed, list):
                    results.extend([{"error": "invalid_json"}] * count)
                    continue

                if len(parsed) != count:
                    results.extend([{"error": "length_mismatch"}] * count)
                    continue

                batch_results: list[dict] = []
                for item in parsed:
                    if isinstance(item, dict):
                        batch_results.append(item)
                    else:
                        batch_results.append({"error": "invalid_json"})
                results.extend(batch_results)
            except Exception as exc:
                logger.error("Ollama classification failed: %s", exc)
                results.extend([{"error": str(exc)}] * count)

            if batch_start + 5 < len(listings):
                time.sleep(0.5)

        return results

    def is_available(self) -> bool:
        """Return True when Ollama tags endpoint responds with HTTP 200."""
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=10.0)
            return response.status_code == 200
        except Exception:
            return False

    def _build_prompt(self, batch: list[dict]) -> str:
        lines: list[str] = []
        lines.append("Du bist ein Experte für Elektronik-Klassifizierung.")
        lines.append("Klassifiziere folgende Inserate und antworte NUR mit einem JSON-Array.")
        lines.append("Kein Text außerhalb. Kein Markdown. Nur JSON-Array mit [] Klammern.")
        lines.append("")

        for idx, listing in enumerate(batch, start=1):
            title = str(listing.get("title") or "Nicht angegeben")
            price_str = self._format_price(listing)
            category = str(listing.get("category") or "Nicht angegeben")
            description = str(listing.get("description") or "")
            description_short = description.replace("\n", " ").strip()[:150]
            if not description_short:
                description_short = "Nicht angegeben"

            lines.append(f"[{idx}] {title}")
            lines.append(f"  Preis: {price_str}")
            lines.append(f"  Kategorie: {category}")
            lines.append(f"  Beschreibung: {description_short}")
            lines.append("")

        lines.append("Antworte mit JSON-Array. Format-Beispiel:")
        lines.append("[")
        lines.append("  {")
        lines.append('    "product_type": "phone|laptop|desktop|console|gpu|monitor|tablet|accessory|service|other",')
        lines.append('    "brand": "markenname oder null",')
        lines.append('    "model": "modellname oder null",')
        lines.append("    \"is_accessory\": true|false,")
        lines.append("    \"is_service\": true|false,")
        lines.append("    \"specs\": {")
        lines.append('      "cpu": "oder null",')
        lines.append("      \"ram_gb\": null,")
        lines.append("      \"storage_gb\": null,")
        lines.append('      "gpu": "oder null",')
        lines.append("      \"screen_inch\": null,")
        lines.append('      "condition": "neu|sehr gut|gut|akzeptabel|defekt|unbekannt",')
        lines.append('      "color": "oder null",')
        lines.append('      "storage_variant": "z.B. 256GB oder null",')
        lines.append('      "included_items": "oder null"')
        lines.append("    },")
        lines.append('    "confidence": 0.85')
        lines.append("  }")
        lines.append("]")
        lines.append("")
        lines.append(f"Wichtig: Array muss exakt {len(batch)} Elemente haben, in dieser Reihenfolge:")

        user_prompt = "\n".join(lines)
        return user_prompt

    @staticmethod
    def _format_price(listing: dict[str, Any]) -> str:
        price = listing.get("price")
        price_negotiable = bool(listing.get("price_negotiable"))

        if price is not None:
            return f"{price} €"
        if price_negotiable:
            return "VB (Verhandlungsbasis)"
        return "Nicht angegeben"
