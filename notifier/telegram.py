from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from html import escape
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        self._send_message_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    @staticmethod
    def _is_truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _price_to_string(listing: dict[str, Any]) -> str:
        price = listing.get("price")
        if price is not None and str(price).strip() != "":
            try:
                price_value = float(price)
                if price_value.is_integer():
                    return f"{int(price_value)} €"
                return f"{price_value:g} €"
            except (TypeError, ValueError):
                value = str(price).strip()
                return value if "€" in value else f"{value} €"

        if TelegramNotifier._is_truthy(listing.get("price_negotiable")):
            return "VB (договорная)"

        return "Цена не указана"

    async def _send_text(self, text: str) -> bool:
        if not self.token or not self.chat_id:
            logger.error("Telegram credentials are missing")
            return False

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(self._send_message_url, json=payload)

            if response.status_code != 200:
                logger.error(
                    "Telegram API returned status %s: %s",
                    response.status_code,
                    response.text,
                )
                return False

            data = response.json()
            if not data.get("ok"):
                logger.error("Telegram API error response: %s", data)
                return False
            return True
        except Exception as exc:
            logger.error("Telegram send failed: %s", exc)
            return False

    async def send_listing(self, listing: dict) -> bool:
        title = escape(str(listing.get("title") or "Без названия"))
        price_str = escape(self._price_to_string(listing))
        location = escape(str(listing.get("location") or "Не указано"))
        first_seen_at = escape(
            str(listing.get("first_seen_at") or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
        )
        url = escape(str(listing.get("url") or ""), quote=True)

        text = (
            f"🆕 <b>{title}</b>\n\n"
            f"💰 {price_str}\n"
            f"📍 {location}\n"
            f"🕒 {first_seen_at}\n\n"
            f"<a href=\"{url}\">Открыть объявление →</a>"
        )
        return await self._send_text(text)

    async def send_batch(self, listings: list[dict], query_name: str) -> int:
        if not listings:
            return 0

        header = f"🔍 <b>{escape(query_name)}</b>: найдено {len(listings)} новых объявлений"
        await self._send_text(header)

        sent_count = 0
        for listing in listings:
            if await self.send_listing(listing):
                sent_count += 1
            await asyncio.sleep(0.5)

        return sent_count

    async def test_connection(self) -> bool:
        return await self._send_text("✅ Kleinanzeigen бот подключён")
