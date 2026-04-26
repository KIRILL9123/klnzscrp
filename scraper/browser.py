from __future__ import annotations

import asyncio
import logging
import random
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import yaml
from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)
_STEALTH = Stealth()

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_BASE_DIR = Path(__file__).resolve().parents[1]
_CONFIG_PATH = _BASE_DIR / "config.yaml"


def _load_scraper_config() -> dict[str, Any]:
    """Load scraper configuration from the global project config."""
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {_CONFIG_PATH}")

    with _CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    scraper_cfg = cfg.get("scraper", {})
    if not isinstance(scraper_cfg, dict):
        raise ValueError("scraper config section is invalid")
    return scraper_cfg


def extract_listing_id(url: str) -> str | None:
    """Extract kleinanzeigen listing ID from URL path."""
    match = re.search(r"/s-anzeige/[^/]+/(\d+)-\d+-\d+", url)
    if match:
        return match.group(1)

    tail = url.rstrip("/").split("/")[-1]
    tail_match = re.match(r"(\d+)-", tail)
    if tail_match:
        return tail_match.group(1)
    return None


def _parse_price(price_text: str) -> tuple[int | None, bool]:
    """Parse numeric euro value and negotiable marker from raw text."""
    negotiable = "vb" in price_text.lower()
    digits = re.sub(r"[^\d]", "", price_text)
    price = int(digits) if digits else None
    return price, negotiable


async def _create_context(browser: Browser) -> BrowserContext:
    """Create browser context with stealth and realistic headers."""
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1366, "height": 768},
        locale="de-DE",
        timezone_id="Europe/Berlin",
    )

    # Apply stealth to every page in this context.
    page = await context.new_page()
    await _STEALTH.apply_stealth_async(page)
    return context


async def _collect_listing_cards(page: Page) -> list[dict[str, Any]]:
    """Extract listing data from current search results page."""
    selectors = [
        "article.aditem",
        "li.ad-listitem",
        "article[data-adid]",
    ]

    cards = []
    for selector in selectors:
        locator = page.locator(selector)
        count = await locator.count()
        if count:
            cards = [locator.nth(i) for i in range(count)]
            break

    if not cards:
        logger.warning("No listing cards found on page: %s", page.url)
        return []

    items: list[dict[str, Any]] = []

    for card in cards:
        raw_text = await card.inner_text()
        if "Top-Inserat" in raw_text:
            continue

        link = card.locator("a[href*='/s-anzeige/']").first
        href = await link.get_attribute("href")
        if not href:
            continue

        full_url = urljoin("https://www.kleinanzeigen.de", href)
        listing_id = extract_listing_id(full_url)
        if not listing_id:
            continue

        title = ""
        title_locator = card.locator("h2, h3, a.ellipsis")
        if await title_locator.count():
            title = (await title_locator.first.inner_text()).strip()

        price_text = ""
        price_locator = card.locator(".aditem-main--middle--price-shipping--price, p.aditem-main--middle--price-shipping--price")
        if await price_locator.count():
            price_text = (await price_locator.first.inner_text()).strip()
        else:
            if "VB" in raw_text or "EUR" in raw_text or "€" in raw_text:
                price_text = raw_text

        price, price_negotiable = _parse_price(price_text)

        location = ""
        location_locator = card.locator(".aditem-main--top--left")
        if await location_locator.count():
            location = (await location_locator.first.inner_text()).strip()

        category = ""
        category_locator = card.locator(".simpletag.tag-small")
        if await category_locator.count():
            category = (await category_locator.first.inner_text()).strip()

        items.append(
            {
                "id": listing_id,
                "title": title,
                "price": price,
                "price_negotiable": price_negotiable,
                "location": location,
                "category": category,
                "url": full_url,
            }
        )

    return items


async def scrape_search(
    url: str,
    max_pages: int,
) -> list[dict[str, Any]]:
    """Scrape a kleinanzeigen search URL page-by-page sequentially."""
    if max_pages < 1:
        return []

    scraper_cfg = _load_scraper_config()
    min_delay_seconds = float(scraper_cfg.get("min_delay_seconds", 2))
    max_delay_seconds = float(scraper_cfg.get("max_delay_seconds", 6))
    headless = bool(scraper_cfg.get("headless", True))

    if min_delay_seconds < 0 or max_delay_seconds < min_delay_seconds:
        raise ValueError("Invalid delay range provided")

    listings: list[dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await _create_context(browser)
        page = await context.new_page()
        await _STEALTH.apply_stealth_async(page)

        current_url = url

        for page_number in range(1, max_pages + 1):
            logger.info("Scraping page %s: %s", page_number, current_url)
            await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1200)

            page_items = await _collect_listing_cards(page)
            listings.extend(page_items)

            next_link = page.locator("a[rel='next'], a[aria-label*='Nächste']").first
            if await next_link.count() == 0:
                break

            href = await next_link.get_attribute("href")
            if not href:
                break

            current_url = urljoin("https://www.kleinanzeigen.de", href)

            # Delay between page transitions is required to reduce ban risk.
            await asyncio.sleep(random.uniform(min_delay_seconds, max_delay_seconds))

        await context.close()
        await browser.close()

    unique = {item["id"]: item for item in listings}
    return list(unique.values())


async def scrape_listing_detail(url: str, headless: bool = True) -> dict[str, Any]:
    """Optionally scrape listing detail page for description."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await _create_context(browser)
        page = await context.new_page()
        await _STEALTH.apply_stealth_async(page)

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)

        description = ""
        desc_locator = page.locator("#viewad-description-text, .addetailslist--detail")
        if await desc_locator.count():
            description = (await desc_locator.first.inner_text()).strip()

        await context.close()
        await browser.close()

    return {"url": url, "description": description}
