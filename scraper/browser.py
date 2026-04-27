from __future__ import annotations

import asyncio
import logging
import random
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse, urljoin
from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)
_STEALTH = Stealth()

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

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
    text = (price_text or "").strip()
    if not text:
        return None, False

    has_vb = bool(re.search(r"\bvb\b", text, flags=re.IGNORECASE))
    match = re.search(r"(\d[\d\s\.,]*)", text)
    if not match:
        return None, has_vb

    digits = re.sub(r"\D", "", match.group(1))
    if not digits:
        return None, has_vb

    return int(digits), has_vb


def _build_next_page_url(current_url: str, next_page_number: int) -> str | None:
    """Build the next Kleinanzeigen search page URL using the seite:N pattern."""
    parsed = urlparse(current_url)
    path = parsed.path or ""
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return None

    next_page_segment = f"seite:{next_page_number}"

    # Replace existing seite:X segment if present.
    for idx, segment in enumerate(segments):
        if re.fullmatch(r"seite:\d+", segment):
            segments[idx] = next_page_segment
            next_path = "/" + "/".join(segments)
            return urlunparse((parsed.scheme, parsed.netloc, next_path, "", "", ""))

    # Insert before the segment that precedes the k0* tail segment.
    # For short paths like /s-iphone-15/k0 this still places seite before k0.
    k0_idx = next((idx for idx, segment in enumerate(segments) if segment.startswith("k0")), None)
    if k0_idx is not None:
        insert_idx = max(k0_idx - 1, 1)
    else:
        # Fallback: insert before the last segment.
        insert_idx = max(len(segments) - 1, 0)

    segments.insert(insert_idx, next_page_segment)

    next_path = "/" + "/".join(segments)
    return urlunparse((parsed.scheme, parsed.netloc, next_path, "", "", ""))


async def _create_context(browser: Browser) -> BrowserContext:
    """Create browser context with realistic headers."""
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1366, "height": 768},
        locale="de-DE",
        timezone_id="Europe/Berlin",
    )
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
    min_delay_seconds: float = 2.0,
    max_delay_seconds: float = 6.0,
    headless: bool = True,
) -> list[dict[str, Any]]:
    """Scrape a kleinanzeigen search URL page-by-page sequentially."""
    if max_pages < 1:
        return []

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
            logger.info("Scraping page %s of max %s: %s", page_number, max_pages, current_url)
            await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1200)

            if page.url.rstrip("/") != current_url.rstrip("/"):
                logger.info(
                    "Stopping pagination because page redirected from %s to %s",
                    current_url,
                    page.url,
                )
                break

            page_items = await _collect_listing_cards(page)
            if not page_items:
                logger.info("Stopping pagination because page has 0 listings: %s", current_url)
                break
            listings.extend(page_items)

            if page_number >= max_pages:
                break

            next_url = _build_next_page_url(current_url=current_url, next_page_number=page_number + 1)
            if not next_url:
                break

            current_url = next_url

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


def _run_next_page_url_unit_test() -> None:
    test_cases = [
        (
            "https://www.kleinanzeigen.de/s-iphone-15/k0",
            2,
            "https://www.kleinanzeigen.de/s-iphone-15/seite:2/k0",
        ),
        (
            "https://www.kleinanzeigen.de/s-konsolen/erfurt/playstation-5/k0c279l3741r30",
            2,
            "https://www.kleinanzeigen.de/s-konsolen/erfurt/seite:2/playstation-5/k0c279l3741r30",
        ),
        (
            "https://www.kleinanzeigen.de/s-iphone-15/seite:3/k0",
            4,
            "https://www.kleinanzeigen.de/s-iphone-15/seite:4/k0",
        ),
        (
            "https://www.kleinanzeigen.de/s-haushalt/moebel/berlin",
            2,
            "https://www.kleinanzeigen.de/s-haushalt/moebel/seite:2/berlin",
        ),
    ]

    passed = 0
    for idx, (source_url, page, expected_url) in enumerate(test_cases, start=1):
        actual_url = _build_next_page_url(source_url, page)
        ok = actual_url == expected_url
        status = "PASS" if ok else "FAIL"
        print(f"[{status}] case #{idx}")
        print(f"  input:    {source_url}")
        print(f"  expected: {expected_url}")
        print(f"  actual:   {actual_url}")
        if ok:
            passed += 1

    total = len(test_cases)
    print(f"\nResult: {passed}/{total} passed")


if __name__ == "__main__":
    _run_next_page_url_unit_test()
