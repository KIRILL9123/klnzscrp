from __future__ import annotations

import asyncio
from pathlib import Path
import sys

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config.yaml"

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scraper.browser import scrape_search


def _load_first_search_url() -> str:
    """Load the first search URL from global config."""
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    searches = cfg.get("searches", [])
    if not searches:
        raise ValueError("No search queries found in config.yaml")

    url = str(searches[0].get("url", "")).strip()
    if not url:
        raise ValueError("First search URL in config.yaml is empty")
    return url


def _format_table(rows: list[dict[str, str]]) -> str:
    """Render a simple plain-text table without external dependencies."""
    headers = ["title", "price", "location", "extracted_id"]
    widths = {h: len(h) for h in headers}

    for row in rows:
        for h in headers:
            widths[h] = max(widths[h], len(row.get(h, "")))

    line = "+" + "+".join("-" * (widths[h] + 2) for h in headers) + "+"
    header_line = "| " + " | ".join(h.ljust(widths[h]) for h in headers) + " |"

    body = []
    for row in rows:
        body.append("| " + " | ".join(row.get(h, "").ljust(widths[h]) for h in headers) + " |")

    return "\n".join([line, header_line, line, *body, line])


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def _run_smoke_test() -> None:
    url = _load_first_search_url()
    listings = await scrape_search(url=url, max_pages=1)

    top = listings[:3]
    if len(top) < 3:
        print(f"WARNING: only {len(top)} listing(s) parsed; expected at least 3 for smoke test")

    table_rows: list[dict[str, str]] = []
    for item in top:
        table_rows.append(
            {
                "title": _normalize_text(item.get("title")),
                "price": _normalize_text(item.get("price")),
                "location": _normalize_text(item.get("location")),
                "extracted_id": _normalize_text(item.get("id")),
            }
        )

    if table_rows:
        print(_format_table(table_rows))
    else:
        print("WARNING: no listings parsed")
        return

    watched_fields = ["title", "price", "location", "extracted_id"]
    for field in watched_fields:
        if all(not row[field] for row in table_rows):
            print(f"WARNING: field '{field}' is empty for all tested listings")


if __name__ == "__main__":
    asyncio.run(_run_smoke_test())
