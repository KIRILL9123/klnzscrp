from __future__ import annotations

import asyncio
import logging
from typing import Any

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler

from scraper.browser import scrape_search
from storage.database import get_listing_ids_for_query, mark_inactive, upsert_listing, update_last_run

logger = logging.getLogger(__name__)


def run_single_search_job(query_id: int, query_name: str, query_url: str, scraper_cfg: dict[str, Any]) -> None:
    """Run one full scrape cycle for a single search query."""
    logger.info("Starting job for query '%s'", query_name)

    listings = asyncio.run(
        scrape_search(
            url=query_url,
            max_pages=int(scraper_cfg["max_pages"]),
        )
    )

    created = 0
    updated = 0
    scraped_ids: set[str] = set()
    for item in listings:
        scraped_ids.add(item["id"])
        result = upsert_listing(item, query_id=query_id)
        if result == "created":
            created += 1
        else:
            updated += 1

    db_ids = set(get_listing_ids_for_query(query_id))
    missing_ids = sorted(db_ids - scraped_ids)
    deactivated = mark_inactive(missing_ids)

    update_last_run(query_id)

    logger.info(
        "Finished query '%s': total=%s, new=%s, updated=%s, deactivated=%s",
        query_name,
        len(listings),
        created,
        updated,
        deactivated,
    )


def build_scheduler() -> BlockingScheduler:
    """Build scheduler with a single worker to enforce sequential scraping."""
    executors = {
        "default": ThreadPoolExecutor(max_workers=1),
    }
    return BlockingScheduler(executors=executors)


def register_jobs(
    scheduler: BlockingScheduler,
    queries: list[dict[str, Any]],
    default_interval_minutes: int,
    scraper_cfg: dict[str, Any],
) -> None:
    """Register interval jobs for each configured search query."""
    for query in queries:
        query_interval = query.get("interval_minutes")
        minutes = int(query_interval) if query_interval is not None else int(default_interval_minutes)
        scheduler.add_job(
            func=run_single_search_job,
            trigger="interval",
            minutes=minutes,
            kwargs={
                "query_id": query["id"],
                "query_name": query["name"],
                "query_url": query["url"],
                "scraper_cfg": scraper_cfg,
            },
            id=f"query_{query['id']}",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
