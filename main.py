from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from scheduler.jobs import build_scheduler, register_jobs, run_single_search_job
from storage.database import ensure_search_query, init_db

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.yaml"


def setup_logging() -> None:
    """Configure global logger for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_config() -> dict[str, Any]:
    """Load YAML configuration from project root."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ValueError("config.yaml has invalid root format")

    return cfg


def validate_config(config: dict[str, Any]) -> None:
    """Validate critical config constraints and anti-ban rules."""
    scheduler_cfg = config.get("scheduler", {})
    interval_minutes = int(scheduler_cfg.get("interval_minutes", 0))
    if interval_minutes < 30:
        raise ValueError("scheduler.interval_minutes must be at least 30")

    scraper_cfg = config.get("scraper", {})
    min_delay = float(scraper_cfg.get("min_delay_seconds", 0))
    max_delay = float(scraper_cfg.get("max_delay_seconds", 0))
    if min_delay < 0 or max_delay < min_delay:
        raise ValueError("scraper delay range is invalid")


def sync_search_queries_from_config(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Insert configured queries into DB if missing and return DB-backed list."""
    searches = config.get("searches", [])
    if not isinstance(searches, list) or not searches:
        raise ValueError("config.yaml must include at least one search in 'searches'")

    queries: list[dict[str, Any]] = []
    for search in searches:
        name = str(search["name"]).strip()
        url = str(search["url"]).strip()
        row = ensure_search_query(name=name, url=url)
        queries.append({"id": row.id, "name": row.name, "url": row.url})

    return queries


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    config = load_config()
    validate_config(config)

    init_db()

    queries = sync_search_queries_from_config(config)
    scraper_cfg = config["scraper"]
    interval_minutes = int(config["scheduler"]["interval_minutes"])

    scheduler = build_scheduler()
    register_jobs(
        scheduler=scheduler,
        queries=queries,
        interval_minutes=interval_minutes,
        scraper_cfg=scraper_cfg,
    )

    # Run each query once at startup, then continue on schedule.
    for query in queries:
        run_single_search_job(
            query_id=query["id"],
            query_name=query["name"],
            query_url=query["url"],
            scraper_cfg=scraper_cfg,
        )

    logger.info("Scheduler started. Running %s jobs every %s minutes.", len(queries), interval_minutes)
    scheduler.start()


if __name__ == "__main__":
    main()
