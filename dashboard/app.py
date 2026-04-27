from __future__ import annotations

import asyncio
import csv
import io
import logging
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, Response, jsonify, render_template, request

from notifier.telegram import TelegramNotifier
from scraper.browser import scrape_search
from storage.database import (
    DB_PATH,
    create_query_for_dashboard,
    create_scrape_log,
    delete_listings_by_query_for_dashboard,
    delete_query_and_listings_for_dashboard,
    finish_scrape_log,
    get_latest_scrape_status_for_query,
    get_query_for_dashboard,
    get_scrape_log,
    get_settings,
    get_listing_ids_for_query,
    init_dashboard_db,
    list_queries_for_dashboard,
    list_scrape_logs,
    mark_inactive,
    toggle_query_for_dashboard,
    update_last_run,
    update_query_for_dashboard,
    update_settings,
    upsert_listing,
)

APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR.parent / "config.yaml"

app = Flask(__name__, template_folder="templates")

scheduler = BackgroundScheduler(executors={"default": ThreadPoolExecutor(max_workers=1)})
running_queries: set[int] = set()
running_queries_lock = threading.Lock()
run_all_lock = threading.Lock()
logger = logging.getLogger(__name__)

TELEGRAM_SETTINGS_DEFAULTS = {
    "telegram_token": "",
    "telegram_chat_id": "",
    "telegram_enabled": "false",
    "telegram_min_price": "",
    "telegram_max_price": "",
}


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _fmt_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_bool(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_priced_filter(value: str | None) -> str:
    if value is None:
        return "all"
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on", "priced"}:
        return "priced"
    if normalized in {"0", "false", "no", "off", "unpriced"}:
        return "unpriced"
    if normalized in {"vb", "negotiable"}:
        return "vb"
    return "all"


def _parse_nullable_interval(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None

    parsed = int(value)
    if parsed < 30:
        raise ValueError("interval_minutes must be >= 30")
    return parsed


def _bool_string(value: str | bool) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return "true" if str(value).strip().lower() in {"1", "true", "yes", "on"} else "false"


def _parse_optional_price_setting(value: Any) -> float | None:
    if value is None:
        return None
    raw = str(value).strip()
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _normalize_optional_price(value: Any) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if raw == "":
        return ""

    parsed = float(raw)
    if parsed < 0:
        raise ValueError("price filters must be >= 0")

    return str(int(parsed)) if parsed.is_integer() else str(parsed)


def _ensure_telegram_settings_defaults() -> None:
    settings = get_settings()
    missing = {key: value for key, value in TELEGRAM_SETTINGS_DEFAULTS.items() if key not in settings}
    if missing:
        update_settings(missing)


def _get_telegram_runtime_settings() -> dict[str, Any]:
    settings = get_settings()
    return {
        "token": str(settings.get("telegram_token", "")).strip(),
        "chat_id": str(settings.get("telegram_chat_id", "")).strip(),
        "enabled": _bool_string(settings.get("telegram_enabled", "false")) == "true",
        "min_price": _parse_optional_price_setting(settings.get("telegram_min_price", "")),
        "max_price": _parse_optional_price_setting(settings.get("telegram_max_price", "")),
    }


def _filter_listings_by_price(
    listings: list[dict[str, Any]],
    min_price: float | None,
    max_price: float | None,
) -> list[dict[str, Any]]:
    if min_price is None and max_price is None:
        return listings

    filtered: list[dict[str, Any]] = []
    for item in listings:
        price = item.get("price")
        if price is None:
            continue
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            continue

        if min_price is not None and price_value < min_price:
            continue
        if max_price is not None and price_value > max_price:
            continue
        filtered.append(item)

    return filtered


def _get_typed_settings() -> dict[str, Any]:
    settings = get_settings()
    return {
        "interval_minutes": int(settings.get("interval_minutes", "120")),
        "min_delay_seconds": float(settings.get("min_delay_seconds", "2")),
        "max_delay_seconds": float(settings.get("max_delay_seconds", "6")),
        "max_pages": int(settings.get("max_pages", "5")),
        "headless": _bool_string(settings.get("headless", "true")) == "true",
    }


def _sync_config_from_settings(settings: dict[str, Any]) -> None:
    if CONFIG_PATH.exists():
        with CONFIG_PATH.open("r", encoding="utf-8") as file_obj:
            cfg = yaml.safe_load(file_obj) or {}
    else:
        cfg = {}

    cfg.setdefault("scheduler", {})
    cfg.setdefault("scraper", {})

    cfg["scheduler"]["interval_minutes"] = int(settings["interval_minutes"])
    cfg["scraper"]["min_delay_seconds"] = float(settings["min_delay_seconds"])
    cfg["scraper"]["max_delay_seconds"] = float(settings["max_delay_seconds"])
    cfg["scraper"]["max_pages"] = int(settings["max_pages"])
    cfg["scraper"]["headless"] = bool(settings["headless"])

    with CONFIG_PATH.open("w", encoding="utf-8") as file_obj:
        yaml.safe_dump(cfg, file_obj, sort_keys=False, allow_unicode=False)


def _job_id(query_id: int) -> str:
    return f"query_{query_id}"


def _is_query_running(query_id: int) -> bool:
    with running_queries_lock:
        return query_id in running_queries


def _mark_query_running(query_id: int) -> bool:
    with running_queries_lock:
        if query_id in running_queries:
            return False
        running_queries.add(query_id)
        return True


def _mark_query_finished(query_id: int) -> None:
    with running_queries_lock:
        running_queries.discard(query_id)


def _register_or_replace_job(query: dict[str, Any], interval_minutes: int) -> None:
    if int(query.get("is_active") or 0) != 1:
        return

    query_interval = query.get("interval_minutes")
    minutes = int(query_interval) if query_interval is not None else int(interval_minutes)

    scheduler.add_job(
        func=_run_query_scrape_now,
        trigger="interval",
        minutes=minutes,
        kwargs={"query_id": int(query["id"]), "scheduled": True},
        id=_job_id(int(query["id"])),
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )


def _reload_scheduler_jobs() -> None:
    settings = _get_typed_settings()
    interval_minutes = int(settings["interval_minutes"])

    for job in scheduler.get_jobs():
        scheduler.remove_job(job.id)

    for query in list_queries_for_dashboard():
        if int(query.get("is_active") or 0) == 1:
            _register_or_replace_job(query, interval_minutes)


def _run_query_scrape_with_log(query_id: int, log_id: int) -> None:
    query = get_query_for_dashboard(query_id)
    if not query:
        finish_scrape_log(log_id=log_id, status="error", error_message="Query not found")
        return

    if not _mark_query_running(query_id):
        finish_scrape_log(log_id=log_id, status="error", error_message="Query already running")
        return

    try:
        settings = _get_typed_settings()
        _sync_config_from_settings(settings)

        listings = asyncio.run(
            scrape_search(
                url=str(query["url"]),
                max_pages=int(settings["max_pages"]),
                min_delay_seconds=float(settings["min_delay_seconds"]),
                max_delay_seconds=float(settings["max_delay_seconds"]),
                headless=bool(settings["headless"]),
            )
        )

        created = 0
        updated = 0
        new_listings: list[dict[str, Any]] = []
        scraped_ids: set[str] = set()
        for item in listings:
            scraped_ids.add(item["id"])
            result = upsert_listing(item, query_id=query_id)
            if result == "created":
                created += 1
                created_item = dict(item)
                created_item.setdefault("first_seen_at", _fmt_datetime(datetime.utcnow()))
                new_listings.append(created_item)
            else:
                updated += 1

        db_ids = set(get_listing_ids_for_query(query_id))
        missing_ids = sorted(db_ids - scraped_ids)
        deactivated = mark_inactive(missing_ids)

        update_last_run(query_id)
        finish_scrape_log(
            log_id=log_id,
            status="success",
            new_count=created,
            updated_count=updated,
            deactivated_count=deactivated,
        )

        telegram = _get_telegram_runtime_settings()
        query_telegram_enabled = int(query.get("telegram_enabled") or 0) == 1
        if created > 0 and telegram["enabled"] and query_telegram_enabled:
            try:
                if not telegram["token"] or not telegram["chat_id"]:
                    logger.warning("Telegram notifications are enabled but token/chat_id are missing")
                else:
                    filtered_new_listings = _filter_listings_by_price(
                        new_listings,
                        min_price=telegram["min_price"],
                        max_price=telegram["max_price"],
                    )
                    if filtered_new_listings:
                        notifier = TelegramNotifier(
                            token=str(telegram["token"]),
                            chat_id=str(telegram["chat_id"]),
                        )
                        sent_count = asyncio.run(
                            notifier.send_batch(
                                filtered_new_listings,
                                query_name=str(query.get("name") or f"query_{query_id}"),
                            )
                        )
                        logger.info(
                            "Telegram notifications sent %s/%s for query_id=%s",
                            sent_count,
                            len(filtered_new_listings),
                            query_id,
                        )
            except Exception as telegram_exc:
                logger.error("Telegram notification flow failed: %s", telegram_exc)
    except Exception as exc:
        finish_scrape_log(log_id=log_id, status="error", error_message=str(exc))
    finally:
        _mark_query_finished(query_id)


def _run_query_scrape_now(query_id: int, scheduled: bool = False) -> dict[str, Any]:
    if _is_query_running(query_id):
        return {"status": "already_running"}

    query = get_query_for_dashboard(query_id)
    if not query:
        return {"status": "not_found"}

    if scheduled and int(query.get("is_active") or 0) != 1:
        return {"status": "inactive"}

    log_id = create_scrape_log(query_id)
    thread = threading.Thread(
        target=_run_query_scrape_with_log,
        kwargs={"query_id": query_id, "log_id": log_id},
        daemon=True,
    )
    thread.start()
    return {"status": "started", "log_id": log_id}


def _run_all_active_queries_thread() -> None:
    with run_all_lock:
        for query in list_queries_for_dashboard():
            query_id = int(query["id"])
            if int(query.get("is_active") or 0) != 1:
                continue

            if _is_query_running(query_id):
                continue

            log_id = create_scrape_log(query_id)
            _run_query_scrape_with_log(query_id=query_id, log_id=log_id)


def _start_run_all() -> bool:
    if not run_all_lock.acquire(blocking=False):
        return False
    run_all_lock.release()

    thread = threading.Thread(target=_run_all_active_queries_thread, daemon=True)
    thread.start()
    return True


def _query_listing_count(query_id: int) -> int:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM listing_query_links WHERE query_id = ?",
            (query_id,),
        ).fetchone()
    return int(row["cnt"] or 0)


def _query_next_run_at(query_id: int) -> str | None:
    job = scheduler.get_job(_job_id(query_id))
    if not job or not job.next_run_time:
        return None
    return job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")


def _build_scraper_status() -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for query in list_queries_for_dashboard():
        query_id = int(query["id"])
        last_log = get_latest_scrape_status_for_query(query_id)
        items.append(
            {
                "id": query_id,
                "name": query["name"],
                "url": query["url"],
                "is_active": int(query.get("is_active") or 0) == 1,
                "telegram_enabled": int(query.get("telegram_enabled") or 0) == 1,
                "last_run_at": query.get("last_run_at"),
                "next_run_at": _query_next_run_at(query_id),
                "interval_minutes": query.get("interval_minutes"),
                "last_status": last_log["status"] if last_log else None,
                "is_running_now": _is_query_running(query_id),
                "listing_count": _query_listing_count(query_id),
            }
        )

    return {
        "scheduler_running": bool(scheduler.running),
        "queries": items,
    }


def _bootstrap() -> None:
    init_dashboard_db()
    _ensure_telegram_settings_defaults()
    _sync_config_from_settings(_get_typed_settings())

    if not scheduler.running:
        _reload_scheduler_jobs()
        scheduler.start()


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/listings")
def listings_page() -> str:
    return render_template("index.html")


@app.get("/queries")
def queries_page() -> str:
    return render_template("index.html")


@app.get("/settings")
def settings_page() -> str:
    return render_template("index.html")


@app.get("/api/stats")
def api_stats() -> Any:
    threshold = _fmt_datetime(datetime.utcnow() - timedelta(hours=24))

    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM listings) AS total_listings,
                (SELECT COUNT(*) FROM listings WHERE is_active = 1) AS active_listings,
                (SELECT COUNT(*) FROM listings WHERE first_seen_at >= ?) AS new_last_24h,
                (SELECT COUNT(*) FROM search_queries) AS total_queries
            """,
            (threshold,),
        ).fetchone()

        latest_rows = conn.execute(
            """
            SELECT id, title, price, price_negotiable, location, first_seen_at, url
            FROM listings
            ORDER BY first_seen_at DESC
            LIMIT 20
            """
        ).fetchall()

    return jsonify(
        {
            "total_listings": int(row["total_listings"]),
            "active_listings": int(row["active_listings"]),
            "new_last_24h": int(row["new_last_24h"]),
            "total_queries": int(row["total_queries"]),
            "latest_listings": [dict(item) for item in latest_rows],
            "scraper_status": _build_scraper_status(),
        }
    )


@app.get("/api/charts")
def api_charts() -> Any:
    today = datetime.utcnow().date()
    start_day = today - timedelta(days=13)
    start_day_str = f"{start_day.isoformat()} 00:00:00"

    with _get_conn() as conn:
        daily_rows = conn.execute(
            """
            SELECT substr(first_seen_at, 1, 10) AS day, COUNT(*) AS cnt
            FROM listings
            WHERE first_seen_at >= ?
            GROUP BY day
            ORDER BY day ASC
            """,
            (start_day_str,),
        ).fetchall()

        price_row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN price IS NOT NULL AND price >= 0 AND price < 100 THEN 1 ELSE 0 END) AS b_0_100,
                SUM(CASE WHEN price IS NOT NULL AND price >= 100 AND price < 300 THEN 1 ELSE 0 END) AS b_100_300,
                SUM(CASE WHEN price IS NOT NULL AND price >= 300 AND price < 500 THEN 1 ELSE 0 END) AS b_300_500,
                SUM(CASE WHEN price IS NOT NULL AND price >= 500 AND price < 1000 THEN 1 ELSE 0 END) AS b_500_1000,
                SUM(CASE WHEN price IS NOT NULL AND price >= 1000 THEN 1 ELSE 0 END) AS b_1000_plus
            FROM listings
            """
        ).fetchone()

    day_to_count = {row["day"]: int(row["cnt"]) for row in daily_rows}
    labels: list[str] = []
    counts: list[int] = []

    for offset in range(14):
        current = start_day + timedelta(days=offset)
        current_str = current.isoformat()
        labels.append(current_str)
        counts.append(day_to_count.get(current_str, 0))

    return jsonify(
        {
            "daily_new": {
                "labels": labels,
                "data": counts,
            },
            "price_distribution": {
                "labels": ["0-100", "100-300", "300-500", "500-1000", "1000+"],
                "data": [
                    int(price_row["b_0_100"] or 0),
                    int(price_row["b_100_300"] or 0),
                    int(price_row["b_300_500"] or 0),
                    int(price_row["b_500_1000"] or 0),
                    int(price_row["b_1000_plus"] or 0),
                ],
            },
        }
    )


@app.get("/api/listings")
def api_listings() -> Any:
    query_id = request.args.get("query_id", type=int)
    only_new = _parse_bool(request.args.get("only_new"))
    priced_filter = _parse_priced_filter(request.args.get("only_priced"))
    sort = request.args.get("sort", "newest")

    page = request.args.get("page", default=1, type=int) or 1
    page = max(page, 1)

    page_size = request.args.get("page_size", default=50, type=int) or 50
    page_size = min(max(page_size, 1), 200)

    conditions: list[str] = []
    params: list[Any] = []

    if query_id is not None:
        conditions.append(
            "EXISTS (SELECT 1 FROM listing_query_links lql WHERE lql.listing_id = l.id AND lql.query_id = ?)"
        )
        params.append(query_id)

    if only_new:
        conditions.append("l.first_seen_at >= ?")
        params.append(_fmt_datetime(datetime.utcnow() - timedelta(hours=24)))

    if priced_filter == "priced":
        conditions.append("l.price IS NOT NULL")
    elif priced_filter == "unpriced":
        conditions.append("l.price IS NULL")
    elif priced_filter == "vb":
        conditions.append("l.price_negotiable = 1")

    where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    sort_mapping = {
        "newest": "l.first_seen_at DESC",
        "oldest": "l.first_seen_at ASC",
        "price_asc": "(l.price IS NULL) ASC, l.price ASC, l.first_seen_at DESC",
        "price_desc": "(l.price IS NULL) ASC, l.price DESC, l.first_seen_at DESC",
    }
    order_sql = sort_mapping.get(sort, sort_mapping["newest"])

    with _get_conn() as conn:
        total_count = int(
            conn.execute(
                f"SELECT COUNT(*) AS cnt FROM listings l {where_sql}",
                tuple(params),
            ).fetchone()["cnt"]
        )

        total_pages = max((total_count + page_size - 1) // page_size, 1)
        page = min(page, total_pages)
        offset = (page - 1) * page_size

        rows = conn.execute(
            f"""
            SELECT
                l.id,
                l.title,
                l.price,
                l.price_negotiable,
                l.location,
                l.first_seen_at,
                l.url,
                l.is_active
            FROM listings l
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            tuple(params + [page_size, offset]),
        ).fetchall()

        search_queries = conn.execute(
            "SELECT id, name, url FROM search_queries ORDER BY id ASC"
        ).fetchall()

    return jsonify(
        {
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "total_count": total_count,
            "search_queries": [dict(row) for row in search_queries],
            "listings": [dict(row) for row in rows],
        }
    )


@app.get("/api/queries")
def api_queries_get() -> Any:
    return jsonify({"queries": _build_scraper_status()["queries"]})


@app.post("/api/queries")
def api_queries_create() -> Any:
    payload = request.get_json(silent=True) or {}
    name = str(payload.get("name", "")).strip()
    url = str(payload.get("url", "")).strip()

    if not name or not url:
        return jsonify({"error": "name and url are required"}), 400

    try:
        interval_minutes = _parse_nullable_interval(payload.get("interval_minutes"))
        telegram_enabled = _bool_string(payload.get("telegram_enabled", "true")) == "true"
        item = create_query_for_dashboard(
            name=name,
            url=url,
            interval_minutes=interval_minutes,
            telegram_enabled=telegram_enabled,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    _register_or_replace_job(item, _get_typed_settings()["interval_minutes"])
    return jsonify({"query": item})


@app.put("/api/queries/<int:query_id>")
def api_queries_update(query_id: int) -> Any:
    payload = request.get_json(silent=True) or {}
    current = get_query_for_dashboard(query_id)
    if not current:
        return jsonify({"error": "query not found"}), 404

    name = str(payload.get("name", current["name"])).strip()
    url = str(payload.get("url", current["url"])).strip()
    is_active = _bool_string(payload.get("is_active", int(current.get("is_active") or 0) == 1)) == "true"
    telegram_enabled = (
        _bool_string(payload.get("telegram_enabled", int(current.get("telegram_enabled") or 0) == 1))
        == "true"
    )

    if not name or not url:
        return jsonify({"error": "name and url are required"}), 400

    try:
        if "interval_minutes" in payload:
            interval_minutes = _parse_nullable_interval(payload.get("interval_minutes"))
        else:
            interval_minutes = current.get("interval_minutes")

        updated = update_query_for_dashboard(
            query_id=query_id,
            name=name,
            url=url,
            is_active=is_active,
            interval_minutes=interval_minutes,
            telegram_enabled=telegram_enabled,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    if not updated:
        return jsonify({"error": "query not found"}), 404

    if is_active:
        _register_or_replace_job(updated, _get_typed_settings()["interval_minutes"])
        try:
            scheduler.resume_job(_job_id(query_id))
        except Exception:
            pass
    else:
        try:
            scheduler.pause_job(_job_id(query_id))
        except Exception:
            pass

    return jsonify({"query": updated})


@app.delete("/api/queries/<int:query_id>")
def api_queries_delete(query_id: int) -> Any:
    if _is_query_running(query_id):
        return jsonify({"error": "query is running"}), 409

    try:
        scheduler.remove_job(_job_id(query_id))
    except Exception:
        pass

    result = delete_query_and_listings_for_dashboard(query_id)
    return jsonify(result)


@app.post("/api/queries/<int:query_id>/toggle")
def api_queries_toggle(query_id: int) -> Any:
    updated = toggle_query_for_dashboard(query_id)
    if not updated:
        return jsonify({"error": "query not found"}), 404

    if int(updated.get("is_active") or 0) == 1:
        _register_or_replace_job(updated, _get_typed_settings()["interval_minutes"])
        try:
            scheduler.resume_job(_job_id(query_id))
        except Exception:
            pass
    else:
        try:
            scheduler.pause_job(_job_id(query_id))
        except Exception:
            pass

    return jsonify({"query": updated})


@app.post("/api/scraper/run/<int:query_id>")
def api_scraper_run_query(query_id: int) -> Any:
    result = _run_query_scrape_now(query_id=query_id, scheduled=False)
    if result["status"] == "not_found":
        return jsonify({"error": "query not found"}), 404
    if result["status"] == "already_running":
        return jsonify({"error": "query is already running"}), 409

    return jsonify({"log_id": result["log_id"], "status": "started"})


@app.post("/api/scraper/run-all")
def api_scraper_run_all() -> Any:
    if not _start_run_all():
        return jsonify({"error": "run-all already in progress"}), 409
    return jsonify({"status": "started"})


@app.get("/api/scraper/status")
def api_scraper_status() -> Any:
    return jsonify(_build_scraper_status())


@app.get("/api/scraper/log")
def api_scraper_log() -> Any:
    return jsonify({"items": list_scrape_logs(limit=50)})


@app.get("/api/scraper/log/<int:log_id>")
def api_scraper_log_item(log_id: int) -> Any:
    item = get_scrape_log(log_id)
    if not item:
        return jsonify({"error": "log not found"}), 404
    return jsonify(item)


@app.get("/api/settings")
def api_settings_get() -> Any:
    return jsonify(get_settings())


@app.put("/api/settings")
def api_settings_put() -> Any:
    payload = request.get_json(silent=True) or {}

    try:
        interval_minutes = int(payload.get("interval_minutes"))
        min_delay = float(payload.get("min_delay_seconds"))
        max_delay = float(payload.get("max_delay_seconds"))
        max_pages = int(payload.get("max_pages"))
        headless = _bool_string(payload.get("headless", "true"))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid settings payload"}), 400

    if interval_minutes < 30:
        return jsonify({"error": "interval_minutes must be >= 30"}), 400
    if min_delay < 0 or max_delay < min_delay:
        return jsonify({"error": "delay range is invalid"}), 400
    if max_pages < 1:
        return jsonify({"error": "max_pages must be >= 1"}), 400

    new_values = {
        "interval_minutes": str(interval_minutes),
        "min_delay_seconds": str(min_delay),
        "max_delay_seconds": str(max_delay),
        "max_pages": str(max_pages),
        "headless": headless,
    }
    merged = update_settings(new_values)
    _sync_config_from_settings(_get_typed_settings())
    _reload_scheduler_jobs()

    return jsonify({"settings": merged, "message": "Scheduler restarted"})


@app.get("/api/telegram/settings")
def api_telegram_settings_get() -> Any:
    settings = get_settings()
    token = str(settings.get("telegram_token", "")).strip()
    return jsonify(
        {
            "enabled": _bool_string(settings.get("telegram_enabled", "false")) == "true",
            "chat_id": str(settings.get("telegram_chat_id", "")).strip(),
            "token_set": bool(token),
            "min_price": str(settings.get("telegram_min_price", "") or ""),
            "max_price": str(settings.get("telegram_max_price", "") or ""),
        }
    )


@app.put("/api/telegram/settings")
def api_telegram_settings_put() -> Any:
    payload = request.get_json(silent=True) or {}
    current = get_settings()

    token_to_store = str(current.get("telegram_token", "")).strip()
    if "token" in payload:
        token_candidate = str(payload.get("token") or "").strip()
        if token_candidate and token_candidate != "••••••••":
            token_to_store = token_candidate

    chat_id = str(payload.get("chat_id", current.get("telegram_chat_id", ""))).strip()
    enabled = _bool_string(payload.get("enabled", current.get("telegram_enabled", "false")))

    try:
        min_price = _normalize_optional_price(
            payload.get("min_price", current.get("telegram_min_price", ""))
        )
        max_price = _normalize_optional_price(
            payload.get("max_price", current.get("telegram_max_price", ""))
        )
    except (TypeError, ValueError):
        return jsonify({"error": "min_price/max_price must be numbers >= 0"}), 400

    if min_price and max_price and float(min_price) > float(max_price):
        return jsonify({"error": "min_price must be <= max_price"}), 400

    merged = update_settings(
        {
            "telegram_token": token_to_store,
            "telegram_chat_id": chat_id,
            "telegram_enabled": enabled,
            "telegram_min_price": min_price,
            "telegram_max_price": max_price,
        }
    )

    return jsonify(
        {
            "success": True,
            "settings": {
                "enabled": _bool_string(merged.get("telegram_enabled", "false")) == "true",
                "chat_id": str(merged.get("telegram_chat_id", "")).strip(),
                "token_set": bool(str(merged.get("telegram_token", "")).strip()),
                "min_price": str(merged.get("telegram_min_price", "") or ""),
                "max_price": str(merged.get("telegram_max_price", "") or ""),
            },
        }
    )


@app.post("/api/telegram/test")
def api_telegram_test() -> Any:
    settings = get_settings()
    token = str(settings.get("telegram_token", "")).strip()
    chat_id = str(settings.get("telegram_chat_id", "")).strip()

    if not token or not chat_id:
        return jsonify({"success": False, "error": "telegram token/chat_id not configured"})

    notifier = TelegramNotifier(token=token, chat_id=chat_id)
    success = asyncio.run(notifier.test_connection())
    if success:
        return jsonify({"success": True, "error": None})
    return jsonify({"success": False, "error": "failed to send test message"})


@app.delete("/api/listings/inactive")
def api_delete_inactive_listings() -> Any:
    days = request.args.get("days", default=30, type=int) or 30
    days = max(days, 1)
    threshold = _fmt_datetime(datetime.utcnow() - timedelta(days=days))

    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id FROM listings
            WHERE is_active = 0 AND last_seen_at < ?
            """,
            (threshold,),
        ).fetchall()
        ids = [row["id"] for row in rows]

        deleted = 0
        if ids:
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"DELETE FROM listing_query_links WHERE listing_id IN ({placeholders})",
                tuple(ids),
            )
            result = conn.execute(
                f"DELETE FROM listings WHERE id IN ({placeholders})",
                tuple(ids),
            )
            deleted = int(result.rowcount or 0)
        conn.commit()

    return jsonify({"deleted": deleted, "days": days})


@app.delete("/api/listings/by-query/<int:query_id>")
def api_delete_listings_by_query(query_id: int) -> Any:
    result = delete_listings_by_query_for_dashboard(query_id)
    return jsonify(result)


@app.get("/api/listings/export")
def api_export_listings() -> Response:
    query_id = request.args.get("query_id", type=int)

    conditions = ["l.is_active = 1"]
    params: list[Any] = []
    if query_id is not None:
        conditions.append(
            "EXISTS (SELECT 1 FROM listing_query_links lql WHERE lql.listing_id = l.id AND lql.query_id = ?)"
        )
        params.append(query_id)

    where_sql = " AND ".join(conditions)

    with _get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT l.id, l.title, l.price, l.location, l.category, l.url,
                   l.first_seen_at, l.last_seen_at, l.is_active
            FROM listings l
            WHERE {where_sql}
            ORDER BY l.first_seen_at DESC
            """,
            tuple(params),
        ).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id",
        "title",
        "price",
        "location",
        "category",
        "url",
        "first_seen_at",
        "last_seen_at",
        "is_active",
    ])
    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["title"],
                row["price"],
                row["location"],
                row["category"],
                row["url"],
                row["first_seen_at"],
                row["last_seen_at"],
                row["is_active"],
            ]
        )

    csv_data = output.getvalue()
    filename = "listings_export.csv" if query_id is None else f"listings_query_{query_id}.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/data/stats")
def api_data_stats() -> Any:
    if DB_PATH.exists():
        size_mb = round(DB_PATH.stat().st_size / (1024 * 1024), 3)
    else:
        size_mb = 0.0

    with _get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM listings").fetchone()

    return jsonify({"db_size_mb": size_mb, "total_records": int(row["cnt"] or 0)})


_bootstrap()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
