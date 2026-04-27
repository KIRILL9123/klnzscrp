from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

from sqlalchemy import Boolean, DateTime, Integer, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "kleinanzeigen.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, echo=False, future=True)


class Base(DeclarativeBase):
    pass


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(Text)
    price: Mapped[int | None] = mapped_column(Integer, nullable=True)
    price_negotiable: Mapped[bool] = mapped_column(Boolean, default=False)
    location: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class SearchQuery(Base):
    __tablename__ = "search_queries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String)
    url: Mapped[str] = mapped_column(Text, unique=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    interval_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)


class ListingQueryLink(Base):
    __tablename__ = "listing_query_links"

    query_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    listing_id: Mapped[str] = mapped_column(String, primary_key=True)


class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[str] = mapped_column(Text)


class ScrapeLog(Base):
    __tablename__ = "scrape_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query_id: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(Text)
    new_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, default=0)
    deactivated_count: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


def init_db() -> None:
    """Create tables if they do not exist."""
    Base.metadata.create_all(engine)


def upsert_listing(listing_dict: dict, query_id: int | None = None) -> str:
    """Insert a new listing or refresh an existing listing.

    Returns:
        "created" if inserted, otherwise "updated".
    """
    now = datetime.utcnow()

    with Session(engine) as session:
        existing = session.get(Listing, listing_dict["id"])
        if existing:
            existing.title = listing_dict.get("title", existing.title)
            existing.price = listing_dict.get("price", existing.price)
            existing.price_negotiable = listing_dict.get(
                "price_negotiable", existing.price_negotiable
            )
            existing.location = listing_dict.get("location", existing.location)
            existing.category = listing_dict.get("category", existing.category)
            existing.url = listing_dict.get("url", existing.url)
            if listing_dict.get("description"):
                existing.description = listing_dict["description"]
            existing.last_seen_at = now
            existing.is_active = True
            if query_id is not None:
                _ensure_listing_query_link(session=session, query_id=query_id, listing_id=existing.id)
            session.commit()
            return "updated"

        listing = Listing(
            id=listing_dict["id"],
            title=listing_dict.get("title", ""),
            price=listing_dict.get("price"),
            price_negotiable=listing_dict.get("price_negotiable", False),
            location=listing_dict.get("location", ""),
            category=listing_dict.get("category", ""),
            url=listing_dict.get("url", ""),
            description=listing_dict.get("description"),
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
        )
        session.add(listing)
        if query_id is not None:
            _ensure_listing_query_link(session=session, query_id=query_id, listing_id=listing.id)
        session.commit()
        return "created"


def _ensure_listing_query_link(session: Session, query_id: int, listing_id: str) -> None:
    """Create relation between search query and listing if missing."""
    stmt = select(ListingQueryLink).where(
        ListingQueryLink.query_id == query_id,
        ListingQueryLink.listing_id == listing_id,
    )
    existing = session.scalar(stmt)
    if existing:
        return
    session.add(ListingQueryLink(query_id=query_id, listing_id=listing_id))


def get_new_listings(since: datetime) -> list[Listing]:
    """Return listings first seen at or after the provided timestamp."""
    with Session(engine) as session:
        stmt = select(Listing).where(Listing.first_seen_at >= since)
        return list(session.scalars(stmt))


def mark_inactive(ids: Iterable[str]) -> int:
    """Mark listings as inactive when they are missing from the latest scrape."""
    ids = list(ids)
    if not ids:
        return 0

    with Session(engine) as session:
        stmt = select(Listing).where(Listing.id.in_(ids), Listing.is_active.is_(True))

        items = list(session.scalars(stmt))
        for listing in items:
            listing.is_active = False

        session.commit()
        return len(items)


def get_listing_ids_for_query(query_id: int) -> list[str]:
    """Return listing IDs linked to a specific search query."""
    with Session(engine) as session:
        stmt = select(ListingQueryLink.listing_id).where(ListingQueryLink.query_id == query_id)
        return list(session.scalars(stmt))


def load_search_queries() -> list[SearchQuery]:
    """Load all search queries from the database."""
    with Session(engine) as session:
        stmt = select(SearchQuery).order_by(SearchQuery.id.asc())
        return list(session.scalars(stmt))


def update_last_run(query_id: int) -> None:
    """Update last run timestamp for the selected search query."""
    with Session(engine) as session:
        item = session.get(SearchQuery, query_id)
        if not item:
            return
        item.last_run_at = datetime.utcnow()
        session.commit()


def ensure_search_query(name: str, url: str) -> SearchQuery:
    """Insert a query if it is missing and return the database row."""
    with Session(engine) as session:
        stmt = select(SearchQuery).where(SearchQuery.url == url)
        existing = session.scalar(stmt)
        if existing:
            if existing.name != name:
                existing.name = name
                session.commit()
            return existing

        query = SearchQuery(name=name, url=url, last_run_at=None)
        session.add(query)
        session.commit()
        session.refresh(query)
        return query


SETTINGS_DEFAULTS = {
    "interval_minutes": "120",
    "min_delay_seconds": "2",
    "max_delay_seconds": "6",
    "max_pages": "5",
    "headless": "true",
}


def _get_sqlite_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_search_query_is_active_column(conn: sqlite3.Connection) -> None:
    columns = conn.execute("PRAGMA table_info(search_queries)").fetchall()
    col_names = {row["name"] for row in columns}
    if "is_active" in col_names:
        return
    conn.execute(
        "ALTER TABLE search_queries ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1"
    )


def _ensure_search_query_interval_column(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE search_queries ADD COLUMN interval_minutes INTEGER")
    except sqlite3.OperationalError:
        # Column already exists in upgraded environments.
        pass


def _read_config_defaults() -> dict[str, str]:
    cfg_path = BASE_DIR / "config.yaml"
    if not cfg_path.exists():
        return SETTINGS_DEFAULTS.copy()

    with cfg_path.open("r", encoding="utf-8") as file_obj:
        cfg = yaml.safe_load(file_obj) or {}

    scheduler_cfg = cfg.get("scheduler", {}) if isinstance(cfg, dict) else {}
    scraper_cfg = cfg.get("scraper", {}) if isinstance(cfg, dict) else {}

    defaults = SETTINGS_DEFAULTS.copy()
    defaults["interval_minutes"] = str(scheduler_cfg.get("interval_minutes", defaults["interval_minutes"]))
    defaults["min_delay_seconds"] = str(scraper_cfg.get("min_delay_seconds", defaults["min_delay_seconds"]))
    defaults["max_delay_seconds"] = str(scraper_cfg.get("max_delay_seconds", defaults["max_delay_seconds"]))
    defaults["max_pages"] = str(scraper_cfg.get("max_pages", defaults["max_pages"]))
    defaults["headless"] = str(scraper_cfg.get("headless", defaults["headless"]))
    return defaults


def init_dashboard_db() -> None:
    """Initialize schema for dashboard runtime, migrations, defaults, and WAL mode."""
    init_db()

    with _get_sqlite_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        _ensure_search_query_is_active_column(conn)
        _ensure_search_query_interval_column(conn)

        # If settings are empty, migrate default values from config.yaml once.
        row = conn.execute("SELECT COUNT(*) AS cnt FROM settings").fetchone()
        if int(row["cnt"] or 0) == 0:
            defaults = _read_config_defaults()
            for key, value in defaults.items():
                conn.execute(
                    "INSERT INTO settings(key, value) VALUES (?, ?)",
                    (key, str(value)),
                )

        conn.commit()


def get_settings() -> dict[str, str]:
    with _get_sqlite_conn() as conn:
        rows = conn.execute(
            "SELECT key, value FROM settings ORDER BY key ASC"
        ).fetchall()
    return {row["key"]: row["value"] for row in rows}


def update_settings(new_values: dict[str, str]) -> dict[str, str]:
    with _get_sqlite_conn() as conn:
        for key, value in new_values.items():
            conn.execute(
                """
                INSERT INTO settings(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(value)),
            )
        conn.commit()
    return get_settings()


def list_queries_for_dashboard() -> list[dict]:
    with _get_sqlite_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, name, url, last_run_at, is_active, interval_minutes
            FROM search_queries
            ORDER BY id ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def get_query_for_dashboard(query_id: int) -> dict | None:
    with _get_sqlite_conn() as conn:
        row = conn.execute(
            "SELECT id, name, url, last_run_at, is_active, interval_minutes FROM search_queries WHERE id = ?",
            (query_id,),
        ).fetchone()
    return dict(row) if row else None


def create_query_for_dashboard(name: str, url: str, interval_minutes: int | None = None) -> dict:
    with _get_sqlite_conn() as conn:
        cur = conn.execute(
            "INSERT INTO search_queries(name, url, last_run_at, is_active, interval_minutes) VALUES (?, ?, NULL, 1, ?)",
            (name, url, interval_minutes),
        )
        conn.commit()
        query_id = int(cur.lastrowid)
    item = get_query_for_dashboard(query_id)
    if item is None:
        raise RuntimeError("Failed to create query")
    return item


def update_query_for_dashboard(
    query_id: int,
    name: str,
    url: str,
    is_active: bool,
    interval_minutes: int | None,
) -> dict | None:
    with _get_sqlite_conn() as conn:
        conn.execute(
            """
            UPDATE search_queries
            SET name = ?, url = ?, is_active = ?, interval_minutes = ?
            WHERE id = ?
            """,
            (name, url, 1 if is_active else 0, interval_minutes, query_id),
        )
        conn.commit()
    return get_query_for_dashboard(query_id)


def toggle_query_for_dashboard(query_id: int) -> dict | None:
    with _get_sqlite_conn() as conn:
        row = conn.execute(
            "SELECT is_active FROM search_queries WHERE id = ?",
            (query_id,),
        ).fetchone()
        if not row:
            return None

        new_value = 0 if int(row["is_active"] or 0) == 1 else 1
        conn.execute(
            "UPDATE search_queries SET is_active = ? WHERE id = ?",
            (new_value, query_id),
        )
        conn.commit()
    return get_query_for_dashboard(query_id)


def delete_query_and_listings_for_dashboard(query_id: int) -> dict[str, int]:
    with _get_sqlite_conn() as conn:
        listing_rows = conn.execute(
            "SELECT listing_id FROM listing_query_links WHERE query_id = ?",
            (query_id,),
        ).fetchall()
        listing_ids = [row["listing_id"] for row in listing_rows]

        deleted_listings = 0
        if listing_ids:
            placeholders = ",".join("?" for _ in listing_ids)
            conn.execute(
                f"DELETE FROM listing_query_links WHERE listing_id IN ({placeholders})",
                tuple(listing_ids),
            )
            result = conn.execute(
                f"DELETE FROM listings WHERE id IN ({placeholders})",
                tuple(listing_ids),
            )
            deleted_listings = int(result.rowcount or 0)

        conn.execute("DELETE FROM scrape_log WHERE query_id = ?", (query_id,))
        conn.execute("DELETE FROM search_queries WHERE id = ?", (query_id,))
        conn.commit()

    return {"deleted_listings": deleted_listings}


def delete_listings_by_query_for_dashboard(query_id: int) -> dict[str, int]:
    with _get_sqlite_conn() as conn:
        listing_rows = conn.execute(
            "SELECT listing_id FROM listing_query_links WHERE query_id = ?",
            (query_id,),
        ).fetchall()
        listing_ids = [row["listing_id"] for row in listing_rows]

        deleted_listings = 0
        if listing_ids:
            placeholders = ",".join("?" for _ in listing_ids)
            conn.execute(
                f"DELETE FROM listing_query_links WHERE listing_id IN ({placeholders})",
                tuple(listing_ids),
            )
            result = conn.execute(
                f"DELETE FROM listings WHERE id IN ({placeholders})",
                tuple(listing_ids),
            )
            deleted_listings = int(result.rowcount or 0)
        conn.commit()

    return {"deleted_listings": deleted_listings}


def create_scrape_log(query_id: int) -> int:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _get_sqlite_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO scrape_log(
                query_id, started_at, finished_at, status,
                new_count, updated_count, deactivated_count, error_message
            ) VALUES (?, ?, NULL, 'running', 0, 0, 0, NULL)
            """,
            (query_id, now),
        )
        conn.commit()
        return int(cur.lastrowid)


def finish_scrape_log(
    log_id: int,
    status: str,
    new_count: int = 0,
    updated_count: int = 0,
    deactivated_count: int = 0,
    error_message: str | None = None,
) -> None:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with _get_sqlite_conn() as conn:
        conn.execute(
            """
            UPDATE scrape_log
            SET finished_at = ?, status = ?, new_count = ?, updated_count = ?,
                deactivated_count = ?, error_message = ?
            WHERE id = ?
            """,
            (now, status, new_count, updated_count, deactivated_count, error_message, log_id),
        )
        conn.commit()


def list_scrape_logs(limit: int = 50) -> list[dict]:
    with _get_sqlite_conn() as conn:
        rows = conn.execute(
            """
            SELECT l.id, l.query_id, q.name AS query_name, l.started_at, l.finished_at,
                   l.status, l.new_count, l.updated_count, l.deactivated_count, l.error_message
            FROM scrape_log l
            LEFT JOIN search_queries q ON q.id = l.query_id
            ORDER BY l.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_scrape_log(log_id: int) -> dict | None:
    with _get_sqlite_conn() as conn:
        row = conn.execute(
            """
            SELECT l.id, l.query_id, q.name AS query_name, l.started_at, l.finished_at,
                   l.status, l.new_count, l.updated_count, l.deactivated_count, l.error_message
            FROM scrape_log l
            LEFT JOIN search_queries q ON q.id = l.query_id
            WHERE l.id = ?
            """,
            (log_id,),
        ).fetchone()
    return dict(row) if row else None


def get_latest_scrape_status_for_query(query_id: int) -> dict | None:
    with _get_sqlite_conn() as conn:
        row = conn.execute(
            """
            SELECT id, status, started_at, finished_at, new_count, updated_count,
                   deactivated_count, error_message
            FROM scrape_log
            WHERE query_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (query_id,),
        ).fetchone()
    return dict(row) if row else None
