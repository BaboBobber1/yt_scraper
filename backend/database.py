"""Database utilities for the Crypto YouTube Harvester backend."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DB_PATH = Path("data") / "channels.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_connection_lock = threading.Lock()
_connection: Optional[sqlite3.Connection] = None

def _get_connection() -> sqlite3.Connection:
    global _connection
    if _connection is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _connection.row_factory = sqlite3.Row
    return _connection

@contextmanager
def get_cursor():
    conn = _get_connection()
    with _connection_lock:
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        finally:
            cursor.close()

def init_db() -> None:
    with get_cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL UNIQUE,
                title TEXT,
                url TEXT NOT NULL,
                subscribers INTEGER,
                language TEXT,
                language_confidence REAL,
                emails TEXT,
                last_updated TEXT,
                created_at TEXT NOT NULL,
                last_attempted TEXT,
                needs_enrichment INTEGER NOT NULL DEFAULT 1,
                last_error TEXT
            )
            """
        )


def insert_channel(channel: Dict[str, Any]) -> bool:
    """Insert a new channel. Returns True if inserted, False if duplicate."""
    with get_cursor() as cursor:
        try:
            cursor.execute(
                """
                INSERT INTO channels (
                    channel_id, title, url, subscribers, language,
                    language_confidence, emails, last_updated, created_at,
                    last_attempted, needs_enrichment, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel["channel_id"],
                    channel.get("title"),
                    channel.get("url"),
                    channel.get("subscribers"),
                    channel.get("language"),
                    channel.get("language_confidence"),
                    channel.get("emails"),
                    channel.get("last_updated"),
                    channel.get("created_at"),
                    channel.get("last_attempted"),
                    1 if channel.get("needs_enrichment", True) else 0,
                    channel.get("last_error"),
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def bulk_insert_channels(channels: Iterable[Dict[str, Any]]) -> int:
    inserted = 0
    for channel in channels:
        if insert_channel(channel):
            inserted += 1
    return inserted


def update_channel_enrichment(
    channel_id: str,
    *,
    title: Optional[str] = None,
    subscribers: Optional[int] = None,
    language: Optional[str] = None,
    language_confidence: Optional[float] = None,
    emails: Optional[str] = None,
    last_updated: Optional[str] = None,
    last_attempted: Optional[str] = None,
    needs_enrichment: Optional[bool] = None,
    last_error: Optional[str] = None,
) -> None:
    fields: List[str] = []
    values: List[Any] = []

    def add(field: str, value: Any) -> None:
        fields.append(f"{field} = ?")
        values.append(value)

    if title is not None:
        add("title", title)
    if subscribers is not None:
        add("subscribers", subscribers)
    if language is not None:
        add("language", language)
    if language_confidence is not None:
        add("language_confidence", language_confidence)
    if emails is not None:
        add("emails", emails)
    if last_updated is not None:
        add("last_updated", last_updated)
    if last_attempted is not None:
        add("last_attempted", last_attempted)
    if needs_enrichment is not None:
        add("needs_enrichment", 1 if needs_enrichment else 0)
    if last_error is not None:
        add("last_error", last_error)

    if not fields:
        return

    values.append(channel_id)

    with get_cursor() as cursor:
        cursor.execute(
            f"UPDATE channels SET {', '.join(fields)} WHERE channel_id = ?",
            values,
        )


def get_channel_totals() -> Dict[str, int]:
    with get_cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) as total, SUM(CASE WHEN needs_enrichment = 1 THEN 1 ELSE 0 END) AS pending FROM channels"
        )
        row = cursor.fetchone()
        total = row["total"] if row and row["total"] is not None else 0
        pending = row["pending"] if row and row["pending"] is not None else 0
        return {
            "total": total,
            "pending_enrichment": pending,
        }


def get_channels(
    *,
    search: Optional[str],
    sort: str,
    order: str,
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    valid_sorts = {
        "title": "title",
        "subscribers": "subscribers",
        "language": "language",
        "last_updated": "last_updated",
        "created_at": "created_at",
    }
    sort_column = valid_sorts.get(sort, "created_at")
    order_direction = "DESC" if order.lower() == "desc" else "ASC"

    params: List[Any] = []
    where_clause = ""
    if search:
        where_clause = "WHERE title LIKE ? OR url LIKE ? OR emails LIKE ?"
        term = f"%{search}%"
        params.extend([term, term, term])

    query = f"SELECT * FROM channels {where_clause} ORDER BY {sort_column} {order_direction} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    with get_cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

        cursor.execute(
            f"SELECT COUNT(*) FROM channels {where_clause}",
            params[:-2] if search else [],
        )
        total = cursor.fetchone()[0]

    items = [dict(row) for row in rows]
    return items, total


def get_pending_channels(limit: Optional[int]) -> List[Dict[str, Any]]:
    limit_clause = "LIMIT ?" if limit is not None else ""
    params: Tuple[Any, ...] = (limit,) if limit is not None else tuple()
    query = (
        "SELECT * FROM channels WHERE needs_enrichment = 1 ORDER BY last_attempted IS NULL DESC, last_attempted ASC "
        + limit_clause
    )
    with get_cursor() as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def ensure_channel_url(channel_id: str, url: Optional[str]) -> str:
    if url:
        return url
    return f"https://www.youtube.com/channel/{channel_id}"
