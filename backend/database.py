"""Database utilities for the Crypto YouTube Harvester backend."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
                last_error TEXT,
                status TEXT NOT NULL DEFAULT 'new',
                status_reason TEXT,
                last_status_change TEXT,
                archived INTEGER NOT NULL DEFAULT 0,
                archived_at TEXT
            )
            """
        )

        _ensure_column(cursor, "channels", "status", "TEXT NOT NULL DEFAULT 'new'")
        _ensure_column(cursor, "channels", "status_reason", "TEXT")
        _ensure_column(cursor, "channels", "last_status_change", "TEXT")
        _ensure_column(cursor, "channels", "archived", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(cursor, "channels", "archived_at", "TEXT")
        # Legacy field cleanup: drop needs_enrichment if present but unused by new status model.
        # We keep the column to avoid destructive migrations but ensure defaults align.


def _ensure_column(cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
    cursor.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def insert_channel(channel: Dict[str, Any]) -> bool:
    """Insert a new channel. Returns True if inserted, False if duplicate."""
    with get_cursor() as cursor:
        try:
            cursor.execute(
                """
                INSERT INTO channels (
                    channel_id, title, url, subscribers, language,
                    language_confidence, emails, last_updated, created_at,
                    last_attempted, needs_enrichment, last_error,
                    status, status_reason, last_status_change,
                    archived, archived_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    channel.get("status", "new"),
                    channel.get("status_reason"),
                    channel.get("last_status_change"),
                    1 if channel.get("archived") else 0,
                    channel.get("archived_at"),
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
    status: Optional[str] = None,
    status_reason: Optional[str] = None,
    last_status_change: Optional[str] = None,
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
    if status is not None:
        add("status", status)
    if status_reason is not None:
        add("status_reason", status_reason)
    if last_status_change is not None:
        add("last_status_change", last_status_change)

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
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_count,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing_count,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count
            FROM channels
            """
        )
        row = cursor.fetchone()
        total = row["total"] if row and row["total"] is not None else 0
        return {
            "total": total,
            "new": row["new_count"] if row and row["new_count"] is not None else 0,
            "processing": row["processing_count"] if row and row["processing_count"] is not None else 0,
            "completed": row["completed_count"] if row and row["completed_count"] is not None else 0,
            "error": row["error_count"] if row and row["error_count"] is not None else 0,
        }


def _build_channel_filters(
    *,
    query_text: Optional[str],
    languages: Optional[Sequence[str]],
    statuses: Optional[Sequence[str]],
    min_subscribers: Optional[int],
    max_subscribers: Optional[int],
    emails_only: bool,
    include_archived: bool,
) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []

    if query_text:
        clauses.append("(title LIKE ? OR url LIKE ? OR emails LIKE ?)")
        term = f"%{query_text}%"
        params.extend([term, term, term])

    if languages:
        placeholders = ",".join("?" for _ in languages)
        clauses.append(f"language IN ({placeholders})")
        params.extend(languages)

    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses)

    if min_subscribers is not None:
        clauses.append("(subscribers IS NOT NULL AND subscribers >= ?)")
        params.append(min_subscribers)

    if max_subscribers is not None:
        clauses.append("(subscribers IS NOT NULL AND subscribers <= ?)")
        params.append(max_subscribers)

    if emails_only:
        clauses.append("(emails IS NOT NULL AND TRIM(emails) != '')")

    if not include_archived:
        clauses.append("(archived IS NULL OR archived = 0)")

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_clause, params


def get_channels(
    *,
    query_text: Optional[str],
    languages: Optional[Sequence[str]],
    statuses: Optional[Sequence[str]],
    min_subscribers: Optional[int],
    max_subscribers: Optional[int],
    sort: str,
    order: str,
    limit: int,
    offset: int,
    emails_only: bool,
    include_archived: bool,
) -> Tuple[List[Dict[str, Any]], int]:
    valid_sorts = {
        "title": "title",
        "subscribers": "subscribers",
        "language": "language",
        "last_updated": "last_updated",
        "created_at": "created_at",
        "status": "status",
        "last_status_change": "last_status_change",
    }
    sort_column = valid_sorts.get(sort, "created_at")
    order_direction = "DESC" if order.lower() == "desc" else "ASC"

    where_clause, params = _build_channel_filters(
        query_text=query_text,
        languages=languages,
        statuses=statuses,
        min_subscribers=min_subscribers,
        max_subscribers=max_subscribers,
        emails_only=emails_only,
        include_archived=include_archived,
    )

    query = (
        f"SELECT * FROM channels {where_clause} "
        f"ORDER BY {sort_column} {order_direction} LIMIT ? OFFSET ?"
    )
    params.extend([limit, offset])

    with get_cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

        cursor.execute(
            f"SELECT COUNT(*) FROM channels {where_clause}",
            params[:-2],
        )
        total_row = cursor.fetchone()
        total = total_row[0] if total_row else 0

    items: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["archived"] = bool(item.get("archived"))
        items.append(item)
    return items, total


def get_pending_channels(limit: Optional[int]) -> List[Dict[str, Any]]:
    limit_clause = "LIMIT ?" if limit is not None else ""
    params: Tuple[Any, ...] = (limit,) if limit is not None else tuple()
    query = (
        "SELECT * FROM channels WHERE status IN ('new', 'error') "
        "AND (archived IS NULL OR archived = 0) "
        "ORDER BY last_attempted IS NULL DESC, last_attempted ASC "
        + limit_clause
    )
    with get_cursor() as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def archive_channels_by_ids(channel_ids: Sequence[str], timestamp: str) -> List[str]:
    if not channel_ids:
        return []

    placeholders = ",".join("?" for _ in channel_ids)
    params: List[Any] = list(channel_ids)

    with get_cursor() as cursor:
        cursor.execute(
            f"SELECT channel_id FROM channels WHERE channel_id IN ({placeholders}) "
            "AND (archived IS NULL OR archived = 0)",
            params,
        )
        targets = [row[0] for row in cursor.fetchall()]
        if not targets:
            return []

        target_placeholders = ",".join("?" for _ in targets)
        cursor.execute(
            f"UPDATE channels SET archived = 1, archived_at = ?, needs_enrichment = 0 "
            f"WHERE channel_id IN ({target_placeholders})",
            [timestamp, *targets],
        )
        return targets


def set_channel_status(channel_id: str, status: str, *, reason: Optional[str], timestamp: Optional[str]) -> None:
    last_error_value = reason
    if reason is None and status in {"new", "processing"}:
        last_error_value = ""
    update_channel_enrichment(
        channel_id,
        status=status,
        status_reason=reason,
        last_status_change=timestamp,
        needs_enrichment=None,
        last_error=last_error_value,
    )


def ensure_channel_url(channel_id: str, url: Optional[str]) -> str:
    if url:
        return url
    return f"https://www.youtube.com/channel/{channel_id}"
