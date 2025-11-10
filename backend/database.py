"""Database utilities for the Crypto YouTube Harvester backend."""
from __future__ import annotations

import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
                archived_at TEXT,
                blacklisted INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        _ensure_column(cursor, "channels", "status", "TEXT NOT NULL DEFAULT 'new'")
        _ensure_column(cursor, "channels", "status_reason", "TEXT")
        _ensure_column(cursor, "channels", "last_status_change", "TEXT")
        _ensure_column(cursor, "channels", "archived", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(cursor, "channels", "archived_at", "TEXT")
        _ensure_column(cursor, "channels", "blacklisted", "INTEGER NOT NULL DEFAULT 0")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS blacklist (
                channel_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS emails_unique (
                email TEXT PRIMARY KEY,
                first_seen_channel_id TEXT,
                last_seen_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channel_emails (
                channel_id TEXT NOT NULL,
                email TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                UNIQUE(channel_id, email),
                FOREIGN KEY(email) REFERENCES emails_unique(email)
            )
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_channel_emails_channel_id ON channel_emails(channel_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_channel_emails_email ON channel_emails(email)"
        )
        # Legacy field cleanup: drop needs_enrichment if present but unused by new status model.
        # We keep the column to avoid destructive migrations but ensure defaults align.


@dataclass(frozen=True)
class ChannelFilters:
    query_text: Optional[str] = None
    languages: Optional[Sequence[str]] = None
    statuses: Optional[Sequence[str]] = None
    min_subscribers: Optional[int] = None
    max_subscribers: Optional[int] = None
    emails_only: bool = False
    include_archived: bool = False


EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def _normalize_email(value: str) -> Optional[str]:
    candidate = value.strip().lower()
    if not candidate or "@" not in candidate:
        return None
    match = EMAIL_PATTERN.fullmatch(candidate)
    return candidate if match else None


def parse_email_candidates(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return EMAIL_PATTERN.findall(value)


def is_blacklisted(channel_id: str) -> bool:
    with get_cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM blacklist WHERE channel_id = ?",
            (channel_id,),
        )
        if cursor.fetchone():
            return True
        cursor.execute(
            "SELECT 1 FROM channels WHERE channel_id = ? AND blacklisted = 1",
            (channel_id,),
        )
        return cursor.fetchone() is not None


def upsert_blacklist_channel(channel_id: str, timestamp: str) -> Tuple[bool, bool]:
    created = False
    updated = False
    url = ensure_channel_url(channel_id, None)

    with get_cursor() as cursor:
        cursor.execute(
            "SELECT channel_id FROM blacklist WHERE channel_id = ?",
            (channel_id,),
        )
        row = cursor.fetchone()
        if row:
            updated = True
            cursor.execute(
                "UPDATE blacklist SET updated_at = ? WHERE channel_id = ?",
                (timestamp, channel_id),
            )
        else:
            created = True
            cursor.execute(
                "INSERT INTO blacklist (channel_id, created_at, updated_at) VALUES (?, ?, ?)",
                (channel_id, timestamp, timestamp),
            )

        cursor.execute(
            "SELECT channel_id FROM channels WHERE channel_id = ?",
            (channel_id,),
        )
        channel_exists = cursor.fetchone() is not None
        if channel_exists:
            cursor.execute(
                """
                UPDATE channels
                SET archived = 1,
                    archived_at = COALESCE(archived_at, ?),
                    needs_enrichment = 0,
                    blacklisted = 1,
                    status = 'blacklisted',
                    status_reason = 'Blacklisted',
                    last_status_change = COALESCE(last_status_change, ?)
                WHERE channel_id = ?
                """,
                (timestamp, timestamp, channel_id),
            )
        else:
            cursor.execute(
                """
                INSERT INTO channels (
                    channel_id, title, url, subscribers, language,
                    language_confidence, emails, last_updated, created_at,
                    last_attempted, needs_enrichment, last_error,
                    status, status_reason, last_status_change,
                    archived, archived_at, blacklisted
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel_id,
                    None,
                    url,
                    None,
                    None,
                    None,
                    None,
                    None,
                    timestamp,
                    None,
                    0,
                    None,
                    'blacklisted',
                    'Blacklisted',
                    timestamp,
                    1,
                    timestamp,
                    1,
                ),
            )

    return updated, created


def _ensure_column(cursor: sqlite3.Cursor, table: str, column: str, definition: str) -> None:
    cursor.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    if column not in existing:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def insert_channel(channel: Dict[str, Any]) -> bool:
    """Insert a new channel. Returns True if inserted, False if duplicate."""
    channel_id = channel["channel_id"]
    if is_blacklisted(channel_id):
        return False
    with get_cursor() as cursor:
        try:
            cursor.execute(
                """
                INSERT INTO channels (
                    channel_id, title, url, subscribers, language,
                    language_confidence, emails, last_updated, created_at,
                    last_attempted, needs_enrichment, last_error,
                    status, status_reason, last_status_change,
                    archived, archived_at, blacklisted
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    channel_id,
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
                    1 if channel.get("blacklisted") else 0,
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


def record_channel_emails(channel_id: str, emails: Iterable[str], timestamp: str) -> Set[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for email in emails:
        if not email:
            continue
        normalized_email = _normalize_email(email)
        if not normalized_email or normalized_email in seen:
            continue
        seen.add(normalized_email)
        normalized.append(normalized_email)

    if not normalized:
        return set()

    with get_cursor() as cursor:
        for email in normalized:
            cursor.execute(
                """
                INSERT INTO emails_unique (email, first_seen_channel_id, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(email) DO UPDATE SET
                    last_seen_at = excluded.last_seen_at,
                    first_seen_channel_id = COALESCE(first_seen_channel_id, excluded.first_seen_channel_id)
                """,
                (email, channel_id, timestamp),
            )
            cursor.execute(
                """
                INSERT INTO channel_emails (channel_id, email, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(channel_id, email) DO UPDATE SET last_seen_at = excluded.last_seen_at
                """,
                (channel_id, email, timestamp),
            )

    return set(normalized)


def get_channel_email_set(channel_id: str) -> Set[str]:
    with get_cursor() as cursor:
        cursor.execute(
            "SELECT email FROM channel_emails WHERE channel_id = ?",
            (channel_id,),
        )
        return {row[0] for row in cursor.fetchall()}


def has_all_known_emails(emails: Iterable[str]) -> bool:
    normalized: Set[str] = set()
    for email in emails:
        if not email:
            continue
        normalized_email = _normalize_email(email)
        if normalized_email:
            normalized.add(normalized_email)
    if not normalized:
        return False
    params = list(normalized)
    placeholders = ",".join("?" for _ in params)
    with get_cursor() as cursor:
        cursor.execute(
            f"SELECT email FROM emails_unique WHERE email IN ({placeholders})",
            params,
        )
        rows = {row[0] for row in cursor.fetchall()}
    return rows == set(params)


def get_unique_email_rows(filters: ChannelFilters) -> List[Dict[str, Any]]:
    where_clause, params = _build_channel_filters(filters, table_alias="c")
    query = (
        """
        SELECT
            ce.email,
            c.channel_id,
            c.title,
            c.url,
            c.last_updated,
            c.created_at,
            eu.first_seen_channel_id,
            eu.last_seen_at
        FROM channel_emails ce
        JOIN channels c ON c.channel_id = ce.channel_id
        LEFT JOIN emails_unique eu ON eu.email = ce.email
        {where_clause}
        ORDER BY eu.last_seen_at DESC, c.last_updated DESC, c.created_at DESC
        """.format(where_clause=where_clause)
    )

    with get_cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()

    grouped: Dict[str, List[sqlite3.Row]] = {}
    for row in rows:
        email = row["email"]
        grouped.setdefault(email, []).append(row)

    unique_rows: List[Dict[str, Any]] = []
    for email, candidates in grouped.items():
        first_seen_channel = candidates[0]["first_seen_channel_id"] if candidates else None
        primary_row = None
        if first_seen_channel:
            for row in candidates:
                if row["channel_id"] == first_seen_channel:
                    primary_row = row
                    break
        if primary_row is None and candidates:
            primary_row = max(
                candidates,
                key=lambda row: (
                    row["last_seen_at"] or "",
                    row["last_updated"] or "",
                    row["created_at"] or "",
                ),
            )
        if primary_row is None:
            continue
        other_count = max(0, len(candidates) - 1)
        unique_rows.append(
            {
                "email": email,
                "primary_channel_id": primary_row["channel_id"],
                "primary_channel_name": primary_row["title"] or "",
                "primary_channel_url": ensure_channel_url(
                    primary_row["channel_id"], primary_row["url"]
                ),
                "other_channels_count": other_count,
                "last_updated": primary_row["last_seen_at"]
                or primary_row["last_updated"]
                or primary_row["created_at"],
            }
        )

    unique_rows.sort(key=lambda row: (row["last_updated"] or ""), reverse=True)
    return unique_rows


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
            WHERE blacklisted IS NULL OR blacklisted = 0
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
    filters: ChannelFilters,
    *,
    table_alias: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    clauses: List[str] = []
    params: List[Any] = []
    prefix = f"{table_alias}." if table_alias else ""

    if filters.query_text:
        clauses.append(f"({prefix}title LIKE ? OR {prefix}url LIKE ? OR {prefix}emails LIKE ?)")
        term = f"%{filters.query_text}%"
        params.extend([term, term, term])

    if filters.languages:
        placeholders = ",".join("?" for _ in filters.languages)
        clauses.append(f"{prefix}language IN ({placeholders})")
        params.extend(filters.languages)

    if filters.statuses:
        placeholders = ",".join("?" for _ in filters.statuses)
        clauses.append(f"{prefix}status IN ({placeholders})")
        params.extend(filters.statuses)

    if filters.min_subscribers is not None:
        clauses.append(f"({prefix}subscribers IS NOT NULL AND {prefix}subscribers >= ?)")
        params.append(filters.min_subscribers)

    if filters.max_subscribers is not None:
        clauses.append(f"({prefix}subscribers IS NOT NULL AND {prefix}subscribers <= ?)")
        params.append(filters.max_subscribers)

    if filters.emails_only:
        clauses.append(f"({prefix}emails IS NOT NULL AND TRIM({prefix}emails) != '')")

    if not filters.include_archived:
        clauses.append(f"({prefix}archived IS NULL OR {prefix}archived = 0)")

    clauses.append(f"({prefix}blacklisted IS NULL OR {prefix}blacklisted = 0)")

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_clause, params


def get_channels(
    filters: ChannelFilters,
    *,
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
        "status": "status",
        "last_status_change": "last_status_change",
    }
    sort_column = valid_sorts.get(sort, "created_at")
    order_direction = "DESC" if order.lower() == "desc" else "ASC"

    where_clause, params = _build_channel_filters(filters)

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
        "AND (blacklisted IS NULL OR blacklisted = 0) "
        "ORDER BY last_attempted IS NULL DESC, last_attempted ASC "
        + limit_clause
    )
    with get_cursor() as cursor:
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_channels_for_email_enrichment(limit: Optional[int]) -> List[Dict[str, Any]]:
    limit_clause = "LIMIT ?" if limit is not None else ""
    params: Tuple[Any, ...] = (limit,) if limit is not None else tuple()
    query = (
        "SELECT * FROM channels WHERE (archived IS NULL OR archived = 0) "
        "AND (blacklisted IS NULL OR blacklisted = 0) "
        "ORDER BY last_updated IS NULL DESC, last_updated ASC "
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
            "AND (archived IS NULL OR archived = 0) "
            "AND (blacklisted IS NULL OR blacklisted = 0)",
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


def archive_or_create_channel(channel_id: str, timestamp: str) -> Tuple[bool, bool]:
    """Mark a channel as blacklisted, creating a placeholder if required."""

    return upsert_blacklist_channel(channel_id, timestamp)


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
