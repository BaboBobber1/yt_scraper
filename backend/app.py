"""FastAPI application powering the Crypto YouTube Harvester."""
from __future__ import annotations

import csv
import datetime as dt
import io
import re
from typing import Any, Dict, List, Optional, Set

from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse

from . import database
from .database import ensure_channel_url
from .enrichment import manager
from .youtube import search_channels

app = FastAPI(title="Crypto YouTube Harvester")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

database.init_db()

DEFAULT_KEYWORDS = [
    "crypto",
    "bitcoin",
    "ethereum",
    "defi",
    "altcoin",
    "memecoin",
    "onchain",
    "crypto trading",
]


def _parse_multi(values: Optional[List[str]]) -> Optional[List[str]]:
    if not values:
        return None
    cleaned = [value.strip() for value in values if value and value.strip()]
    return cleaned or None


CHANNEL_ID_PATTERN = re.compile(r"(UC[\w-]{22})", re.IGNORECASE)


def _extract_channel_id(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    cleaned = cleaned.split("?")[0].rstrip("/")
    match = CHANNEL_ID_PATTERN.search(cleaned)
    if match:
        return match.group(1).upper()
    if cleaned.upper().startswith("UC") and len(cleaned) == 24 and CHANNEL_ID_PATTERN.match(cleaned):
        return cleaned.upper()
    return None


def _parse_int(value: Optional[str], *, field: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"{field} must be an integer")


def _collect_filters(
    *,
    q: Optional[str],
    languages: Optional[List[str]],
    statuses: Optional[List[str]],
    min_subscribers: Optional[str],
    max_subscribers: Optional[str],
    emails_only: bool,
    include_archived: bool,
) -> Dict[str, Any]:
    language_values = [value.lower() for value in _parse_multi(languages) or []] or None
    status_values = [value.lower() for value in _parse_multi(statuses) or []] or None
    if status_values:
        allowed = {"new", "processing", "completed", "error"}
        invalid = [value for value in status_values if value not in allowed]
        if invalid:
            raise HTTPException(status_code=400, detail=f"Invalid status values: {', '.join(invalid)}")

    min_subs_int = _parse_int(min_subscribers, field="min_subscribers")
    max_subs_int = _parse_int(max_subscribers, field="max_subscribers")
    if (
        min_subs_int is not None
        and max_subs_int is not None
        and min_subs_int > max_subs_int
    ):
        raise HTTPException(status_code=400, detail="min_subscribers cannot exceed max_subscribers")

    return {
        "query_text": q.strip() if q else None,
        "languages": language_values,
        "statuses": status_values,
        "min_subscribers": min_subs_int,
        "max_subscribers": max_subs_int,
        "emails_only": emails_only,
        "include_archived": include_archived,
    }


@app.get("/")
def serve_index() -> FileResponse:
    return FileResponse("frontend/index.html")


@app.get("/static/{path:path}")
def serve_static(path: str) -> FileResponse:
    return FileResponse(f"frontend/{path}")


@app.post("/api/discover")
def api_discover(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    keywords = payload.get("keywords", DEFAULT_KEYWORDS)
    per_keyword = int(payload.get("perKeyword", 5))
    if not isinstance(keywords, list) or per_keyword <= 0:
        raise HTTPException(status_code=400, detail="Invalid payload")

    now = dt.datetime.utcnow().isoformat()
    new_channels: List[Dict[str, Any]] = []

    for keyword in keywords:
        if not isinstance(keyword, str):
            keyword = str(keyword)
        keyword = keyword.strip()
        if not keyword:
            continue
        try:
            results = search_channels(keyword, per_keyword)
        except Exception as exc:  # pragma: no cover - network errors
            print(f"Failed to search for keyword '{keyword}': {exc}")
            continue
        for result in results:
            new_channels.append(
                {
                    "channel_id": result.channel_id,
                    "title": result.title,
                    "url": ensure_channel_url(result.channel_id, result.url),
                    "subscribers": result.subscribers,
                    "created_at": now,
                    "last_updated": None,
                    "last_attempted": None,
                    "needs_enrichment": True,
                    "emails": None,
                    "language": None,
                    "language_confidence": None,
                    "last_error": None,
                    "status": "new",
                    "status_reason": None,
                    "last_status_change": now,
                    "archived": False,
                    "archived_at": None,
                }
            )

    inserted = database.bulk_insert_channels(new_channels)
    totals = database.get_channel_totals()

    return JSONResponse({"found": inserted, "uniqueTotal": totals["total"]})


@app.post("/api/blacklist/import")
async def api_blacklist_import(file: UploadFile = File(...)) -> JSONResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing CSV file")

    try:
        raw_bytes = await file.read()
    except Exception as exc:  # pragma: no cover - I/O errors are rare
        raise HTTPException(status_code=400, detail=f"Failed to read upload: {exc}") from exc

    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    try:
        decoded = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded") from exc

    reader = csv.DictReader(io.StringIO(decoded))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV is missing headers")

    normalized_headers = {header.strip().lower() for header in reader.fieldnames if header}
    if not {"channel_id", "url"} & normalized_headers:
        raise HTTPException(status_code=400, detail="CSV must include a 'channel_id' or 'url' column")

    timestamp = dt.datetime.utcnow().isoformat()
    seen: Set[str] = set()
    summary = {"imported": 0, "updated": 0, "created": 0, "skipped": 0}

    for row in reader:
        if not row:
            summary["skipped"] += 1
            continue
        normalized = {str(key).strip().lower(): (value or "").strip() for key, value in row.items() if key}
        candidate = normalized.get("channel_id") or normalized.get("url")
        channel_id = _extract_channel_id(candidate)
        if not channel_id or channel_id in seen:
            summary["skipped"] += 1
            continue

        seen.add(channel_id)
        summary["imported"] += 1
        updated, created = database.archive_or_create_channel(channel_id, timestamp)
        if created:
            summary["created"] += 1
        elif updated:
            summary["updated"] += 1
        else:
            summary["skipped"] += 1

    return JSONResponse(summary)


@app.post("/api/enrich")
def api_enrich(payload: Dict[str, Any] = Body(default={})) -> JSONResponse:
    limit = payload.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="limit must be an integer or null")
        if limit <= 0:
            raise HTTPException(status_code=400, detail="limit must be greater than zero")

    mode = payload.get("mode", "full")
    if mode not in {"full", "email_only"}:
        raise HTTPException(status_code=400, detail="mode must be 'full' or 'email_only'")

    job = manager.start_job(limit, mode=mode)
    return JSONResponse({"jobId": job.job_id, "total": job.total, "mode": job.mode})


@app.get("/api/enrich/stream/{job_id}")
def api_enrich_stream(job_id: str) -> StreamingResponse:
    try:
        generator = manager.stream(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Unknown enrichment job")
    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(generator, media_type="text/event-stream", headers=headers)


@app.get("/api/channels")
def api_channels(
    q: Optional[str] = Query(default=None),
    language: Optional[List[str]] = Query(default=None),
    status: Optional[List[str]] = Query(default=None),
    min_subscribers: Optional[str] = Query(default=None),
    max_subscribers: Optional[str] = Query(default=None),
    sort: str = Query(default="created_at"),
    order: str = Query(default="desc"),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    emails_only: bool = Query(default=False),
    include_archived: bool = Query(default=False),
) -> JSONResponse:
    filters = _collect_filters(
        q=q,
        languages=language,
        statuses=status,
        min_subscribers=min_subscribers,
        max_subscribers=max_subscribers,
        emails_only=emails_only,
        include_archived=include_archived,
    )

    items, total = database.get_channels(
        sort=sort,
        order=order,
        limit=limit,
        offset=offset,
        emails_only=emails_only,
        include_archived=include_archived,
        **filters,
    )
    return JSONResponse({"items": items, "total": total})


@app.post("/api/channels/{channel_id}/archive")
def api_archive_channel(channel_id: str) -> JSONResponse:
    timestamp = dt.datetime.utcnow().isoformat()
    archived_ids = database.archive_channels_by_ids([channel_id], timestamp)
    if not archived_ids:
        raise HTTPException(status_code=404, detail="Channel not found or already archived")
    return JSONResponse({"archived": len(archived_ids), "archivedIds": archived_ids, "archivedAt": timestamp})


@app.post("/api/channels/archive_bulk")
def api_archive_bulk(
    payload: Dict[str, Any] = Body(default={}),
    q: Optional[str] = Query(default=None),
    language: Optional[List[str]] = Query(default=None),
    status: Optional[List[str]] = Query(default=None),
    min_subscribers: Optional[str] = Query(default=None),
    max_subscribers: Optional[str] = Query(default=None),
    sort: str = Query(default="created_at"),
    order: str = Query(default="desc"),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    emails_only: bool = Query(default=False),
    include_archived: bool = Query(default=False),
) -> JSONResponse:
    channel_ids: Optional[List[str]] = None
    if isinstance(payload, dict):
        ids = payload.get("channel_ids")
        if ids is not None:
            if not isinstance(ids, list) or not all(isinstance(value, str) for value in ids):
                raise HTTPException(status_code=400, detail="channel_ids must be a list of strings")
            channel_ids = ids
        filter_mode = payload.get("filter")
        if filter_mode == "emails_only":
            emails_only = True

    timestamp = dt.datetime.utcnow().isoformat()

    if channel_ids is None:
        filters = _collect_filters(
            q=q,
            languages=language,
            statuses=status,
            min_subscribers=min_subscribers,
            max_subscribers=max_subscribers,
            emails_only=emails_only,
            include_archived=include_archived,
        )
        items, _ = database.get_channels(
            sort=sort,
            order=order,
            limit=limit,
            offset=offset,
            emails_only=emails_only,
            include_archived=include_archived,
            **filters,
        )
        channel_ids = [item["channel_id"] for item in items]

    archived_ids = database.archive_channels_by_ids(channel_ids or [], timestamp)
    return JSONResponse({"archived": len(archived_ids), "archivedIds": archived_ids, "archivedAt": timestamp})


@app.get("/api/export/csv")
def api_export_csv(
    q: Optional[str] = Query(default=None),
    language: Optional[List[str]] = Query(default=None),
    status: Optional[List[str]] = Query(default=None),
    min_subscribers: Optional[str] = Query(default=None),
    max_subscribers: Optional[str] = Query(default=None),
    sort: str = Query(default="created_at"),
    order: str = Query(default="desc"),
    emails_only: bool = Query(default=False),
    include_archived: bool = Query(default=False),
) -> PlainTextResponse:
    filters = _collect_filters(
        q=q,
        languages=language,
        statuses=status,
        min_subscribers=min_subscribers,
        max_subscribers=max_subscribers,
        emails_only=emails_only,
        include_archived=include_archived,
    )

    items, _ = database.get_channels(
        sort=sort,
        order=order,
        limit=10_000,
        offset=0,
        emails_only=emails_only,
        include_archived=include_archived,
        **filters,
    )
    headers = [
        "Channel Name",
        "URL",
        "Subscribers",
        "Language",
        "Emails",
        "Status",
        "Last Updated",
        "Last Status Change",
        "Created At",
        "Last Attempted",
        "Error Reason",
    ]
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)
    for item in items:
        writer.writerow(
            [
                item.get("title") or "",
                item.get("url") or "",
                item.get("subscribers") or "",
                item.get("language") or "",
                item.get("emails") or "",
                item.get("status") or "",
                item.get("last_updated") or "",
                item.get("last_status_change") or "",
                item.get("created_at") or "",
                item.get("last_attempted") or "",
                item.get("status_reason") or item.get("last_error") or "",
            ]
        )

    csv_data = buffer.getvalue()
    return PlainTextResponse(content=csv_data, media_type="text/csv; charset=utf-8")


@app.get("/api/stats")
def api_stats() -> JSONResponse:
    totals = database.get_channel_totals()
    return JSONResponse(totals)
