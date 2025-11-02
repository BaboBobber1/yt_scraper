"""FastAPI application powering the Crypto YouTube Harvester."""
from __future__ import annotations

import csv
import datetime as dt
import io
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from . import database
from .database import ensure_channel_url
from .youtube import enrich_channel, search_channels

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
                }
            )

    inserted = database.bulk_insert_channels(new_channels)
    totals = database.get_channel_totals()

    return JSONResponse({"found": inserted, "uniqueTotal": totals["total"]})


@app.post("/api/enrich")
def api_enrich(payload: Dict[str, Any] = Body(default={})) -> JSONResponse:
    limit = payload.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise HTTPException(status_code=400, detail="limit must be an integer or null")
        if limit <= 0:
            raise HTTPException(status_code=400, detail="limit must be greater than zero")

    pending = database.get_pending_channels(limit)
    processed = 0
    now = dt.datetime.utcnow().isoformat()

    for channel in pending:
        processed += 1
        try:
            enriched = enrich_channel(channel["url"])
            emails = ", ".join(enriched.get("emails", [])) if enriched.get("emails") else None
            database.update_channel_enrichment(
                channel["channel_id"],
                title=enriched.get("title") or channel.get("title"),
                subscribers=enriched.get("subscribers"),
                language=enriched.get("language"),
                language_confidence=enriched.get("language_confidence"),
                emails=emails,
                last_updated=enriched.get("last_updated") or now,
                last_attempted=now,
                needs_enrichment=False,
                last_error=None,
            )
        except Exception as exc:  # pragma: no cover - yt_dlp/network errors
            database.update_channel_enrichment(
                channel["channel_id"],
                last_attempted=now,
                needs_enrichment=True,
                last_error=str(exc),
            )

    return JSONResponse({"processed": processed})


@app.get("/api/channels")
def api_channels(
    search: Optional[str] = Query(default=None),
    sort: str = Query(default="created_at"),
    order: str = Query(default="desc"),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
) -> JSONResponse:
    items, total = database.get_channels(
        search=search,
        sort=sort,
        order=order,
        limit=limit,
        offset=offset,
    )
    return JSONResponse({"items": items, "total": total})


@app.get("/api/export/csv")
def api_export_csv() -> PlainTextResponse:
    items, _ = database.get_channels(search=None, sort="title", order="asc", limit=10_000, offset=0)
    headers = [
        "Channel Name",
        "URL",
        "Subscribers",
        "Language",
        "Language Confidence",
        "Emails",
        "Last Updated",
        "Created At",
        "Last Attempted",
        "Last Error",
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
                f"{item.get('language_confidence'):.2f}" if item.get("language_confidence") is not None else "",
                item.get("emails") or "",
                item.get("last_updated") or "",
                item.get("created_at") or "",
                item.get("last_attempted") or "",
                item.get("last_error") or "",
            ]
        )

    csv_data = buffer.getvalue()
    return PlainTextResponse(content=csv_data, media_type="text/csv; charset=utf-8")


@app.get("/api/stats")
def api_stats() -> JSONResponse:
    totals = database.get_channel_totals()
    return JSONResponse(totals)
