"""FastAPI application powering the Crypto YouTube Harvester."""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import zipfile
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse

from . import database
from .database import ChannelCategory, ChannelFilters, ensure_channel_url
from .enrichment import manager
from .youtube import (
    ChannelResolution,
    DiscoveryMetadata,
    fetch_discovery_metadata,
    normalize_channel_reference,
    resolve_channel,
    sanitize_channel_input,
    search_channels,
)

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


def _unwrap_single_value(value: Any) -> Any:
    if isinstance(value, list):
        for candidate in value:
            unwrapped = _unwrap_single_value(candidate)
            if unwrapped not in (None, ""):
                return unwrapped
        return None
    return value


def _parse_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    candidate = str(value).strip()
    if not candidate:
        return None
    candidate = candidate.replace(",", "").replace("_", "")
    if candidate.endswith("+"):
        candidate = candidate[:-1]
    if not candidate:
        return None
    multiplier_map = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
    suffix = candidate[-1].lower()
    multiplier = multiplier_map.get(suffix)
    if multiplier:
        candidate = candidate[:-1]
        try:
            return int(float(candidate) * multiplier)
        except ValueError:
            return None
    try:
        return int(candidate)
    except ValueError:
        try:
            return int(float(candidate))
        except ValueError:
            return None


def _collect_filters(
    *,
    q: Optional[str],
    languages: Optional[List[str]],
    statuses: Optional[List[str]],
    min_subscribers: Optional[str],
    max_subscribers: Optional[str],
    emails_only: bool,
    include_archived: bool,
    email_gate_only: bool,
    unique_emails: bool,
) -> ChannelFilters:
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

    return ChannelFilters(
        query_text=q.strip() if q else None,
        languages=language_values,
        statuses=status_values,
        min_subscribers=min_subs_int,
        max_subscribers=max_subs_int,
        emails_only=emails_only,
        include_archived=include_archived,
        email_gate_only=email_gate_only,
        unique_emails=emails_only and unique_emails,
    )


def _parse_iso_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    candidate = candidate.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _parse_category(value: Optional[str]) -> ChannelCategory:
    if value is None:
        return ChannelCategory.ACTIVE
    try:
        return ChannelCategory(value.lower())
    except ValueError as exc:
        allowed = ", ".join(category.value for category in ChannelCategory)
        raise HTTPException(status_code=400, detail=f"category must be one of: {allowed}") from exc


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

    now_dt = dt.datetime.now(dt.timezone.utc)
    now = now_dt.isoformat()
    new_channels: List[Dict[str, Any]] = []
    blacklisted_candidates = 0

    max_age_value = _unwrap_single_value(payload.get("last_upload_max_age_days"))
    if max_age_value in (None, ""):
        max_age_value = _unwrap_single_value(payload.get("lastUploadMaxAgeDays"))
    if isinstance(max_age_value, str):
        max_age_value = max_age_value.strip()
    if max_age_value == "":
        max_age_value = None
    last_upload_max_age_days: Optional[int] = None
    if max_age_value is not None:
        try:
            last_upload_max_age_days = int(max_age_value)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="last_upload_max_age_days must be an integer"
            )
        if last_upload_max_age_days < 0:
            raise HTTPException(
                status_code=400, detail="last_upload_max_age_days cannot be negative"
            )

    deny_languages_raw = payload.get("deny_languages")
    if deny_languages_raw is None:
        deny_languages_raw = payload.get("denyLanguages")
    deny_languages: Set[str] = set()
    if isinstance(deny_languages_raw, str):
        candidates = [segment.strip() for segment in deny_languages_raw.split(",")]
        deny_languages = {value.lower() for value in candidates if value}
    elif isinstance(deny_languages_raw, list):
        deny_languages = {
            str(value).strip().lower()
            for value in deny_languages_raw
            if isinstance(value, (str, int, float)) and str(value).strip()
        }

    requires_metadata = bool(deny_languages or last_upload_max_age_days is not None)
    metadata_cache: Dict[str, DiscoveryMetadata] = {}

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
            if database.is_blacklisted(result.channel_id):
                database.ensure_blacklisted_channel(result.channel_id, now)
                continue

            metadata: Optional[DiscoveryMetadata] = None
            if requires_metadata:
                metadata = metadata_cache.get(result.channel_id)
                if metadata is None:
                    metadata = fetch_discovery_metadata(result.channel_id)
                    metadata_cache[result.channel_id] = metadata

            violations: List[str] = []
            if deny_languages and metadata and metadata.language:
                language_value = str(metadata.language).strip()
                if language_value and language_value.lower() in deny_languages:
                    violations.append(
                        f"Language '{language_value}' denied during discovery"
                    )

            if (
                last_upload_max_age_days is not None
                and metadata
                and metadata.last_upload
            ):
                last_upload_dt = _parse_iso_datetime(metadata.last_upload)
                if last_upload_dt is not None:
                    last_upload_utc = last_upload_dt.astimezone(dt.timezone.utc)
                    age = now_dt - last_upload_utc
                    if age > dt.timedelta(days=last_upload_max_age_days):
                        violations.append(
                            "Last upload is older than "
                            f"{last_upload_max_age_days} days (last: {last_upload_utc.date().isoformat()})"
                        )

            if violations:
                database.ensure_blacklisted_channel(
                    result.channel_id,
                    now,
                    url=ensure_channel_url(result.channel_id, result.url),
                    name=result.title,
                    reason="; ".join(violations),
                )
                blacklisted_candidates += 1
                continue

            new_channels.append(
                {
                    "channel_id": result.channel_id,
                    "name": result.title,
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
                }
            )

            if metadata:
                channel_record = new_channels[-1]
                if metadata.last_upload:
                    channel_record["last_updated"] = metadata.last_upload
                if metadata.language:
                    channel_record["language"] = metadata.language
                if metadata.language_confidence is not None:
                    channel_record["language_confidence"] = metadata.language_confidence

    inserted = database.bulk_insert_channels(new_channels)
    totals = database.get_channel_totals()

    response_payload = {"found": inserted, "uniqueTotal": totals["total"]}
    if blacklisted_candidates:
        response_payload["blacklisted"] = blacklisted_candidates

    return JSONResponse(response_payload)


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
    cache: Dict[str, Tuple[Optional[ChannelResolution], Optional[str]]] = {}
    created: List[Dict[str, Any]] = []
    updated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    processed = 0

    unresolved_messages = {
        "invalid_input": "No usable channel reference found in this row.",
        "invalid_url": "Value is not a valid YouTube channel URL or ID.",
        "network_error": "Network error while contacting YouTube.",
        "not_found": "Channel appears to be missing or unavailable.",
        "resolution_failed": "Unable to resolve a channel ID from the provided value.",
    }
    skipped_messages = {
        "duplicate_in_file": "Duplicate channel in uploaded CSV.",
        "already_blacklisted": "Channel already blacklisted.",
    }

    for row in reader:
        processed += 1
        normalized = {
            str(key).strip().lower(): (value or "").strip()
            for key, value in row.items()
            if key
        }
        metadata: Dict[str, Any] = {}
        csv_subscribers = normalized.get("subscribers") or normalized.get("subscriber_count")
        subscribers_value = _parse_optional_int(csv_subscribers)
        if subscribers_value is not None:
            metadata["subscribers"] = subscribers_value
        csv_language = normalized.get("language")
        if csv_language:
            metadata["language"] = csv_language
        csv_emails = normalized.get("emails") or normalized.get("email")
        if csv_emails:
            parsed_emails = database.parse_email_candidates(csv_emails)
            if parsed_emails:
                unique_emails = list(dict.fromkeys(email.strip() for email in parsed_emails if email.strip()))
                if unique_emails:
                    metadata["emails"] = ", ".join(unique_emails)
            elif csv_emails:
                metadata["emails"] = csv_emails
        source_column = "channel_id" if normalized.get("channel_id") else "url"
        candidate_value = normalized.get(source_column) or normalized.get("url") or ""
        row_number = reader.line_num
        original_value = candidate_value.strip()
        sanitized_value = sanitize_channel_input(candidate_value)
        if not sanitized_value:
            unresolved.append(
                {
                    "row": row_number,
                    "input": original_value,
                    "normalized": sanitized_value,
                    "reason": "invalid_input",
                    "message": unresolved_messages["invalid_input"],
                    "column": source_column,
                }
            )
            continue

        normalized_reference = normalize_channel_reference(sanitized_value)
        if not normalized_reference:
            unresolved.append(
                {
                    "row": row_number,
                    "input": original_value or sanitized_value,
                    "normalized": sanitized_value,
                    "reason": "invalid_url",
                    "message": unresolved_messages["invalid_url"],
                    "column": source_column,
                }
            )
            continue

        cache_key = normalized_reference.lower()
        if cache_key not in cache:
            cache[cache_key] = resolve_channel(normalized_reference)
        resolution, reason = cache[cache_key]
        if not resolution:
            reason_code = reason or "resolution_failed"
            unresolved.append(
                {
                    "row": row_number,
                    "input": original_value or sanitized_value,
                    "normalized": sanitized_value,
                    "reason": reason_code,
                    "message": unresolved_messages.get(
                        reason_code, unresolved_messages["resolution_failed"]
                    ),
                    "column": source_column,
                }
            )
            continue

        channel_id = resolution.channel_id.upper()
        if channel_id in seen:
            skipped.append(
                {
                    "row": row_number,
                    "channel_id": channel_id,
                    "reason": "duplicate_in_file",
                    "message": skipped_messages["duplicate_in_file"],
                    "column": source_column,
                }
            )
            continue

        seen.add(channel_id)
        if database.is_blacklisted(channel_id):
            skipped.append(
                {
                    "row": row_number,
                    "channel_id": channel_id,
                    "reason": "already_blacklisted",
                    "message": skipped_messages["already_blacklisted"],
                    "column": source_column,
                }
            )
            continue

        moved = database.blacklist_channels_by_ids([channel_id], timestamp)
        database.ensure_blacklisted_channel(
            channel_id,
            timestamp,
            url=resolution.canonical_url,
            name=resolution.title or resolution.handle,
            metadata=metadata,
        )
        record = {
            "channel_id": channel_id,
            "url": resolution.canonical_url,
            "handle": resolution.handle,
            "name": resolution.title or resolution.handle,
        }
        if metadata.get("subscribers") is not None:
            record["subscribers"] = metadata["subscribers"]
        if metadata.get("language"):
            record["language"] = metadata["language"]
        if metadata.get("emails"):
            record["emails"] = metadata["emails"]
        if moved:
            updated.append(record)
        else:
            created.append(record)

    result = {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "unresolved": unresolved,
        "counts": {
            "created": len(created),
            "updated": len(updated),
            "skipped": len(skipped),
            "unresolved": len(unresolved),
            "processed": processed,
        },
        "processedAt": timestamp,
    }

    return JSONResponse(result)


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

    force_run = bool(payload.get("forceRun"))
    never_reenrich = bool(payload.get("neverReenrich"))

    job = manager.start_job(
        limit,
        mode=mode,
        force_run=force_run,
        never_reenrich=never_reenrich,
    )
    return JSONResponse(
        {
            "jobId": job.job_id,
            "total": job.total,
            "mode": job.mode,
            "requested": job.requested,
            "skipped": job.skipped,
        }
    )


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
    email_gate_only: bool = Query(default=False),
    unique_emails: bool = Query(default=False),
    category: Optional[str] = Query(default=ChannelCategory.ACTIVE.value),
) -> JSONResponse:
    category_value = _parse_category(category)
    filters = _collect_filters(
        q=q,
        languages=language,
        statuses=status,
        min_subscribers=min_subscribers,
        max_subscribers=max_subscribers,
        emails_only=emails_only,
        include_archived=include_archived,
        email_gate_only=email_gate_only,
        unique_emails=unique_emails,
    )
    items, total = database.get_channels(
        category_value,
        filters,
        sort=sort,
        order=order,
        limit=limit,
        offset=offset,
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
    email_gate_only: bool = Query(default=False),
    unique_emails: bool = Query(default=False),
    category: Optional[str] = Query(default=ChannelCategory.ACTIVE.value),
) -> JSONResponse:
    category_value = _parse_category(category)
    if category_value is not ChannelCategory.ACTIVE:
        raise HTTPException(status_code=400, detail="Archive bulk only supported for active channels")
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
            email_gate_only=email_gate_only,
            unique_emails=unique_emails,
        )
        items, _ = database.get_channels(
            category_value,
            filters,
            sort=sort,
            order=order,
            limit=limit,
            offset=offset,
        )
        channel_ids = [item["channel_id"] for item in items]

    archived_ids = database.archive_channels_by_ids(channel_ids or [], timestamp)
    return JSONResponse({"archived": len(archived_ids), "archivedIds": archived_ids, "archivedAt": timestamp})


@app.post("/api/channels/archive_exported")
def api_archive_exported(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    exported_at_raw = payload.get("exported_at")
    if not isinstance(exported_at_raw, str) or not exported_at_raw.strip():
        raise HTTPException(status_code=400, detail="exported_at must be provided")

    exported_at = exported_at_raw.strip()
    timestamp = dt.datetime.utcnow().isoformat()
    archived_ids = database.archive_channels_by_exported_at(exported_at, timestamp)
    return JSONResponse({"archived": len(archived_ids), "archivedIds": archived_ids, "archivedAt": timestamp})


@app.post("/api/channels/{channel_id}/blacklist")
def api_blacklist_channel(channel_id: str, category: Optional[str] = Query(default=None)) -> JSONResponse:
    timestamp = dt.datetime.utcnow().isoformat()
    sources: Optional[List[ChannelCategory]] = None
    if category:
        parsed = _parse_category(category)
        if parsed is ChannelCategory.BLACKLISTED:
            raise HTTPException(status_code=400, detail="Channel already blacklisted")
        sources = [parsed]
    blacklisted_ids = database.blacklist_channels_by_ids([channel_id], timestamp, source_categories=sources)
    database.ensure_blacklisted_channel(channel_id, timestamp)
    if not blacklisted_ids and not database.is_blacklisted(channel_id):
        raise HTTPException(status_code=404, detail="Channel not found")
    return JSONResponse(
        {
            "blacklisted": len(blacklisted_ids) or 1,
            "blacklistedIds": blacklisted_ids or [channel_id],
            "blacklistedAt": timestamp,
        }
    )


@app.post("/api/channels/blacklist_bulk")
def api_blacklist_bulk(
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
    email_gate_only: bool = Query(default=False),
    unique_emails: bool = Query(default=False),
    category: Optional[str] = Query(default=ChannelCategory.ACTIVE.value),
) -> JSONResponse:
    category_value = _parse_category(category)
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

    if channel_ids is None:
        filters = _collect_filters(
            q=q,
            languages=language,
            statuses=status,
            min_subscribers=min_subscribers,
            max_subscribers=max_subscribers,
            emails_only=emails_only,
            include_archived=include_archived,
            email_gate_only=email_gate_only,
            unique_emails=unique_emails,
        )
        items, _ = database.get_channels(
            category_value,
            filters,
            sort=sort,
            order=order,
            limit=limit,
            offset=offset,
        )
        channel_ids = [item["channel_id"] for item in items]

    timestamp = dt.datetime.utcnow().isoformat()
    sources: Optional[List[ChannelCategory]] = None
    if category_value is not ChannelCategory.BLACKLISTED:
        sources = [category_value]
    blacklisted_ids = database.blacklist_channels_by_ids(channel_ids or [], timestamp, source_categories=sources)
    for channel_id in channel_ids or []:
        database.ensure_blacklisted_channel(channel_id, timestamp)
    return JSONResponse(
        {
            "blacklisted": len(blacklisted_ids),
            "blacklistedIds": blacklisted_ids,
            "blacklistedAt": timestamp,
        }
    )


@app.post("/api/channels/{channel_id}/restore")
def api_restore_channel(channel_id: str) -> JSONResponse:
    timestamp = dt.datetime.utcnow().isoformat()
    restored_ids = database.restore_channels_by_ids([channel_id], timestamp)
    if not restored_ids:
        raise HTTPException(status_code=404, detail="Channel not found in archived or blacklisted tables")
    return JSONResponse({"restored": len(restored_ids), "restoredIds": restored_ids, "restoredAt": timestamp})


@app.post("/api/channels/restore_bulk")
def api_restore_bulk(
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
    email_gate_only: bool = Query(default=False),
    unique_emails: bool = Query(default=False),
    category: Optional[str] = Query(default=ChannelCategory.ARCHIVED.value),
) -> JSONResponse:
    category_value = _parse_category(category)
    if category_value is ChannelCategory.ACTIVE:
        raise HTTPException(status_code=400, detail="Restore requires archived or blacklisted category")
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

    if channel_ids is None:
        filters = _collect_filters(
            q=q,
            languages=language,
            statuses=status,
            min_subscribers=min_subscribers,
            max_subscribers=max_subscribers,
            emails_only=emails_only,
            include_archived=include_archived,
            email_gate_only=email_gate_only,
            unique_emails=unique_emails,
        )
        items, _ = database.get_channels(
            category_value,
            filters,
            sort=sort,
            order=order,
            limit=limit,
            offset=offset,
        )
        channel_ids = [item["channel_id"] for item in items]

    timestamp = dt.datetime.utcnow().isoformat()
    restored_ids = database.restore_channels_by_ids(channel_ids or [], timestamp, source_categories=[category_value])
    return JSONResponse({"restored": len(restored_ids), "restoredIds": restored_ids, "restoredAt": timestamp})


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
    unique_emails: bool = Query(default=False),
    email_gate_only: bool = Query(default=False),
    category: Optional[str] = Query(default=ChannelCategory.ACTIVE.value),
    archive_exported: bool = Query(default=False),
) -> PlainTextResponse:
    category_value = _parse_category(category)
    filters = _collect_filters(
        q=q,
        languages=language,
        statuses=status,
        min_subscribers=min_subscribers,
        max_subscribers=max_subscribers,
        emails_only=emails_only,
        include_archived=include_archived,
        email_gate_only=email_gate_only,
        unique_emails=unique_emails,
    )
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    exported_channel_ids: List[str] = []
    export_timestamp = dt.datetime.utcnow().isoformat()

    if emails_only and unique_emails:
        rows = database.get_unique_email_rows(filters, category=category_value)
        writer.writerow(
            [
                "Email",
                "Primary Channel Name",
                "Primary Channel URL",
                "Other Channels Count",
                "Last Updated",
            ]
        )
        for row in rows:
            channel_id = row.get("primary_channel_id")
            if channel_id:
                exported_channel_ids.append(channel_id)
            writer.writerow(
                [
                    row.get("email", ""),
                    row.get("primary_channel_name", ""),
                    row.get("primary_channel_url", ""),
                    row.get("other_channels_count", 0),
                    row.get("last_updated", ""),
                ]
            )
    else:
        items, _ = database.get_channels(
            category_value,
            filters,
            sort=sort,
            order=order,
            limit=10_000,
            offset=0,
        )
        writer.writerow(
            [
                "Channel Name",
                "URL",
                "Subscribers",
                "Language",
                "Emails",
                "Email Gate",
                "Status",
                "Last Updated",
                "Last Status Change",
                "Created At",
                "Last Attempted",
                "Exported At",
                "Archived At",
                "Error Reason",
            ]
        )
        for item in items:
            channel_id = item.get("channel_id")
            if channel_id:
                exported_channel_ids.append(channel_id)
            writer.writerow(
                [
                    item.get("name") or "",
                    item.get("url") or "",
                    item.get("subscribers") or "",
                    item.get("language") or "",
                    item.get("emails") or "",
                    "Yes"
                    if item.get("email_gate_present")
                    else ("No" if item.get("email_gate_present") == 0 else ""),
                    item.get("status") or "",
                    item.get("last_updated") or "",
                    item.get("last_status_change") or "",
                    item.get("created_at") or "",
                    item.get("last_attempted") or "",
                    item.get("exported_at") or "",
                    item.get("archived_at") or "",
                    item.get("status_reason") or item.get("last_error") or "",
                ]
            )

    csv_data = buffer.getvalue()
    if exported_channel_ids:
        try:
            database.mark_channels_exported(
                category_value,
                exported_channel_ids,
                export_timestamp,
                archive=archive_exported,
            )
        except Exception as exc:  # pragma: no cover - defensive
            raise HTTPException(status_code=500, detail="Failed to update exported rows") from exc
    headers = {"X-Export-Timestamp": export_timestamp}
    return PlainTextResponse(content=csv_data, media_type="text/csv; charset=utf-8", headers=headers)


@app.get("/api/export/bundle")
def api_export_bundle() -> StreamingResponse:
    data, email_index = database.fetch_project_bundle_data()
    export_timestamp = dt.datetime.utcnow().replace(microsecond=0).isoformat()

    channel_counts = {
        category: len(records) for category, records in data.get("channels", {}).items()
    }
    meta = {
        "schemaVersion": database.PROJECT_BUNDLE_SCHEMA_VERSION,
        "exportedAt": export_timestamp,
        "channelCounts": channel_counts,
        "blacklistCount": len(data.get("blacklist", [])),
        "emailRelations": {
            "uniqueEmails": len(data.get("emails_unique", [])),
            "channelEmailLinks": len(data.get("channel_emails", [])),
        },
        "globalEmailIndex": email_index,
    }

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(
            "data.json",
            json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True),
        )
        bundle.writestr(
            "meta.json",
            json.dumps(meta, indent=2, ensure_ascii=False, sort_keys=True),
        )

    buffer.seek(0)
    filename = f"project-bundle-{export_timestamp.replace(':', '').replace('T', '_')}.zip"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Export-Timestamp": export_timestamp,
    }
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@app.post("/api/import/bundle")
async def api_import_bundle(
    file: UploadFile = File(...),
    dry_run: bool = Query(False, alias="dryRun"),
) -> JSONResponse:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing bundle archive")

    try:
        raw_bytes = await file.read()
    except Exception as exc:  # pragma: no cover - defensive I/O handling
        raise HTTPException(status_code=400, detail=f"Failed to read upload: {exc}") from exc

    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Uploaded bundle is empty")

    try:
        bundle_file = io.BytesIO(raw_bytes)
        with zipfile.ZipFile(bundle_file) as bundle:
            try:
                data_bytes = bundle.read("data.json")
            except KeyError as exc:
                raise HTTPException(status_code=400, detail="Bundle archive is missing data.json") from exc

            meta: Optional[Dict[str, Any]] = None
            if "meta.json" in bundle.namelist():
                try:
                    meta_bytes = bundle.read("meta.json")
                    meta = json.loads(meta_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise HTTPException(status_code=400, detail=f"meta.json is invalid: {exc}") from exc

            try:
                data = json.loads(data_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise HTTPException(status_code=400, detail=f"data.json is invalid: {exc}") from exc
    except zipfile.BadZipFile as exc:
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid bundle archive") from exc

    try:
        summary = database.restore_project_bundle(data, meta=meta, dry_run=dry_run)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return JSONResponse(summary)


@app.get("/api/stats")
def api_stats() -> JSONResponse:
    totals = database.get_channel_totals()
    return JSONResponse(totals)
