"""Background enrichment manager for channel processing."""
from __future__ import annotations

import datetime as dt
import json
import queue
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import database
from .youtube import EnrichmentError, enrich_channel, enrich_channel_email_only


@dataclass
class EnrichmentJob:
    """Represents a single enrichment batch run."""

    job_id: str
    channels: List[Dict]
    mode: str = "full"
    started_at: float = field(default_factory=time.monotonic)
    completed: int = 0
    errors: int = 0
    queue: "queue.Queue[Optional[Dict]]" = field(default_factory=queue.Queue)
    lock: threading.Lock = field(default_factory=threading.Lock)
    done_event: threading.Event = field(default_factory=threading.Event)

    def push_update(self, payload: Dict) -> None:
        self.queue.put(payload)

    def mark_done(self) -> None:
        if self.done_event.is_set():
            return
        self.done_event.set()
        self.queue.put(None)

    @property
    def total(self) -> int:
        return len(self.channels)

    def update_counts(self, *, completed: bool) -> None:
        with self.lock:
            if completed:
                self.completed += 1
            else:
                self.errors += 1
            summary = self.summary()
        self.push_update({"type": "progress", **summary})

    def summary(self) -> Dict:
        elapsed = time.monotonic() - self.started_at
        pending = max(0, self.total - self.completed - self.errors)
        return {
            "jobId": self.job_id,
            "total": self.total,
            "completed": self.completed,
            "errors": self.errors,
            "pending": pending,
            "durationSeconds": round(elapsed, 2),
            "mode": self.mode,
        }


class EnrichmentManager:
    """Coordinates enrichment jobs and exposes streaming progress."""

    def __init__(self, *, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: Dict[str, EnrichmentJob] = {}
        self._lock = threading.Lock()

    def start_job(self, limit: Optional[int], mode: str = "full") -> EnrichmentJob:
        if mode not in {"full", "email_only"}:
            raise ValueError(f"Unsupported enrichment mode: {mode}")
        if mode == "email_only":
            channels = database.get_channels_for_email_enrichment(limit)
        else:
            channels = database.get_pending_channels(limit)
        job_id = str(uuid.uuid4())
        job = EnrichmentJob(job_id=job_id, channels=channels, mode=mode)
        with self._lock:
            self._jobs[job_id] = job

        if not channels:
            job.mark_done()
            return job

        for channel in channels:
            self._executor.submit(self._process_channel, job, channel)

        # Emit initial summary to kick off UI progress display.
        job.push_update({"type": "progress", **job.summary()})
        return job

    def stream(self, job_id: str):
        job = self._jobs.get(job_id)
        if not job:
            raise KeyError(job_id)

        def event_stream():
            try:
                while True:
                    try:
                        item = job.queue.get(timeout=10)
                    except queue.Empty:
                        # Periodic heartbeat to keep connection alive.
                        yield "data: {}\n\n"
                        continue
                    if item is None:
                        summary = job.summary()
                        summary["done"] = True
                        yield f"data: {json.dumps({'type': 'progress', **summary})}\n\n"
                        break
                    yield f"data: {json.dumps(item)}\n\n"
            finally:
                job.mark_done()
                with self._lock:
                    self._jobs.pop(job_id, None)

        return event_stream()

    def _process_channel(self, job: EnrichmentJob, channel: Dict) -> None:
        if job.mode == "email_only":
            self._process_channel_email_only(job, channel)
        else:
            self._process_channel_full(job, channel)

    def _process_channel_full(self, job: EnrichmentJob, channel: Dict) -> None:
        channel_id = channel["channel_id"]
        now = dt.datetime.utcnow().isoformat()
        database.update_channel_enrichment(
            channel_id,
            last_attempted=now,
        )
        database.set_channel_status(channel_id, "processing", reason=None, timestamp=now)
        job.push_update(
            {
                "type": "channel",
                "channelId": channel_id,
                "status": "processing",
                "statusReason": None,
                "lastStatusChange": now,
                "mode": job.mode,
            }
        )

        try:
            enriched = enrich_channel(channel)
        except EnrichmentError as exc:
            error_time = dt.datetime.utcnow().isoformat()
            reason = str(exc)
            database.update_channel_enrichment(
                channel_id,
                needs_enrichment=True,
                last_error=reason,
                status="error",
                status_reason=reason,
                last_status_change=error_time,
            )
            job.update_counts(completed=False)
            job.push_update(
                {
                    "type": "channel",
                    "channelId": channel_id,
                    "status": "error",
                    "statusReason": reason,
                    "lastStatusChange": error_time,
                    "mode": job.mode,
                }
            )
            if job.completed + job.errors >= job.total:
                job.mark_done()
            return
        except Exception as exc:  # Catch-all safety net
            error_time = dt.datetime.utcnow().isoformat()
            reason = f"Unexpected error: {exc}"[:500]
            database.update_channel_enrichment(
                channel_id,
                needs_enrichment=True,
                last_error=reason,
                status="error",
                status_reason=reason,
                last_status_change=error_time,
            )
            job.update_counts(completed=False)
            job.push_update(
                {
                    "type": "channel",
                    "channelId": channel_id,
                    "status": "error",
                    "statusReason": reason,
                    "lastStatusChange": error_time,
                    "mode": job.mode,
                }
            )
            if job.completed + job.errors >= job.total:
                job.mark_done()
            return

        success_time = dt.datetime.utcnow().isoformat()
        enriched_emails = enriched.get("emails") or []
        if enriched_emails:
            database.record_channel_emails(channel_id, enriched_emails, success_time)
        emails = ", ".join(enriched_emails) if enriched_emails else None
        database.update_channel_enrichment(
            channel_id,
            name=enriched.get("name") or enriched.get("title") or channel.get("name") or channel.get("title"),
            subscribers=enriched.get("subscribers"),
            language=enriched.get("language"),
            language_confidence=enriched.get("language_confidence"),
            emails=emails,
            last_updated=enriched.get("last_updated") or success_time,
            last_attempted=success_time,
            needs_enrichment=False,
            last_error=None,
            status="completed",
            status_reason=None,
            last_status_change=success_time,
        )

        job.update_counts(completed=True)
        job.push_update(
            {
                "type": "channel",
                "channelId": channel_id,
                "status": "completed",
                "statusReason": None,
                "lastStatusChange": success_time,
                "subscribers": enriched.get("subscribers"),
                "language": enriched.get("language"),
                "languageConfidence": enriched.get("language_confidence"),
                "emails": enriched_emails,
                "lastUpdated": enriched.get("last_updated") or success_time,
                "mode": job.mode,
            }
        )

        if job.completed + job.errors >= job.total:
            job.mark_done()

    def _process_channel_email_only(self, job: EnrichmentJob, channel: Dict) -> None:
        channel_id = channel["channel_id"]
        start_time = dt.datetime.utcnow().isoformat()

        parsed_emails = database.parse_email_candidates(channel.get("emails"))
        stored_emails = database.get_channel_email_set(channel_id)
        display_emails: List[str] = list(parsed_emails)
        if not display_emails and stored_emails:
            display_emails = sorted(stored_emails)
        should_skip = bool(stored_emails)
        if not should_skip and display_emails:
            should_skip = database.has_all_known_emails(display_emails)
        if should_skip:
            if display_emails:
                database.record_channel_emails(channel_id, display_emails, start_time)
            elif stored_emails:
                database.record_channel_emails(channel_id, stored_emails, start_time)
            emails_value = ", ".join(display_emails) if display_emails else channel.get("emails")
            if emails_value:
                database.update_channel_enrichment(channel_id, emails=emails_value)
            job.update_counts(completed=True)
            job.push_update(
                {
                    "type": "channel",
                    "channelId": channel_id,
                    "status": "completed",
                    "statusReason": "emails unchanged",
                    "lastStatusChange": start_time,
                    "emails": display_emails,
                    "lastUpdated": channel.get("last_updated") or start_time,
                    "mode": job.mode,
                }
            )
            if job.completed + job.errors >= job.total:
                job.mark_done()
            return

        job.push_update(
            {
                "type": "channel",
                "channelId": channel_id,
                "status": "processing",
                "statusReason": None,
                "lastStatusChange": start_time,
                "mode": job.mode,
            }
        )

        try:
            enriched = enrich_channel_email_only(channel)
        except EnrichmentError as exc:
            error_time = dt.datetime.utcnow().isoformat()
            reason = str(exc)
            job.update_counts(completed=False)
            job.push_update(
                {
                    "type": "channel",
                    "channelId": channel_id,
                    "status": "error",
                    "statusReason": reason,
                    "lastStatusChange": error_time,
                    "mode": job.mode,
                }
            )
            if job.completed + job.errors >= job.total:
                job.mark_done()
            return
        except Exception as exc:  # pragma: no cover - defensive guard
            error_time = dt.datetime.utcnow().isoformat()
            reason = f"Unexpected error: {exc}"[:500]
            job.update_counts(completed=False)
            job.push_update(
                {
                    "type": "channel",
                    "channelId": channel_id,
                    "status": "error",
                    "statusReason": reason,
                    "lastStatusChange": error_time,
                    "mode": job.mode,
                }
            )
            if job.completed + job.errors >= job.total:
                job.mark_done()
            return

        success_time = dt.datetime.utcnow().isoformat()
        emails = enriched.get("emails") or []
        if emails:
            database.record_channel_emails(channel_id, emails, success_time)
        emails_value = ", ".join(emails) if emails else None
        last_updated = enriched.get("last_updated") or success_time
        database.update_channel_enrichment(
            channel_id,
            emails=emails_value,
            last_updated=last_updated,
        )

        job.update_counts(completed=True)
        job.push_update(
            {
                "type": "channel",
                "channelId": channel_id,
                "status": "completed",
                "statusReason": None,
                "lastStatusChange": success_time,
                "emails": emails,
                "lastUpdated": last_updated,
                "mode": job.mode,
            }
        )

        if job.completed + job.errors >= job.total:
            job.mark_done()


manager = EnrichmentManager()

