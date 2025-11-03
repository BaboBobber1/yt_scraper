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
from .youtube import EnrichmentError, enrich_channel


@dataclass
class EnrichmentJob:
    """Represents a single enrichment batch run."""

    job_id: str
    channels: List[Dict]
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
        }


class EnrichmentManager:
    """Coordinates enrichment jobs and exposes streaming progress."""

    def __init__(self, *, max_workers: int = 4):
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._jobs: Dict[str, EnrichmentJob] = {}
        self._lock = threading.Lock()

    def start_job(self, limit: Optional[int]) -> EnrichmentJob:
        channels = database.get_pending_channels(limit)
        job_id = str(uuid.uuid4())
        job = EnrichmentJob(job_id=job_id, channels=channels)
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
                }
            )
            if job.completed + job.errors >= job.total:
                job.mark_done()
            return

        success_time = dt.datetime.utcnow().isoformat()
        emails = ", ".join(enriched.get("emails", [])) if enriched.get("emails") else None
        database.update_channel_enrichment(
            channel_id,
            title=enriched.get("title") or channel.get("title"),
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
                "emails": enriched.get("emails", []),
                "lastUpdated": enriched.get("last_updated") or success_time,
            }
        )

        if job.completed + job.errors >= job.total:
            job.mark_done()


manager = EnrichmentManager()

