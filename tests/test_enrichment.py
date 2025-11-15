import datetime as dt
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import enrichment


@pytest.fixture
def manager():
    return enrichment.EnrichmentManager()


@pytest.fixture
def update_calls(monkeypatch):
    calls = []

    def fake_update(channel_id, **updates):
        calls.append((channel_id, updates))

    monkeypatch.setattr(enrichment.database, "update_channel_enrichment", fake_update)
    return calls


def _isoformat(dt_value: dt.datetime) -> str:
    return dt_value.replace(microsecond=0).isoformat()


def test_new_channel_without_cache_processed(manager, update_calls):
    channel = {"channel_id": "chan-new", "status": "new"}
    filtered, skipped = manager._filter_channels(
        [channel], force_run=False, never_reenrich=False
    )
    assert filtered == [channel]
    assert skipped == []
    assert update_calls == []


def test_channel_with_emails_not_skipped(manager, update_calls):
    now = dt.datetime.utcnow()
    enriched_time = _isoformat(now - dt.timedelta(days=2))
    channel = {
        "channel_id": "chan-email",
        "status": enrichment.RECENT_NO_EMAIL_STATUS,
        "last_enriched_at": enriched_time,
        "last_enriched_result": "no_emails",
        "emails": "test@example.com",
    }
    filtered, skipped = manager._filter_channels(
        [channel], force_run=False, never_reenrich=False
    )
    assert filtered == [channel]
    assert skipped == []
    assert update_calls
    channel_id, updates = update_calls[-1]
    assert channel_id == "chan-email"
    assert updates["status"] == "new"
    assert updates["status_reason"] is None


def test_recent_no_email_channel_skipped(manager, update_calls):
    now = dt.datetime.utcnow()
    enriched_time = _isoformat(now - dt.timedelta(days=1))
    channel = {
        "channel_id": "chan-skip",
        "status": "completed",
        "last_enriched_at": enriched_time,
        "last_enriched_result": "no_emails",
    }
    filtered, skipped = manager._filter_channels(
        [channel], force_run=False, never_reenrich=False
    )
    assert filtered == []
    assert skipped and skipped[0]["channel_id"] == "chan-skip"
    assert skipped[0]["skip_reason"] == "recent_no_email"
    assert update_calls
    channel_id, updates = update_calls[-1]
    assert channel_id == "chan-skip"
    assert updates["status"] == enrichment.RECENT_NO_EMAIL_STATUS
    assert updates["status_reason"] == enrichment.RECENT_NO_EMAIL_REASON
    assert updates["last_attempted"] == updates["last_status_change"]


def test_recent_no_email_not_skipped_after_error(manager, update_calls):
    now = dt.datetime.utcnow()
    enriched_time = _isoformat(now - dt.timedelta(hours=6))
    channel = {
        "channel_id": "chan-error",
        "status": "error",
        "last_enriched_at": enriched_time,
        "last_enriched_result": "no_emails",
    }
    filtered, skipped = manager._filter_channels(
        [channel], force_run=False, never_reenrich=False
    )
    assert filtered == [channel]
    assert skipped == []
    assert not update_calls


def test_no_email_outside_cooldown_processed(manager, update_calls):
    now = dt.datetime.utcnow()
    enriched_time = _isoformat(now - (enrichment.NO_EMAIL_RETRY_WINDOW + dt.timedelta(days=5)))
    channel = {
        "channel_id": "chan-retry",
        "status": enrichment.RECENT_NO_EMAIL_STATUS,
        "last_enriched_at": enriched_time,
        "last_enriched_result": "no_emails",
    }
    filtered, skipped = manager._filter_channels(
        [channel], force_run=False, never_reenrich=False
    )
    assert filtered == [channel]
    assert skipped == []
    assert update_calls
    channel_id, updates = update_calls[-1]
    assert channel_id == "chan-retry"
    assert updates["status"] == "new"
    assert updates["status_reason"] is None


def test_process_channel_feed_unavailable(monkeypatch):
    job = enrichment.EnrichmentJob(job_id="job", channels=[{"channel_id": "UC1"}])
    channel = {"channel_id": "UC1", "status": "new"}
    updates = []

    def fake_set_status(channel_id, status, reason, timestamp):
        updates.append(("set_status", channel_id, status, reason, timestamp))

    def fake_update(channel_id, **fields):
        updates.append(("update", channel_id, fields))

    def fake_record_emails(channel_id, emails, timestamp):
        updates.append(("record_emails", channel_id, emails, timestamp))

    monkeypatch.setattr(enrichment.database, "set_channel_status", fake_set_status)
    monkeypatch.setattr(enrichment.database, "update_channel_enrichment", fake_update)
    monkeypatch.setattr(enrichment.database, "record_channel_emails", fake_record_emails)
    monkeypatch.setattr(
        enrichment,
        "enrich_channel",
        lambda channel: {
            "name": "Example",
            "emails": ["feed@example.com"],
            "email_gate_present": False,
            "status": "feed_unavailable",
            "status_reason": "Channel feed not available",
        },
    )

    manager = enrichment.EnrichmentManager()
    manager._process_channel_full(job, channel)

    # Expect status update during processing and final enrichment update.
    assert any(entry[0] == "update" for entry in updates)
    final_update = [entry for entry in updates if entry[0] == "update"][-1]
    fields = final_update[2]
    assert fields["status"] == "feed_unavailable"
    assert fields["status_reason"] == "Channel feed not available"
    assert fields["last_enriched_result"] == "emails_found"
    assert job.completed == 1
    assert job.errors == 0


def test_process_channel_invalid_reference(monkeypatch):
    job = enrichment.EnrichmentJob(job_id="job", channels=[{"channel_id": "bad"}])
    channel = {"channel_id": "bad", "status": "new"}
    updates = []

    def fake_set_status(channel_id, status, reason, timestamp):
        updates.append(("set_status", channel_id, status, reason, timestamp))

    def fake_update(channel_id, **fields):
        updates.append(("update", channel_id, fields))

    monkeypatch.setattr(enrichment.database, "set_channel_status", fake_set_status)
    monkeypatch.setattr(enrichment.database, "update_channel_enrichment", fake_update)
    monkeypatch.setattr(enrichment, "enrich_channel", lambda channel: (_ for _ in ()).throw(enrichment.EnrichmentError("invalid_channel")))

    manager = enrichment.EnrichmentManager()
    manager._process_channel_full(job, channel)

    error_update = [entry for entry in updates if entry[0] == "update"][-1]
    fields = error_update[2]
    assert fields["status"] == "invalid_channel"
    assert fields["status_reason"] == "invalid_channel"
    assert fields["last_enriched_result"] == "invalid_channel"
    assert job.completed == 1
    assert job.errors == 0
