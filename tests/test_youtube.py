import json
from typing import Any, Dict, Iterator, List

import pytest
import requests

from backend import youtube


class DummyResponse:
    def __init__(self, status_code: int, text: str = "", url: str = "https://example.com"):
        self.status_code = status_code
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


@pytest.fixture(autouse=True)
def _no_rate_limit(monkeypatch):
    monkeypatch.setattr(youtube.RATE_LIMITER, "wait", lambda: None)


def _build_playlist_payload(video_id: str = "abc123") -> Dict[str, Any]:
    return {
        "metadata": {
            "playlistMetadataRenderer": {
                "title": "Uploads from Example",
            }
        },
        "contents": {
            "twoColumnBrowseResultsRenderer": {
                "tabs": [
                    {
                        "tabRenderer": {
                            "content": {
                                "sectionListRenderer": {
                                    "contents": [
                                        {
                                            "itemSectionRenderer": {
                                                "contents": [
                                                    {
                                                        "playlistVideoListRenderer": {
                                                            "contents": [
                                                                {
                                                                    "playlistVideoRenderer": {
                                                                        "videoId": video_id,
                                                                        "title": {"simpleText": "First video"},
                                                                        "descriptionSnippet": {
                                                                            "runs": [{"text": "Example description"}]
                                                                        },
                                                                        "publishedTimeText": {
                                                                            "simpleText": "1 day ago"
                                                                        },
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
    }


def _playlist_html(payload: Dict[str, Any]) -> str:
    return f"<script>var ytInitialData = {json.dumps(payload)};</script>"


def test_fetch_rss_fallback_uses_playlist(monkeypatch):
    responses: Iterator[DummyResponse] = iter(
        [
            DummyResponse(status_code=404),
            DummyResponse(status_code=200, text=_playlist_html(_build_playlist_payload())),
        ]
    )

    def fake_get(url: str, timeout: int, **_: Any) -> DummyResponse:
        try:
            response = next(responses)
        except StopIteration:  # pragma: no cover - defensive
            raise AssertionError("Unexpected request")
        response.url = url
        return response

    monkeypatch.setattr(youtube.SESSION, "get", fake_get)

    title, description, video = youtube._fetch_rss("UC1234567890123456789012")

    assert title == "Uploads from Example"
    assert description == "Example description"
    assert video["video_id"] == "abc123"
    assert video["title"] == "First video"
    assert video["timestamp"] == "1 day ago"


def test_fetch_rss_fallback_raises_when_playlist_empty(monkeypatch):
    responses: Iterator[DummyResponse] = iter(
        [
            DummyResponse(status_code=404),
            DummyResponse(status_code=200, text=_playlist_html({})),
        ]
    )

    def fake_get(url: str, timeout: int, **_: Any) -> DummyResponse:
        try:
            response = next(responses)
        except StopIteration:  # pragma: no cover - defensive
            raise AssertionError("Unexpected request")
        response.url = url
        return response

    monkeypatch.setattr(youtube.SESSION, "get", fake_get)

    with pytest.raises(youtube.EnrichmentError):
        youtube._fetch_rss("UC1234567890123456789012")


def test_enrich_channel_with_feed_success(monkeypatch):
    captured: List[str] = []

    def fake_fetch_rss(channel_id: str, timeout: int = 8):
        captured.append(channel_id)
        return (
            "Feed Title",
            "Feed Description",
            {
                "video_id": "VID123",
                "title": "Latest upload",
                "description": "Watch description",
                "timestamp": "2024-01-01",
            },
        )

    def fake_fetch_watch(video_id: str, timeout: int = 10):
        assert video_id == "VID123"
        return {
            "description": "Watch long description",
            "language": "en",
            "upload_date": "2024-01-02",
            "subscribers": 12345,
        }

    def fake_about(channel: Dict[str, Any], timeout: int = 5):
        assert channel["channel_id"] == "UC1234567890123456789012"
        return ["contact@example.com"], False

    monkeypatch.setattr(youtube, "_fetch_rss", fake_fetch_rss)
    monkeypatch.setattr(youtube, "_fetch_watch_details", fake_fetch_watch)
    monkeypatch.setattr(youtube, "_fetch_about_emails", fake_about)
    monkeypatch.setattr(
        youtube,
        "detect_language",
        lambda text: {"language": "en", "confidence": 0.9},
    )

    result = youtube.enrich_channel({"channel_id": "UC1234567890123456789012"})

    assert captured == ["UC1234567890123456789012"]
    assert result["status"] == "completed"
    assert result["emails"] == ["contact@example.com"]
    assert result["last_updated"] == "2024-01-02"
    assert result["language"] == "en"


def test_enrich_channel_resolves_handle(monkeypatch):
    resolutions: List[str] = []

    def fake_resolve(value: str, *, timeout: int = 8):
        resolutions.append(value)
        return (
            youtube.ChannelResolution(
                channel_id="UC9999999999999999999999",
                canonical_url="https://www.youtube.com/channel/UC9999999999999999999999",
                handle="@example",
                title="Example Channel",
            ),
            None,
        )

    captured: List[str] = []

    def fake_fetch_rss(channel_id: str, timeout: int = 8):
        captured.append(channel_id)
        return (
            "Example Channel",
            None,
            {
                "video_id": "VIDHANDLE",
                "title": "Handle video",
                "description": "Handle description",
                "timestamp": "2024-02-01",
            },
        )

    def fake_fetch_watch(video_id: str, timeout: int = 10):
        assert video_id == "VIDHANDLE"
        return {
            "description": "Handle watch",
            "language": "en",
            "upload_date": "2024-02-02",
            "subscribers": 42,
        }

    def fake_about(channel: Dict[str, Any], timeout: int = 5):
        assert channel["channel_id"] == "UC9999999999999999999999"
        return ["handle@example.com"], False

    monkeypatch.setattr(youtube, "resolve_channel", fake_resolve)
    monkeypatch.setattr(youtube, "_fetch_rss", fake_fetch_rss)
    monkeypatch.setattr(youtube, "_fetch_watch_details", fake_fetch_watch)
    monkeypatch.setattr(youtube, "_fetch_about_emails", fake_about)
    monkeypatch.setattr(
        youtube,
        "detect_language",
        lambda text: {"language": "en", "confidence": 0.7},
    )

    result = youtube.enrich_channel({"channel_id": "@example"})

    assert "@example" in resolutions
    assert captured == ["UC9999999999999999999999"]
    assert result["emails"] == ["handle@example.com"]
    assert result["status"] == "completed"


def test_enrich_channel_feed_unavailable(monkeypatch):
    def fake_fetch_rss(channel_id: str, timeout: int = 8):
        raise youtube.EnrichmentError("Channel feed not available")

    def fake_about(channel: Dict[str, Any], timeout: int = 5):
        assert channel["channel_id"] == "UC1234567890123456789012"
        return ["fallback@example.com"], False

    def fail_watch(video_id: str, timeout: int = 10):  # pragma: no cover - should not run
        raise AssertionError("Watch details should not be fetched when feed is unavailable")

    monkeypatch.setattr(youtube, "_fetch_rss", fake_fetch_rss)
    monkeypatch.setattr(youtube, "_fetch_watch_details", fail_watch)
    monkeypatch.setattr(youtube, "_fetch_about_emails", fake_about)
    monkeypatch.setattr(youtube, "detect_language", lambda text: None)

    result = youtube.enrich_channel({"channel_id": "UC1234567890123456789012"})

    assert result["status"] == "feed_unavailable"
    assert result["status_reason"] == "Channel feed not available"
    assert result["emails"] == ["fallback@example.com"]
    assert result["last_updated"] is None


def test_enrich_channel_invalid_reference(monkeypatch):
    def fake_resolve(value: str, *, timeout: int = 8):
        return (None, "not_found")

    monkeypatch.setattr(youtube, "resolve_channel", fake_resolve)

    with pytest.raises(youtube.EnrichmentError) as exc:
        youtube.enrich_channel({"channel_id": None, "url": "https://www.youtube.com/c/does-not-exist"})

    assert str(exc.value) == "invalid_channel"
