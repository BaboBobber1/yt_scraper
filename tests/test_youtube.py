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
