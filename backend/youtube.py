"""Utilities for interacting with YouTube without requiring official APIs."""
from __future__ import annotations

import html
import json
import re
import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests
from langdetect import DetectorFactory, LangDetectException, detect_langs

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

DetectorFactory.seed = 0

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    }
)


class RateLimiter:
    """Simple thread-safe rate limiter based on a minimum interval."""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_time = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_time = self.min_interval - (now - self._last_time)
            if wait_time > 0:
                time.sleep(wait_time)
            self._last_time = time.monotonic()


RATE_LIMITER = RateLimiter(min_interval=0.35)  # ~3 requests per second globally

RSS_TEMPLATE = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
MEDIA_NS = "{http://search.yahoo.com/mrss/}"


class EnrichmentError(RuntimeError):
    """Raised when enrichment of a channel fails due to data issues."""



@dataclass
class ChannelSearchResult:
    channel_id: str
    title: str
    url: str
    subscribers: Optional[int]


def _extract_ytinitialdata(html: str) -> Optional[Dict]:
    patterns = [
        r"ytInitialData\s*=\s*(\{.*?\});",
        r"var ytInitialData\s*=\s*(\{.*?\});",
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            json_str = match.group(1)
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                continue
    return None


def _find_channel_renderers(data: Dict) -> Iterable[Dict]:
    stack = [data]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if "channelRenderer" in node:
                yield node["channelRenderer"]
            else:
                stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def _parse_subscriber_count(text: str) -> Optional[int]:
    text = text.replace(" subscribers", "").strip()
    multiplier = 1
    if text.endswith("K"):
        multiplier = 1_000
        text = text[:-1]
    elif text.endswith("M"):
        multiplier = 1_000_000
        text = text[:-1]
    elif text.endswith("B"):
        multiplier = 1_000_000_000
        text = text[:-1]
    text = text.replace(",", "")
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return None


def search_channels(keyword: str, limit: int) -> List[ChannelSearchResult]:
    params = {
        "search_query": keyword,
        "sp": "EgIQAg%3D%3D",  # channel filter
    }
    RATE_LIMITER.wait()
    response = SESSION.get("https://www.youtube.com/results", params=params, timeout=10)
    response.raise_for_status()
    data = _extract_ytinitialdata(response.text)
    if not data:
        return []

    results: List[ChannelSearchResult] = []
    for renderer in _find_channel_renderers(data):
        channel_id = renderer.get("channelId")
        if not channel_id:
            continue
        title_runs = renderer.get("title", {}).get("runs", [])
        title = title_runs[0]["text"] if title_runs else renderer.get("title", {}).get("simpleText", "")
        nav = renderer.get("navigationEndpoint", {}).get("browseEndpoint", {})
        canonical = nav.get("canonicalBaseUrl")
        if canonical:
            url = f"https://www.youtube.com{canonical}"
        elif channel_id:
            url = f"https://www.youtube.com/channel/{channel_id}"
        else:
            continue
        sub_text_obj = renderer.get("subscriberCountText", {})
        if "simpleText" in sub_text_obj:
            subscribers = _parse_subscriber_count(sub_text_obj["simpleText"])
        else:
            runs = sub_text_obj.get("runs", [])
            subscribers = _parse_subscriber_count(runs[0]["text"]) if runs else None
        results.append(ChannelSearchResult(channel_id=channel_id, title=title, url=url, subscribers=subscribers))
        if len(results) >= limit:
            break

    return results


def detect_language(text: str) -> Optional[Dict[str, float]]:
    cleaned = text.strip()
    if not cleaned:
        return None
    try:
        langs = detect_langs(cleaned)
    except LangDetectException:
        return None
    if not langs:
        return None
    best = langs[0]
    return {"language": best.lang, "confidence": float(best.prob)}


def extract_emails(texts: Iterable[str]) -> List[str]:
    pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
    found: List[str] = []
    for text in texts:
        if not text:
            continue
        found.extend(pattern.findall(text))
    unique = []
    seen = set()
    for email in found:
        email_lower = email.lower()
        if email_lower not in seen:
            unique.append(email)
            seen.add(email_lower)
    return unique


def _fetch_rss(channel_id: str) -> Tuple[str, Optional[str], Dict[str, Optional[str]]]:
    RATE_LIMITER.wait()
    response = SESSION.get(RSS_TEMPLATE.format(channel_id=channel_id), timeout=8)
    if response.status_code == 404:
        raise EnrichmentError("Channel feed not available")
    response.raise_for_status()

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as exc:  # pragma: no cover - network artifact
        raise EnrichmentError(f"Malformed channel feed: {exc}")

    title = root.findtext("atom:title", default="", namespaces=ATOM_NS)
    description = root.findtext(f"atom:subtitle", default="", namespaces=ATOM_NS)
    entry = root.find("atom:entry", ATOM_NS)
    if entry is None:
        raise EnrichmentError("No public videos found in feed")

    video_id = entry.findtext("yt:videoId", default="", namespaces=ATOM_NS)
    if not video_id:
        raise EnrichmentError("Unable to read latest video id")

    media_group = entry.find(f"{MEDIA_NS}group")
    video_title = media_group.findtext(f"{MEDIA_NS}title", default="") if media_group is not None else ""
    video_description = (
        media_group.findtext(f"{MEDIA_NS}description", default="") if media_group is not None else ""
    )
    updated = entry.findtext("atom:updated", default="", namespaces=ATOM_NS)
    published = entry.findtext("atom:published", default="", namespaces=ATOM_NS)

    return title or "", description or None, {
        "video_id": video_id,
        "title": video_title.strip(),
        "description": video_description.strip(),
        "timestamp": updated or published,
    }


def _extract_json_blob(html_text: str, marker: str) -> Optional[Dict]:
    pattern = re.compile(rf"{marker}\s*=\s*(\{{.*?\}});", re.DOTALL)
    match = pattern.search(html_text)
    if not match:
        return None
    json_text = match.group(1)
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        return None


def _find_first(node: object, key: str) -> Optional[Dict]:
    if isinstance(node, dict):
        if key in node:
            return node[key]
        for value in node.values():
            found = _find_first(value, key)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_first(item, key)
            if found is not None:
                return found
    return None


def _fetch_watch_details(video_id: str) -> Dict[str, Optional[str]]:
    RATE_LIMITER.wait()
    response = SESSION.get(
        "https://www.youtube.com/watch",
        params={"v": video_id},
        timeout=10,
    )
    if response.status_code == 429:
        raise EnrichmentError("Rate limited by YouTube")
    if response.status_code == 410:
        raise EnrichmentError("Video is no longer available")
    response.raise_for_status()

    html_text = response.text
    player = _extract_json_blob(html_text, "ytInitialPlayerResponse")
    data = _extract_json_blob(html_text, "ytInitialData")
    if not player:
        raise EnrichmentError("Unable to parse video metadata")

    video_details = player.get("videoDetails", {}) if isinstance(player, dict) else {}
    short_description = html.unescape(video_details.get("shortDescription", ""))

    microformat = player.get("microformat", {}) if isinstance(player, dict) else {}
    micro_renderer = microformat.get("playerMicroformatRenderer", {})
    language_hint = micro_renderer.get("language")
    upload_date = micro_renderer.get("uploadDate")

    owner_renderer = _find_first(data, "videoOwnerRenderer") if data else None
    subscriber_count = None
    if owner_renderer:
        sub_text = owner_renderer.get("subscriberCountText", {})
        if isinstance(sub_text, dict):
            if "simpleText" in sub_text:
                subscriber_count = _parse_subscriber_count(sub_text["simpleText"])
            else:
                runs = sub_text.get("runs", [])
                if runs:
                    subscriber_count = _parse_subscriber_count(runs[0].get("text", ""))

    return {
        "description": short_description,
        "language": language_hint,
        "upload_date": upload_date,
        "subscribers": subscriber_count,
    }


def enrich_channel(channel: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
    channel_id = channel.get("channel_id")
    if not channel_id:
        raise EnrichmentError("Missing channel id")

    feed_title, feed_description, video = _fetch_rss(channel_id)
    watch = _fetch_watch_details(video["video_id"])

    combined_description = watch.get("description") or video.get("description") or ""
    combined_texts = [video.get("title", ""), combined_description, feed_description or ""]
    lang_result = detect_language("\n".join(filter(None, combined_texts)))

    emails = extract_emails([combined_description])
    if feed_description:
        emails.extend(extract_emails([feed_description]))
    # Deduplicate again after combining feed and watch descriptions.
    unique_emails = []
    seen = set()
    for email in emails:
        lower = email.lower()
        if lower not in seen:
            unique_emails.append(email)
            seen.add(lower)
        if len(unique_emails) >= 5:
            break

    return {
        "title": feed_title or channel.get("title"),
        "subscribers": watch.get("subscribers"),
        "language": lang_result["language"] if lang_result else (watch.get("language") or None),
        "language_confidence": lang_result["confidence"] if lang_result else None,
        "emails": unique_emails,
        "last_updated": watch.get("upload_date") or video.get("timestamp"),
    }
