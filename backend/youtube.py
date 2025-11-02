"""Utilities for interacting with YouTube without requiring official APIs."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests
from langdetect import DetectorFactory, LangDetectException, detect_langs
from yt_dlp import YoutubeDL

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

DetectorFactory.seed = 0


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
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = requests.get("https://www.youtube.com/results", params=params, headers=headers, timeout=15)
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


def _collect_texts(entries: Iterable[Dict]) -> str:
    texts: List[str] = []
    for entry in entries:
        title = entry.get("title")
        description = entry.get("description")
        if title:
            texts.append(title)
        if description:
            texts.append(description)
    return " \n".join(texts)


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


def enrich_channel(url: str) -> Dict[str, Optional[str]]:
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "ignoreerrors": True,
        "extract_flat": "discard_in_playlist",
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    subscriber_count = info.get("subscriber_count") if isinstance(info, dict) else None
    description = info.get("description") if isinstance(info, dict) else None
    entries = info.get("entries", []) if isinstance(info, dict) else []

    texts = _collect_texts(entries)
    combined_text = "\n".join(filter(None, [description, texts]))
    lang_result = detect_language(combined_text)
    emails = extract_emails([description or "", texts])

    latest_updated = info.get("modified_date") or info.get("upload_date")

    return {
        "title": info.get("title") if isinstance(info, dict) else None,
        "subscribers": subscriber_count,
        "language": lang_result["language"] if lang_result else None,
        "language_confidence": lang_result["confidence"] if lang_result else None,
        "emails": emails,
        "last_updated": latest_updated,
        "description": description,
        "videos": entries,
    }
