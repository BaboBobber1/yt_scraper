from typing import List, Optional

import requests
from yt_dlp import YoutubeDL

from .util import extract_emails


def fetch_about_text(channel_id: str) -> str:
    url = f"https://www.youtube.com/channel/{channel_id}/about"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception:
        return ""
    return resp.text


def fetch_latest_video_description(channel_id: str) -> Optional[str]:
    channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
    options = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": "discard_in_playlist",
    }
    try:
        with YoutubeDL(options) as ydl:
            info = ydl.extract_info(channel_url, download=False)
    except Exception:
        return None
    entries = info.get("entries", []) if isinstance(info, dict) else []
    if not entries:
        return None
    first_video = entries[0]
    video_id = first_video.get("id")
    if not video_id:
        return None
    try:
        with YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
            video_info = ydl.extract_info(video_id, download=False)
    except Exception:
        return None
    return video_info.get("description")


def collect_emails(channel_id: str) -> List[str]:
    about_text = fetch_about_text(channel_id)
    latest_description = fetch_latest_video_description(channel_id) or ""
    emails = extract_emails(about_text)
    emails.extend(extract_emails(latest_description))
    return sorted(set(emails))
