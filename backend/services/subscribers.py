import os
import re
from typing import Optional

import requests

SUBSCRIBER_REGEXES = [
    re.compile(r"([0-9.,]+)\s*(subscribers|Abonnenten|Abonnés|suscriptores|seguidores)", re.IGNORECASE),
]


class SubscriberFetcher:
    def __init__(self) -> None:
        self.api_key = os.getenv("YT_API_KEY")

    def _fetch_via_api(self, channel_id: str) -> Optional[int]:
        if not self.api_key:
            return None
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "statistics",
            "id": channel_id,
            "key": self.api_key,
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            if not items:
                return None
            stats = items[0].get("statistics", {})
            subs = stats.get("subscriberCount")
            if subs is None:
                return None
            return int(subs)
        except Exception:
            return None

    def _parse_about_html(self, channel_id: str) -> Optional[int]:
        url = f"https://www.youtube.com/channel/{channel_id}/about"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except Exception:
            return None
        html = resp.text
        for regex in SUBSCRIBER_REGEXES:
            match = regex.search(html)
            if match:
                count_str = match.group(1).replace(",", "").replace(".", "")
                try:
                    return int(count_str)
                except ValueError:
                    continue
        return None

    def fetch(self, channel_id: str) -> Optional[int]:
        return self._fetch_via_api(channel_id) or self._parse_about_html(channel_id)
