from typing import Dict, Iterable, Tuple

from yt_dlp import YoutubeDL

from ..db import get_session
from ..models import Channel
from .progress import ProgressManager
from .util import now_ts, rate_limited_sleep

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


def _channel_from_entry(entry: Dict) -> Tuple[str, str]:
    channel_id = entry.get("channel_id") or entry.get("uploader_id")
    channel_name = entry.get("channel") or entry.get("uploader") or "Unknown"
    if channel_id and channel_id.startswith("UC"):
        return channel_id, channel_name
    return "", channel_name


async def discover_channels(
    *,
    keywords: Iterable[str],
    per_keyword: int,
    progress: ProgressManager,
    rate_sleep: float,
) -> Dict[str, int]:
    found_total = 0
    unique_channels = set()

    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
    }

    for keyword in keywords:
        query = f"ytsearch{per_keyword}:{keyword}"
        await progress.publish(
            {
                "stage": "discover",
                "keyword": keyword,
                "status": "searching",
            }
        )
        try:
            with YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(query, download=False)
        except Exception as exc:
            await progress.publish(
                {
                    "stage": "discover",
                    "keyword": keyword,
                    "status": "error",
                    "detail": str(exc),
                }
            )
            continue

        entries = info.get("entries", []) if isinstance(info, dict) else []
        found_total += len(entries)
        for entry in entries:
            channel_id, channel_name = _channel_from_entry(entry)
            if not channel_id:
                continue
            unique_channels.add((channel_id, channel_name))
        await rate_limited_sleep(rate_sleep)

    now = now_ts()
    inserted = 0
    with get_session() as session:
        for channel_id, channel_name in unique_channels:
            channel_url = f"https://www.youtube.com/channel/{channel_id}"
            existing = session.get(Channel, channel_id)
            if existing:
                existing.channel_name = channel_name or existing.channel_name
                existing.update_timestamps(seen=now)
                continue
            channel = Channel(
                channel_id=channel_id,
                channel_name=channel_name,
                channel_url=channel_url,
                first_seen=now,
                last_updated=now,
            )
            session.add(channel)
            inserted += 1

    await progress.publish(
        {
            "stage": "discover",
            "status": "completed",
            "found": found_total,
            "unique": len(unique_channels),
            "inserted": inserted,
        }
    )

    return {"found": found_total, "uniqueTotal": len(unique_channels)}
