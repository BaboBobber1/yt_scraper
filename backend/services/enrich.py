from typing import Dict, List, Optional

from sqlalchemy import select

from ..db import get_session
from ..models import Channel
from .emails import collect_emails
from .language import detect_channel_language, ffmpeg_available
from .progress import ProgressManager
from .subscribers import SubscriberFetcher
from .util import rate_limited_sleep


class EnrichmentService:
    def __init__(self, progress: ProgressManager, rate_sleep: float) -> None:
        self.progress = progress
        self.rate_sleep = rate_sleep
        self.subscriber_fetcher = SubscriberFetcher()
        self._ffmpeg_checked = False

    async def _publish(self, event: Dict) -> None:
        await self.progress.publish(event)

    async def enrich_channels(self, limit: Optional[int] = None) -> Dict[str, int]:
        enriched = 0
        with get_session() as session:
            query = select(Channel)
            if limit:
                query = query.limit(limit)
            channels: List[Channel] = session.execute(query).scalars().all()

        if not self._ffmpeg_checked:
            if not ffmpeg_available():
                await self._publish(
                    {
                        "stage": "enrich",
                        "status": "warning",
                        "detail": "ffmpeg not found. Audio sampling disabled.",
                    }
                )
            self._ffmpeg_checked = True

        for channel in channels:
            await self._publish(
                {
                    "stage": "enrich",
                    "channel_id": channel.channel_id,
                    "status": "processing",
                }
            )
            try:
                subscribers = self.subscriber_fetcher.fetch(channel.channel_id)
                lang, confidence, sampled_videos = detect_channel_language(
                    channel.channel_id, self.rate_sleep
                )
                emails = collect_emails(channel.channel_id)
                channel.subscribers = subscribers
                channel.detected_language = lang
                channel.lang_confidence = confidence
                channel.emails = ",".join(emails) if emails else None
                channel.sampled_videos = sampled_videos
                channel.update_timestamps()
                enriched += 1
                await self._publish(
                    {
                        "stage": "enrich",
                        "channel_id": channel.channel_id,
                        "status": "completed",
                        "subscribers": subscribers,
                        "language": lang,
                    }
                )
            except Exception as exc:
                await self._publish(
                    {
                        "stage": "enrich",
                        "channel_id": channel.channel_id,
                        "status": "error",
                        "detail": str(exc),
                    }
                )
            finally:
                await rate_limited_sleep(self.rate_sleep)
                with get_session() as session:
                    db_channel = session.get(Channel, channel.channel_id)
                    if db_channel:
                        db_channel.subscribers = channel.subscribers
                        db_channel.detected_language = channel.detected_language
                        db_channel.lang_confidence = channel.lang_confidence
                        db_channel.emails = channel.emails
                        db_channel.sampled_videos = channel.sampled_videos
                        db_channel.last_updated = channel.last_updated
        return {"processed": enriched}
