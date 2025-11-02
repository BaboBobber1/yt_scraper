import time
from typing import Optional

from sqlalchemy import Column, Float, Integer, String, Text

from .db import Base


def _timestamp() -> int:
    return int(time.time())


class Channel(Base):
    __tablename__ = "channels"

    channel_id = Column(String, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_url = Column(String, nullable=False)
    subscribers = Column(Integer, nullable=True)
    detected_language = Column(String, nullable=True)
    lang_confidence = Column(Float, nullable=True)
    emails = Column(Text, nullable=True)
    sampled_videos = Column(Integer, default=0)
    first_seen = Column(Integer, default=_timestamp)
    last_updated = Column(Integer, default=_timestamp)

    def update_timestamps(self, *, seen: Optional[int] = None) -> None:
        now = seen or int(time.time())
        if not self.first_seen:
            self.first_seen = now
        self.last_updated = now
