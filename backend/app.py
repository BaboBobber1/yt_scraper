import csv
import io
import os
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import or_, select

from .db import Base, engine, get_session
from .models import Channel
from .services.discover import DEFAULT_KEYWORDS, discover_channels
from .services.enrich import EnrichmentService
from .services.progress import ProgressManager

load_dotenv()

app = FastAPI(title="Crypto YouTube Harvester")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

RATE_SLEEP = float(os.getenv("RATE_SLEEP", "1.0"))
progress_manager = ProgressManager()


class DiscoverRequest(BaseModel):
    keywords: Optional[List[str]] = Field(default=None)
    perKeyword: Optional[int] = Field(default=100, ge=1, le=200)


class DiscoverResponse(BaseModel):
    found: int
    uniqueTotal: int


class EnrichRequest(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1)


class EnrichResponse(BaseModel):
    processed: int


class ChannelResponse(BaseModel):
    channel_id: str
    channel_name: str
    channel_url: str
    subscribers: Optional[int]
    detected_language: Optional[str]
    lang_confidence: Optional[float]
    emails: Optional[str]
    sampled_videos: Optional[int]
    last_updated: Optional[int]

    @classmethod
    def from_model(cls, model: Channel) -> "ChannelResponse":
        return cls(
            channel_id=model.channel_id,
            channel_name=model.channel_name,
            channel_url=model.channel_url,
            subscribers=model.subscribers,
            detected_language=model.detected_language,
            lang_confidence=model.lang_confidence,
            emails=model.emails,
            sampled_videos=model.sampled_videos,
            last_updated=model.last_updated,
        )


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)


@app.post("/api/discover", response_model=DiscoverResponse)
async def discover_endpoint(payload: DiscoverRequest) -> DiscoverResponse:
    keywords = payload.keywords or DEFAULT_KEYWORDS
    per_keyword = payload.perKeyword or 100
    result = await discover_channels(
        keywords=keywords,
        per_keyword=per_keyword,
        progress=progress_manager,
        rate_sleep=RATE_SLEEP,
    )
    return DiscoverResponse(**result)


@app.post("/api/enrich", response_model=EnrichResponse)
async def enrich_endpoint(payload: EnrichRequest) -> EnrichResponse:
    service = EnrichmentService(progress=progress_manager, rate_sleep=RATE_SLEEP)
    result = await service.enrich_channels(limit=payload.limit)
    return EnrichResponse(**result)


@app.get("/api/progress")
async def progress_stream() -> StreamingResponse:
    async def event_generator():
        async for event in progress_manager.subscribe():
            yield event

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/channels", response_model=List[ChannelResponse])
def list_channels(
    q: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> List[ChannelResponse]:
    with get_session() as session:
        query = select(Channel)
        if q:
            like = f"%{q}%"
            query = query.filter(
                or_(Channel.channel_name.ilike(like), Channel.emails.ilike(like))
            )
        query = query.offset(offset).limit(limit)
        models = session.execute(query).scalars().all()
    return [ChannelResponse.from_model(model) for model in models]


@app.get("/api/export/csv")
def export_csv() -> StreamingResponse:
    with get_session() as session:
        channels = session.execute(select(Channel)).scalars().all()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "channel_id",
            "channel_name",
            "channel_url",
            "subscribers",
            "detected_language",
            "lang_confidence",
            "emails",
            "sampled_videos",
            "first_seen",
            "last_updated",
        ]
    )
    for channel in channels:
        writer.writerow(
            [
                channel.channel_id,
                channel.channel_name,
                channel.channel_url,
                channel.subscribers or "",
                channel.detected_language or "",
                channel.lang_confidence or "",
                channel.emails or "",
                channel.sampled_videos or "",
                channel.first_seen or "",
                channel.last_updated or "",
            ]
        )
    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=channels_enriched.csv"}
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv", headers=headers
    )
