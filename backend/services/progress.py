import asyncio
import json
import uuid
from typing import Any, AsyncGenerator, Dict


class ProgressManager:
    def __init__(self) -> None:
        self._subscribers: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def publish(self, event: Dict[str, Any]) -> None:
        payload = json.dumps(event)
        async with self._lock:
            subscribers = list(self._subscribers.values())
        for queue in subscribers:
            await queue.put(payload)

    async def subscribe(self) -> AsyncGenerator[str, None]:
        queue: asyncio.Queue = asyncio.Queue()
        subscriber_id = str(uuid.uuid4())
        async with self._lock:
            self._subscribers[subscriber_id] = queue
        try:
            while True:
                payload = await queue.get()
                yield f"data: {payload}\n\n"
        finally:
            async with self._lock:
                self._subscribers.pop(subscriber_id, None)

    async def flush(self) -> None:
        async with self._lock:
            for queue in self._subscribers.values():
                await queue.put(json.dumps({"event": "keepalive"}))
