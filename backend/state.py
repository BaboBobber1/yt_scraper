"""Runtime state tracking helpers for discovery loop orchestration."""
from __future__ import annotations

import datetime as dt
import threading
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


def _utcnow_iso() -> str:
    """Return a UTC ISO timestamp without microseconds for consistency."""
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()


def _sanitize_count(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


@dataclass
class DiscoveryLoopState:
    """Mutable snapshot of the discovery loop lifecycle."""

    running: bool = False
    stop_requested: bool = False
    runs: int = 0
    discovered: int = 0
    last_started_at: Optional[str] = None
    last_completed_at: Optional[str] = None
    updated_at: Optional[str] = None
    version: int = 0
    last_reason: Optional[str] = None
    last_error: Optional[str] = None


class DiscoveryStateManager:
    """Thread-safe coordinator for discovery loop status."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = DiscoveryLoopState()

    def mark_started(self, *, runs: int = 0, discovered: int = 0) -> Dict[str, Any]:
        now = _utcnow_iso()
        with self._lock:
            self._state.running = True
            self._state.stop_requested = False
            self._state.runs = _sanitize_count(runs)
            self._state.discovered = _sanitize_count(discovered)
            self._state.last_started_at = now
            self._state.updated_at = now
            self._state.version += 1
            self._state.last_reason = None
            self._state.last_error = None
            return asdict(self._state)

    def update_progress(self, *, runs: int, discovered: int) -> Dict[str, Any]:
        now = _utcnow_iso()
        with self._lock:
            self._state.runs = _sanitize_count(runs)
            self._state.discovered = _sanitize_count(discovered)
            self._state.updated_at = now
            self._state.version += 1
            return asdict(self._state)

    def request_stop(self) -> Dict[str, Any]:
        now = _utcnow_iso()
        with self._lock:
            if self._state.running:
                self._state.stop_requested = True
                self._state.updated_at = now
                self._state.version += 1
            return asdict(self._state)

    def mark_completed(
        self,
        *,
        runs: int,
        discovered: int,
        reason: Optional[str] = None,
        error: bool = False,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utcnow_iso()
        with self._lock:
            stop_requested = self._state.stop_requested
            derived_reason: Optional[str]
            if reason:
                derived_reason = reason
            elif error:
                derived_reason = "error"
            elif stop_requested:
                derived_reason = "stopped"
            else:
                derived_reason = "completed"
            self._state.running = False
            self._state.stop_requested = False
            self._state.runs = _sanitize_count(runs)
            self._state.discovered = _sanitize_count(discovered)
            self._state.last_completed_at = now
            self._state.updated_at = now
            self._state.version += 1
            self._state.last_reason = derived_reason
            self._state.last_error = message if error else None
            return asdict(self._state)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return asdict(self._state)


discovery_state = DiscoveryStateManager()
