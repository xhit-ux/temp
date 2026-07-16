from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import aiohttp

LOGGER = logging.getLogger(__name__)

# Default maximum retries with exponential backoff: 5 retries
# Delays: 1s -> 2s -> 4s -> 8s -> 16s (with jitter)
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_DELAY = 1.0
DEFAULT_MAX_DELAY = 30.0

# Default local fallback directory for events that fail after all retries
DEFAULT_DEAD_LETTER_DIR = Path("data/dead-letter")

# HTTP timeout settings
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_REQUEST_TIMEOUT = 10.0


class BackoffStrategy:
    """Exponential backoff with random jitter.

    Delays follow: base_delay * 2^attempt + random jitter.
    """

    def __init__(
        self,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
    ) -> None:
        self.base_delay = base_delay
        self.max_delay = max_delay

    def delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number (1-based)."""
        raw = self.base_delay * (2 ** (attempt - 1))
        capped = min(raw, self.max_delay)
        # Add ±25% jitter to avoid thundering herd
        jitter = capped * 0.25 * random.uniform(-1.0, 1.0)
        return max(0.0, capped + jitter)


class DeadLetterWriter:
    """Writes failed events to local NDJSON files for later compensation.

    Files are organized by date and stored in NDJSON format (one JSON object per line).
    """

    def __init__(self, base_dir: Path = DEFAULT_DEAD_LETTER_DIR) -> None:
        self.base_dir = Path(base_dir)
        self._lock = asyncio.Lock()

    def _file_path(self) -> Path:
        """Generate a date-based file path."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        return self.base_dir / f"dlq-{date_str}.ndjson"

    async def write(self, events: Sequence[dict[str, Any]], reason: str) -> None:
        """Append failed events to the dead letter file.

        Each record includes metadata: reason, retry_count, first_failure_at.

        Args:
            events: List of event dicts that failed to send.
            reason: Human-readable failure reason.
        """
        if not events:
            return

        filepath = self._file_path()
        now = datetime.now(timezone.utc).isoformat()
        records = []

        for event in events:
            record = {
                **event,
                "dlq_reason": reason,
                "dlq_retry_count": event.get("_retry_count", 0),
                "dlq_first_failure_at": event.get("_first_failure_at", now),
                "dlq_written_at": now,
            }
            # Remove internal tracking fields from the event body
            record.pop("_retry_count", None)
            record.pop("_first_failure_at", None)
            records.append(json.dumps(record, ensure_ascii=False) + "\n")

        async with self._lock:
            with open(filepath, "a", encoding="utf-8") as fp:
                fp.writelines(records)

        LOGGER.warning(
            "Wrote %d events to dead letter file %s (reason: %s)",
            len(records),
            filepath,
            reason,
        )


class EventSender:
    """HTTP sender for submitting events to the Go backend.

    Features:
    - Batch or single event submission to /api/v1/ingest/events
    - Exponential backoff with jitter for retries
    - Local dead letter file fallback when all retries exhausted
    - Respects 429/503 backpressure responses
    """

    def __init__(
        self,
        backend_url: str,
        dead_letter_dir: Path = DEFAULT_DEAD_LETTER_DIR,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        # Normalize URL: strip trailing slash
        self.backend_url = backend_url.rstrip("/")
        self.max_retries = max_retries
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.backoff = BackoffStrategy(base_delay=base_delay)
        self.dead_letter = DeadLetterWriter(base_dir=dead_letter_dir)

    async def send_event(self, event: dict[str, Any]) -> bool:
        """Send a single event to the backend. Returns True on success."""
        return await self.send_events([event])

    async def send_events(self, events: list[dict[str, Any]]) -> bool:
        """Send a batch of events to the backend with retry logic.

        Returns True if all events were successfully sent.
        Returns False if events were written to dead letter after all retries.
        """
        if not events:
            return True

        payload = json.dumps(events, ensure_ascii=False)
        endpoint = f"{self.backend_url}/api/v1/ingest/events"
        last_error: Optional[str] = None

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(
                        connect=self.connect_timeout,
                        total=self.request_timeout,
                    )
                ) as session:
                    async with session.post(
                        endpoint,
                        data=payload,
                        headers={"Content-Type": "application/json"},
                    ) as response:
                        if response.status in (200, 201, 204):
                            # Log at INFO so throughput is visible; uses modulo to avoid log flood
                            if attempt == 1 and len(events) > 0:
                                LOGGER.info("Sent %d events successfully", len(events))
                            else:
                                LOGGER.debug("Sent %d events successfully (attempt %d)", len(events), attempt)
                            return True

                        # Backpressure: queue full or service unavailable
                        if response.status in (429, 503):
                            LOGGER.warning(
                                "Backend returned %d, backing off (attempt %d/%d)",
                                response.status,
                                attempt,
                                self.max_retries,
                            )
                            last_error = f"HTTP {response.status}: backend busy"
                        else:
                            body = await response.text()
                            LOGGER.warning(
                                "Backend returned %d: %s (attempt %d/%d)",
                                response.status,
                                body[:200],
                                attempt,
                                self.max_retries,
                            )
                            last_error = f"HTTP {response.status}: {body[:200]}"

            except asyncio.TimeoutError:
                LOGGER.warning("Request timed out (attempt %d/%d)", attempt, self.max_retries)
                last_error = "request timeout"
            except aiohttp.ClientConnectionError as exc:
                LOGGER.warning("Connection error: %s (attempt %d/%d)", exc, attempt, self.max_retries)
                last_error = f"connection error: {exc}"
            except aiohttp.ClientError as exc:
                LOGGER.warning("HTTP client error: %s (attempt %d/%d)", exc, attempt, self.max_retries)
                last_error = f"client error: {exc}"
            except Exception as exc:
                LOGGER.exception("Unexpected error sending events (attempt %d/%d)", attempt, self.max_retries)
                last_error = f"unexpected: {exc}"

            # Don't sleep after the last attempt
            if attempt < self.max_retries:
                delay = self.backoff.delay(attempt)
                LOGGER.debug("Retrying in %.2fs", delay)
                await asyncio.sleep(delay)

        # All retries exhausted — write to dead letter
        LOGGER.error(
            "Failed to send %d events after %d retries. Writing to dead letter.",
            len(events),
            self.max_retries,
        )
        await self.dead_letter.write(events, last_error or "unknown error")
        return False