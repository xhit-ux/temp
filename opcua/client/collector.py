from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any, Optional

from asyncua import Client, ua

# Import from sibling packages
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from server.config_loader import load_config  # type: ignore[import-untyped]
from client.event_mapper import EventMapper  # type: ignore[import-untyped]
from client.sender import EventSender, DeadLetterWriter  # type: ignore[import-untyped]

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration defaults (overridable via CLI)
# ---------------------------------------------------------------------------
# Read backend host/port from environment for the default backend URL
# Falls back to localhost:4048 if env vars are not set.
_BACKEND_HOST = os.environ.get("SERVER_HOST", "localhost")
_BACKEND_PORT = os.environ.get("SERVER_PORT", "4048")
DEFAULT_BACKEND_URL = f"http://{_BACKEND_HOST}:{_BACKEND_PORT}"
DEFAULT_QUEUE_SIZE = 10_000
DEFAULT_SEND_BATCH_SIZE = 50
DEFAULT_SEND_INTERVAL_SECONDS = 0.5
DEFAULT_RECONNECT_BASE_DELAY = 2.0
DEFAULT_RECONNECT_MAX_DELAY = 60.0
DEFAULT_PUBLISHING_INTERVAL_MS = 500.0
DEFAULT_DEVICE_STALE_TIMEOUT = 30.0

# ---------------------------------------------------------------------------
# Subscription handler — runs inside OPC UA notification thread
# MUST NOT perform blocking I/O, HTTP requests, or DB operations
# ---------------------------------------------------------------------------


class SubscriptionHandler:
    """Receives DataChange notifications from OPC UA subscription.

    This handler runs in the asyncua event loop thread.
    To avoid blocking the notification path, it pushes events into an
    asyncio.Queue via call_soon_threadsafe (or directly if on same loop).

    It also tracks the last-seen timestamp per device for stale detection.
    """

    def __init__(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        event_loop: asyncio.AbstractEventLoop,
        device_configs: dict[str, Any],
        namespace_idx: int,
    ) -> None:
        self.queue = queue
        self.loop = event_loop
        self.device_configs = device_configs
        self.namespace_idx = namespace_idx
        # device_id -> last_seen_monotonic
        self.last_seen: dict[str, float] = {}
        self.count = 0
        self._node_id_to_device_point: dict[str, tuple[str, str, str, str]] = {}
        self._built = False

    def build_node_map(self) -> None:
        """Pre-build a mapping from NodeId string -> (device_id, point_name, data_type, unit).

        This avoids config lookups inside the hot notification path.
        """
        for device in self.device_configs["devices"]:
            for point_name, point_cfg in self.device_configs["points"].items():
                node_id_str = f"ns={self.namespace_idx};s={device.device_id}.{point_name}"
                self._node_id_to_device_point[node_id_str] = (
                    device.device_id,
                    point_name,
                    point_cfg.get("data_type", "Double"),
                    point_cfg.get("unit", ""),
                )
        self._built = True
        LOGGER.info("Built node map with %d entries", len(self._node_id_to_device_point))

    def datachange_notification(self, node: Any, value: Any, data: Any) -> None:
        """Called by asyncua when a MonitoredItem value changes.

        This method must be fast — no blocking I/O, no HTTP, no DB writes.
        Uses call_soon_threadsafe to safely push into the asyncio queue
        from asyncua's notification thread.
        """
        self.count += 1
        node_id_str = str(node)

        # Extract metadata from pre-built map
        mapping = self._node_id_to_device_point.get(node_id_str)
        if mapping is None:
            LOGGER.debug("Ignoring notification for unmapped node: %s", node_id_str)
            return

        device_id, point_name, data_type, unit = mapping

        # Extract DataValue fields
        data_value = getattr(getattr(data, "monitored_item", None), "Value", None)
        raw_status = (
            getattr(data_value, "StatusCode_", None)
            if data_value is not None
            else None
        )
        # Convert StatusCode object to plain int for JSON serialization
        status_code = int(raw_status.value) if raw_status is not None else 0

        source_ts = getattr(data_value, "SourceTimestamp", None) if data_value is not None else None
        server_ts = getattr(data_value, "ServerTimestamp", None) if data_value is not None else None

        # Update last-seen for stale detection
        now_mono = time.monotonic()
        self.last_seen[device_id] = now_mono

        # Map to standard event
        event = EventMapper.map_datachange_to_event(
            node_id=node_id_str,
            device_id=device_id,
            point_name=point_name,
            data_type=data_type,
            unit=unit,
            value=value,
            status_code=status_code,
            source_timestamp=source_ts,
            server_timestamp=server_ts,
        )

        # Push to queue — use call_soon_threadsafe for cross-thread safety
        def _enqueue(evt: dict[str, Any]) -> None:
            try:
                self.queue.put_nowait(evt)
            except asyncio.QueueFull:
                LOGGER.warning(
                    "Collector queue full (%d), dropping event for %s/%s",
                    self.queue.maxsize,
                    evt.get("device_id", "?"),
                    evt.get("point_name", "?"),
                )

        self.loop.call_soon_threadsafe(_enqueue, event)

        # Periodic heartbeat log (every 100 notifications)
        if self.count % 100 == 0:
            LOGGER.info(
                "Received %d notifications, queue≈%d",
                self.count,
                self.queue.qsize(),
            )


# ---------------------------------------------------------------------------
# Stale device monitor — generates device_offline events
# ---------------------------------------------------------------------------


class StaleDeviceMonitor:
    """Periodically checks for stale devices and emits device_offline events."""

    def __init__(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        handler: SubscriptionHandler,
        device_ids: list[str],
        timeout_seconds: float = DEFAULT_DEVICE_STALE_TIMEOUT,
    ) -> None:
        self.queue = queue
        self.handler = handler
        self.device_ids = device_ids
        self.timeout = timeout_seconds
        # Track which devices are currently offline to avoid duplicate events
        self.offline_devices: set[str] = set()

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run periodic stale detection until stop_event is set."""
        check_interval = max(1.0, self.timeout / 3.0)
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=check_interval)
                break  # stop_event was set
            except asyncio.TimeoutError:
                pass  # Normal: check stale devices

            now_mono = time.monotonic()
            for device_id in self.device_ids:
                last_seen = self.handler.last_seen.get(device_id)
                if last_seen is None:
                    # Device hasn't sent any data yet — skip
                    continue

                elapsed = now_mono - last_seen
                is_offline = elapsed > self.timeout

                if is_offline and device_id not in self.offline_devices:
                    self.offline_devices.add(device_id)
                    event = EventMapper.create_device_event(
                        device_id=device_id,
                        event_type="device_offline",
                        message=f"Device {device_id} offline: no data for {elapsed:.0f}s",
                    )
                    try:
                        self.queue.put_nowait(event)
                    except asyncio.QueueFull:
                        LOGGER.warning("Queue full, dropped device_offline event for %s", device_id)
                    LOGGER.warning("Device %s marked as offline (stale for %.0fs)", device_id, elapsed)

                if not is_offline and device_id in self.offline_devices:
                    self.offline_devices.discard(device_id)
                    event = EventMapper.create_device_event(
                        device_id=device_id,
                        event_type="device_online",
                        message=f"Device {device_id} recovered",
                        quality_code=0,  # Good
                    )
                    try:
                        self.queue.put_nowait(event)
                    except asyncio.QueueFull:
                        LOGGER.warning("Queue full, dropped device_online event for %s", device_id)
                    LOGGER.info("Device %s recovered from offline state", device_id)


# ---------------------------------------------------------------------------
# Background sender — drains the queue and sends events to Go backend
# ---------------------------------------------------------------------------

DEFAULT_REQUEST_TIMEOUT = 10.0
_DRAIN_GRACE_SECONDS = 5.0


async def background_sender(
    queue: asyncio.Queue[dict[str, Any]],
    sender: EventSender,
    stop_event: asyncio.Event,
    batch_size: int = DEFAULT_SEND_BATCH_SIZE,
    interval: float = DEFAULT_SEND_INTERVAL_SECONDS,
) -> None:
    """Continuously drain the event queue and batch-send to backend.

    Stops when stop_event is set AND the queue has been drained.
    """
    LOGGER.info("Background sender started (batch_size=%d, interval=%.2fs)", batch_size, interval)

    while True:
        # Collect a batch of events
        batch: list[dict[str, Any]] = []

        # Try to get the first event
        try:
            event = await asyncio.wait_for(queue.get(), timeout=interval)
            batch.append(event)
            queue.task_done()
        except asyncio.TimeoutError:
            # Check if we should stop
            if stop_event.is_set() and queue.empty():
                break
            continue

        # Drain remaining events up to batch_size (non-blocking)
        while len(batch) < batch_size:
            try:
                event = queue.get_nowait()
                batch.append(event)
                queue.task_done()
            except asyncio.QueueEmpty:
                break

        # Send the batch
        if batch:
            try:
                ok = await sender.send_events(batch)
                if not ok:
                    LOGGER.warning("Batch send returned False (events in dead letter)")
            except Exception as exc:
                LOGGER.error("Batch send crashed: %s", exc, exc_info=True)

        # Check stop condition
        if stop_event.is_set() and queue.empty():
            break

    # Final drain: collect any remaining events
    LOGGER.info("Background sender stopping, performing final drain...")
    remaining: list[dict[str, Any]] = []
    while not queue.empty():
        try:
            event = queue.get_nowait()
            remaining.append(event)
            queue.task_done()
        except asyncio.QueueEmpty:
            break

    if remaining:
        LOGGER.info("Sending %d remaining events during final drain", len(remaining))
        # Send in batches
        for i in range(0, len(remaining), batch_size):
            chunk = remaining[i : i + batch_size]
            try:
                await asyncio.wait_for(
                    sender.send_events(chunk),
                    timeout=DEFAULT_REQUEST_TIMEOUT,
                )
            except asyncio.TimeoutError:
                LOGGER.error("Final drain timed out, writing %d events to dead letter", len(chunk))
                await sender.dead_letter.write(chunk, "graceful_shutdown_timeout")
            except Exception as exc:
                LOGGER.error("Final drain failed: %s, writing %d events to dead letter", exc, len(chunk))
                await sender.dead_letter.write(chunk, f"graceful_shutdown_error: {exc}")

    LOGGER.info("Background sender finished")


# ---------------------------------------------------------------------------
# OPC UA connection manager with exponential backoff reconnection
# ---------------------------------------------------------------------------


class OPCUACollector:
    """Manages OPC UA connection, subscription, and reconnection lifecycle.

    Responsible for:
    - Connecting to OPC UA Server with retry
    - Creating Subscriptions and MonitoredItems
    - Detecting disconnection and re-establishing subscription
    - Coordinating graceful shutdown
    """

    def __init__(
        self,
        endpoint: str,
        namespace_uri: str,
        device_configs: dict[str, Any],
        queue: asyncio.Queue[dict[str, Any]],
        publishing_interval_ms: float = DEFAULT_PUBLISHING_INTERVAL_MS,
        reconnect_base_delay: float = DEFAULT_RECONNECT_BASE_DELAY,
        reconnect_max_delay: float = DEFAULT_RECONNECT_MAX_DELAY,
    ) -> None:
        self.endpoint = endpoint
        self.namespace_uri = namespace_uri
        self.device_configs = device_configs
        self.queue = queue
        self.publishing_interval_ms = publishing_interval_ms
        self.reconnect_base_delay = reconnect_base_delay
        self.reconnect_max_delay = reconnect_max_delay
        self._handler: Optional[SubscriptionHandler] = None
        self._client: Optional[Client] = None
        self._subscription = None
        self._connected_event = asyncio.Event()

    @property
    def last_seen(self) -> dict[str, float]:
        if self._handler is not None:
            return self._handler.last_seen
        return {}

    async def connect_with_retry(self, stop_event: asyncio.Event) -> bool:
        """Connect to OPC UA Server with exponential backoff retry.

        Returns True if connected successfully, False if stop_event was set.
        """
        attempt = 1
        while not stop_event.is_set():
            try:
                self._client = Client(url=self.endpoint)
                # Set a shorter timeout for the connection attempt
                self._client.timeout = 10
                await self._client.connect()
                LOGGER.info("Connected to OPC UA server at %s (attempt %d)", self.endpoint, attempt)
                return True
            except Exception as exc:
                LOGGER.warning(
                    "Failed to connect to OPC UA server: %s (attempt %d)",
                    exc,
                    attempt,
                )
                if self._client is not None:
                    try:
                        await self._client.disconnect()
                    except Exception:
                        pass
                    self._client = None

                delay = min(
                    self.reconnect_base_delay * (2 ** (attempt - 1)),
                    self.reconnect_max_delay,
                )
                LOGGER.info("Reconnecting in %.0fs...", delay)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    return False  # stop_event was set
                except asyncio.TimeoutError:
                    pass
                attempt += 1

        return False

    async def create_subscription(self) -> Optional[SubscriptionHandler]:
        """Create subscription and register MonitoredItems for all points.

        Returns the SubscriptionHandler on success, None on failure.
        """
        if self._client is None:
            LOGGER.error("Cannot create subscription: not connected")
            return None

        try:
            namespace_idx = await self._client.get_namespace_index(self.namespace_uri)
        except Exception as exc:
            LOGGER.error("Failed to get namespace index: %s", exc)
            return None

        handler = SubscriptionHandler(
            queue=self.queue,
            event_loop=asyncio.get_event_loop(),
            device_configs=self.device_configs,
            namespace_idx=namespace_idx,
        )
        handler.build_node_map()

        # Build node list
        node_ids = list(handler._node_id_to_device_point.keys())
        if not node_ids:
            LOGGER.error("No nodes to subscribe. Check device and point configuration.")
            return None

        nodes = [self._client.get_node(node_id) for node_id in node_ids]

        try:
            self._subscription = await self._client.create_subscription(
                self.publishing_interval_ms,
                handler,
            )
            await self._subscription.subscribe_data_change(nodes)
        except Exception as exc:
            LOGGER.error("Failed to create subscription: %s", exc)
            return None

        LOGGER.info(
            "Created subscription: %d nodes, publishing_interval=%dms",
            len(nodes),
            int(self.publishing_interval_ms),
        )
        self._handler = handler
        return handler

    async def disconnect(self) -> None:
        """Disconnect from OPC UA server gracefully."""
        if self._subscription is not None:
            try:
                await asyncio.wait_for(self._subscription.delete(), timeout=5.0)
            except Exception as exc:
                LOGGER.debug("Error deleting subscription: %s", exc)
            self._subscription = None

        if self._client is not None:
            try:
                await self._client.disconnect()
            except Exception as exc:
                LOGGER.debug("Error disconnecting client: %s", exc)
            self._client = None

        self._handler = None
        LOGGER.info("Disconnected from OPC UA server")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    default_config = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
    parser = argparse.ArgumentParser(
        description="OPC UA Collector: subscribe to DataChange, map to events, send to Go backend."
    )
    parser.add_argument(
        "--config",
        default=str(default_config),
        help="Path to simulation YAML config.",
    )
    parser.add_argument(
        "--backend-url",
        default=DEFAULT_BACKEND_URL,
        help=f"Base URL of the Go backend (default read from SERVER_HOST:SERVER_PORT env, or {DEFAULT_BACKEND_URL}).",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=DEFAULT_QUEUE_SIZE,
        help=f"Maximum size of the internal event queue (default: {DEFAULT_QUEUE_SIZE}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_SEND_BATCH_SIZE,
        help=f"Maximum events per HTTP batch (default: {DEFAULT_SEND_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--send-interval-seconds",
        type=float,
        default=DEFAULT_SEND_INTERVAL_SECONDS,
        help=f"Maximum wait between sending batches (default: {DEFAULT_SEND_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--publishing-interval-ms",
        type=float,
        default=DEFAULT_PUBLISHING_INTERVAL_MS,
        help=f"OPC UA subscription publishing interval in ms (default: {DEFAULT_PUBLISHING_INTERVAL_MS}).",
    )
    parser.add_argument(
        "--device-stale-timeout",
        type=float,
        default=DEFAULT_DEVICE_STALE_TIMEOUT,
        help=f"Seconds before a device is considered offline (default: {DEFAULT_DEVICE_STALE_TIMEOUT}).",
    )
    parser.add_argument(
        "--dead-letter-dir",
        default=str(Path("data/dead-letter")),
        help="Directory for dead letter NDJSON files.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------


async def run() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    # Load config
    config = load_config(args.config)
    endpoint = config.server.endpoint.replace("0.0.0.0", "127.0.0.1")
    namespace_uri = config.server.namespace_uri

    # Prepare device configs dict for SubscriptionHandler
    device_configs = {
        "devices": config.devices,
        "points": config.points,
    }
    device_ids = [d.device_id for d in config.devices]

    # Create the bounded queue
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=args.queue_size)

    # Create the event sender
    sender = EventSender(
        backend_url=args.backend_url,
        dead_letter_dir=Path(args.dead_letter_dir),
    )

    # Create the OPC UA collector
    collector = OPCUACollector(
        endpoint=endpoint,
        namespace_uri=namespace_uri,
        device_configs=device_configs,
        queue=queue,
        publishing_interval_ms=args.publishing_interval_ms,
    )

    # Stop event for coordinating shutdown
    main_stop = asyncio.Event()
    shutdown_complete = asyncio.Event()

    def _signal_handler() -> None:
        if not main_stop.is_set():
            LOGGER.info("Received stop signal, initiating graceful shutdown...")
            main_stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows does not support add_signal_handler for SIGTERM
            pass

    # Track background tasks for cleanup
    bg_tasks: list[asyncio.Task[Any]] = []
    handler: Optional[SubscriptionHandler] = None

    try:
        # Step 1: Connect to OPC UA server
        connected = await collector.connect_with_retry(main_stop)
        if not connected:
            LOGGER.info("Shutdown requested before connection established.")
            return

        # Step 2: Create subscription
        handler = await collector.create_subscription()
        if handler is None:
            LOGGER.error("Failed to create subscription. Exiting.")
            return

        # Step 3: Start background sender
        sender_task = asyncio.create_task(
            background_sender(
                queue=queue,
                sender=sender,
                stop_event=main_stop,
                batch_size=args.batch_size,
                interval=args.send_interval_seconds,
            ),
            name="sender",
        )
        bg_tasks.append(sender_task)

        # Step 4: Start stale device monitor
        stale_stop = asyncio.Event()
        stale_monitor = StaleDeviceMonitor(
            queue=queue,
            handler=handler,
            device_ids=device_ids,
            timeout_seconds=args.device_stale_timeout,
        )
        stale_task = asyncio.create_task(
            stale_monitor.run(stale_stop),
            name="stale_monitor",
        )
        bg_tasks.append(stale_task)

        LOGGER.info(
            "Collector running: endpoint=%s devices=%d points=%d queue_size=%d",
            endpoint,
            len(device_ids),
            len(config.points),
            args.queue_size,
        )

        # Step 5: Wait for stop signal
        await main_stop.wait()

    except asyncio.CancelledError:
        LOGGER.info("Collector task cancelled")
        main_stop.set()
    except Exception:
        LOGGER.exception("Unexpected error in collector main loop")
        main_stop.set()
    finally:
        # Step 6: Graceful shutdown sequence
        LOGGER.info("Initiating graceful shutdown...")

        # Stop stale monitor
        stale_stop.set()

        # Disconnect from OPC UA server
        await collector.disconnect()

        # Wait for sender to drain queue and finish
        if bg_tasks:
            done, pending = await asyncio.wait(
                bg_tasks,
                timeout=30.0,
                return_when=asyncio.ALL_COMPLETED,
            )
            for task in pending:
                LOGGER.warning("Cancelling task %s (timeout)", task.get_name())
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            for task in done:
                if task.exception() is not None:
                    LOGGER.error(
                        "Task %s raised exception: %s",
                        task.get_name(),
                        task.exception(),
                    )

        LOGGER.info(
            "Graceful shutdown complete. Total notifications received: %d",
            handler.count if handler else 0,
        )
        shutdown_complete.set()


def main() -> None:
    """Entry point for the OPC UA Collector."""
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user")


if __name__ == "__main__":
    main()