from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from asyncua import Client

from server.config_loader import load_config

LOGGER = logging.getLogger(__name__)


class SubscriptionHandler:
    def __init__(self) -> None:
        self.count = 0

    def datachange_notification(self, node: Any, value: Any, data: Any) -> None:
        self.count += 1
        data_value = getattr(getattr(data, "monitored_item", None), "Value", None)
        status = getattr(data_value, "StatusCode_", None)
        source_ts = getattr(data_value, "SourceTimestamp", None)
        print(
            f"[{self.count:04d}] node={node} value={value} "
            f"status={status} source_ts={source_ts} ingest_ts={datetime.now(timezone.utc).isoformat()}"
        )


def parse_args() -> argparse.Namespace:
    default_config = Path(__file__).resolve().parents[1] / "config" / "simulation_config.yaml"
    parser = argparse.ArgumentParser(description="Subscribe to simulator nodes and print DataChange events.")
    parser.add_argument("--config", default=str(default_config), help="Path to simulation YAML config.")
    parser.add_argument("--endpoint", default=None, help="Override OPC UA endpoint.")
    parser.add_argument("--duration-seconds", type=float, default=10.0, help="Probe duration.")
    parser.add_argument("--publishing-interval-ms", type=float, default=500.0, help="Subscription interval.")
    parser.add_argument(
        "--max-nodes",
        type=int,
        default=6,
        help="Maximum number of variables to subscribe. Use 0 for all configured variables.",
    )
    return parser.parse_args()


async def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    args = parse_args()
    config = load_config(args.config)
    endpoint = args.endpoint or config.server.endpoint.replace("0.0.0.0", "127.0.0.1")

    async with Client(url=endpoint) as client:
        namespace_idx = await client.get_namespace_index(config.server.namespace_uri)
        node_ids = [
            f"ns={namespace_idx};s={device.device_id}.{point_name}"
            for device in config.devices
            for point_name in config.points
        ]
        if args.max_nodes > 0:
            node_ids = node_ids[: args.max_nodes]

        nodes = [client.get_node(node_id) for node_id in node_ids]
        handler = SubscriptionHandler()
        subscription = await client.create_subscription(args.publishing_interval_ms, handler)
        await subscription.subscribe_data_change(nodes)

        LOGGER.info("Subscribed %s nodes from %s", len(nodes), endpoint)
        await asyncio.sleep(args.duration_seconds)
        await subscription.delete()
        LOGGER.info("Received %s DataChange notifications.", handler.count)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
