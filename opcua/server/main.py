from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from asyncua import Server

from .address_space import build_address_space
from .config_loader import load_config
from .data_generator import DataGenerator

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run an asyncua-based OPC UA simulator.")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parents[1] / "config" / "config.yaml"),
        help="Path to simulation YAML config.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Load and validate config without starting the OPC UA server.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="Stop automatically after N seconds. Use 0 to run until interrupted.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser.parse_args()


async def run() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    config = load_config(args.config)
    LOGGER.info(
        "Loaded config: endpoint=%s devices=%s points=%s",
        config.server.endpoint,
        len(config.devices),
        len(config.points),
    )

    if args.validate_only:
        LOGGER.info("Config validation completed.")
        return

    server = Server()
    await server.init()
    server.set_endpoint(config.server.endpoint)
    server.set_server_name(config.server.server_name)
    namespace_idx = await server.register_namespace(config.server.namespace_uri)

    device_nodes = await build_address_space(server, config, namespace_idx)
    generator = DataGenerator(config, device_nodes)

    LOGGER.info("Starting OPC UA server at %s", config.server.endpoint)
    async with server:
        await generator.start()
        try:
            if args.duration_seconds > 0:
                await asyncio.sleep(args.duration_seconds)
            else:
                stop_event = asyncio.Event()
                await stop_event.wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            LOGGER.info("Stopping OPC UA simulator.")
        finally:
            await generator.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
