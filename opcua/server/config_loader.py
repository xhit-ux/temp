from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ServerConfig:
    endpoint: str
    server_name: str
    namespace_uri: str
    update_interval_seconds: float


@dataclass(frozen=True)
class DeviceConfig:
    device_id: str
    display_name: str
    clock_offset_seconds: float
    drift_seconds_per_minute: float


@dataclass(frozen=True)
class SimulationConfig:
    server: ServerConfig
    devices: list[DeviceConfig]
    points: dict[str, dict[str, Any]]
    simulation: dict[str, Any]


def load_config(path: str | Path) -> SimulationConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as fp:
        raw = yaml.safe_load(fp)

    if not isinstance(raw, dict):
        raise ValueError("Config file must contain a YAML mapping.")

    server_raw = _required_mapping(raw, "server")
    devices_raw = raw.get("devices")
    points_raw = _required_mapping(raw, "points")
    simulation_raw = raw.get("simulation", {})

    if not isinstance(devices_raw, list) or not devices_raw:
        raise ValueError("Config field 'devices' must be a non-empty list.")

    server = ServerConfig(
        endpoint=str(server_raw["endpoint"]),
        server_name=str(server_raw.get("server_name", "OPC UA Simulator")),
        namespace_uri=str(server_raw["namespace_uri"]),
        update_interval_seconds=float(server_raw.get("update_interval_seconds", 1.0)),
    )

    devices: list[DeviceConfig] = []
    for item in devices_raw:
        if not isinstance(item, dict):
            raise ValueError("Each device must be a YAML mapping.")
        device_id = str(item["id"])
        devices.append(
            DeviceConfig(
                device_id=device_id,
                display_name=str(item.get("display_name", device_id)),
                clock_offset_seconds=float(item.get("clock_offset_seconds", 0.0)),
                drift_seconds_per_minute=float(item.get("drift_seconds_per_minute", 0.0)),
            )
        )

    _validate_point_config(points_raw)

    return SimulationConfig(
        server=server,
        devices=devices,
        points=points_raw,
        simulation=simulation_raw if isinstance(simulation_raw, dict) else {},
    )


def _required_mapping(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict) or not value:
        raise ValueError(f"Config field '{key}' must be a non-empty mapping.")
    return value


def _validate_point_config(points: dict[str, dict[str, Any]]) -> None:
    required_points = {"temperature", "pressure", "speed", "current", "voltage", "device_clock"}
    missing = required_points.difference(points)
    if missing:
        raise ValueError(f"Missing required point definitions: {sorted(missing)}")

    for point_name, point_cfg in points.items():
        if not isinstance(point_cfg, dict):
            raise ValueError(f"Point '{point_name}' must be a YAML mapping.")
        if "data_type" not in point_cfg:
            raise ValueError(f"Point '{point_name}' must define data_type.")
        if point_cfg["data_type"] not in {"Double", "DateTime"}:
            raise ValueError(f"Point '{point_name}' uses unsupported data_type: {point_cfg['data_type']}")
