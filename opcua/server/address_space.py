from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from asyncua import Server, ua

from .config_loader import DeviceConfig, SimulationConfig


@dataclass
class DeviceNodes:
    device: DeviceConfig
    object_node: Any
    variables: dict[str, Any]


async def build_address_space(server: Server, config: SimulationConfig, namespace_idx: int) -> dict[str, DeviceNodes]:
    objects = server.nodes.objects
    devices_folder = await objects.add_folder(ua.NodeId("Devices", namespace_idx), "Devices")
    device_nodes: dict[str, DeviceNodes] = {}

    for device in config.devices:
        device_object = await devices_folder.add_object(
            ua.NodeId(device.device_id, namespace_idx),
            device.device_id,
        )

        variables: dict[str, Any] = {}
        for point_name, point_cfg in config.points.items():
            value = _initial_value(point_cfg)
            variant_type = _variant_type(point_cfg["data_type"])
            node_id = ua.NodeId(f"{device.device_id}.{point_name}", namespace_idx)
            variable = await device_object.add_variable(node_id, point_name, value, varianttype=variant_type)
            await variable.set_writable(False)
            variables[point_name] = variable

        device_nodes[device.device_id] = DeviceNodes(
            device=device,
            object_node=device_object,
            variables=variables,
        )

    return device_nodes


def make_data_value(value: Any, data_type: str, status_code: int, now: Optional[datetime] = None) -> ua.DataValue:
    if now is None:
        now = datetime.now(timezone.utc)
    return ua.DataValue(
        Value=ua.Variant(value, _variant_type(data_type)),
        StatusCode_=ua.StatusCode(status_code),
        SourceTimestamp=now,
        ServerTimestamp=now,
    )


def _initial_value(point_cfg: dict[str, Any]) -> Any:
    if point_cfg["data_type"] == "DateTime":
        return datetime.now(timezone.utc)
    return float(point_cfg.get("initial", 0.0))


def _variant_type(data_type: str) -> ua.VariantType:
    if data_type == "Double":
        return ua.VariantType.Double
    if data_type == "DateTime":
        return ua.VariantType.DateTime
    raise ValueError(f"Unsupported OPC UA data type: {data_type}")
