from __future__ import annotations

import logging
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Optional

from asyncua import ua

LOGGER = logging.getLogger(__name__)

# OPC UA StatusCode to human-readable quality name mapping
_QUALITY_MAP: dict[int, str] = {
    0: "Good",
    ua.StatusCodes.Uncertain: "Uncertain",
    ua.StatusCodes.Bad: "Bad",
}


def quality_code_to_name(status_code: int) -> str:
    """Convert OPC UA StatusCode to a human-readable quality name."""
    return _QUALITY_MAP.get(status_code, f"Unknown({status_code})")


def generate_event_id() -> str:
    """Generate a time-ordered, globally unique event_id (UUID v7 style).

    Uses a combination of timestamp and UUID to produce sortable identifiers.
    """
    # UUID1 includes timestamp + MAC and is time-sortable
    raw = uuid.uuid1()
    # Convert to standard hex format with dashes
    return str(raw)


def _opcua_dt_to_iso(timestamp: Any) -> Optional[str]:
    """Convert an OPC UA timestamp to ISO 8601 UTC string.

    Returns None if the timestamp is null/invalid.
    """
    if timestamp is None:
        return None
    try:
        if hasattr(timestamp, "isoformat"):
            return timestamp.isoformat()
        dt = datetime.fromisoformat(str(timestamp))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        LOGGER.debug("Failed to convert timestamp: %s", timestamp)
        return None


class EventMapper:
    """Maps OPC UA DataChange notifications to the standard JSON event model.

    See Section 5 of the PoC implementation document for the event structure.
    """

    @staticmethod
    def map_datachange_to_event(
        node_id: str,
        device_id: str,
        point_name: str,
        data_type: str,
        unit: str,
        value: Any,
        status_code: int,
        source_timestamp: Any,
        server_timestamp: Any,
    ) -> dict[str, Any]:
        """Convert a single DataChange notification to a standard event dict.

        Args:
            node_id: OPC UA NodeId string of the variable.
            device_id: Device identifier extracted from NodeId.
            point_name: Point (variable) name.
            data_type: OPC UA data type string ("Double" or "DateTime").
            unit: Engineering unit string from config.
            value: The variable value from the notification.
            status_code: OPC UA StatusCode integer value.
            source_timestamp: SourceTimestamp from the DataValue.
            server_timestamp: ServerTimestamp from the DataValue.

        Returns:
            A dict conforming to the standard event model defined in Section 5.
        """
        event_id = generate_event_id()
        collector_ts = datetime.now(timezone.utc).isoformat()

        quality_name = quality_code_to_name(status_code)
        is_good = status_code == ua.StatusCodes.Good

        # Determine event_type based on quality
        if is_good:
            event_type = "data_change"
        else:
            event_type = "quality_alarm"

        # Populate value fields based on data_type
        value_number: Optional[float] = None
        value_text: Optional[str] = None
        value_time: Optional[str] = None

        if value is not None:
            if data_type == "Double":
                try:
                    value_number = float(value)
                except (ValueError, TypeError):
                    value_text = str(value)
            elif data_type == "DateTime":
                value_time = _opcua_dt_to_iso(value)
            else:
                value_text = str(value)

        source_ts_str = _opcua_dt_to_iso(source_timestamp)
        server_ts_str = _opcua_dt_to_iso(server_timestamp)

        event: dict[str, Any] = {
            "event_id": event_id,
            "device_id": device_id,
            "point_name": point_name,
            "data_type": data_type,
            "unit": unit,
            "value_number": value_number,
            "value_text": value_text,
            "value_time": value_time,
            "quality_code": status_code,
            "quality_name": quality_name,
            "source_timestamp": source_ts_str,
            "server_timestamp": server_ts_str,
            "collector_timestamp": collector_ts,
            "event_type": event_type,
        }

        return event

    @staticmethod
    def create_device_event(
        device_id: str,
        event_type: str,
        message: str,
        quality_code: int = ua.StatusCodes.Bad,
    ) -> dict[str, Any]:
        """Create a synthetic event for device-level status changes.

        Used for device offline/online notifications and other non-data events.

        Args:
            device_id: Device identifier.
            event_type: Event type string (e.g. "device_offline", "device_online").
            message: Human-readable description.
            quality_code: OPC UA status code to record.

        Returns:
            A dict conforming to the standard event model.
        """
        event_id = generate_event_id()
        collector_ts = datetime.now(timezone.utc).isoformat()
        quality_name = quality_code_to_name(quality_code)

        return {
            "event_id": event_id,
            "device_id": device_id,
            "point_name": "device_status",
            "data_type": "String",
            "unit": "",
            "value_number": None,
            "value_text": message,
            "value_time": None,
            "quality_code": quality_code,
            "quality_name": quality_name,
            "source_timestamp": collector_ts,
            "server_timestamp": collector_ts,
            "collector_timestamp": collector_ts,
            "event_type": event_type,
        }