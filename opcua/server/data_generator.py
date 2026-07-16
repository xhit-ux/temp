from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from asyncua import ua

from .address_space import DeviceNodes, make_data_value
from .config_loader import DeviceConfig, SimulationConfig

LOGGER = logging.getLogger(__name__)


@dataclass
class DeviceRuntimeState:
    started_at_monotonic: float
    values: dict[str, float] = field(default_factory=dict)
    speed_next_change_at: float = 0.0
    offline_until: float = 0.0


class DataGenerator:
    def __init__(self, config: SimulationConfig, device_nodes: dict[str, DeviceNodes]) -> None:
        self.config = config
        self.device_nodes = device_nodes
        self.interval = config.server.update_interval_seconds
        self.random = random.Random(config.simulation.get("random_seed"))
        self.states = {
            device_id: self._initial_state(device_nodes_obj.device)
            for device_id, device_nodes_obj in device_nodes.items()
        }
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        for device_id in self.device_nodes:
            self._tasks.append(asyncio.create_task(self._run_device(device_id), name=f"sim-{device_id}"))
        LOGGER.info("Started %s device simulation tasks.", len(self._tasks))

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _run_device(self, device_id: str) -> None:
        while self._running:
            loop_started_at = time.monotonic()
            try:
                await self._update_device(device_id, loop_started_at)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Failed to update device %s.", device_id)

            elapsed = time.monotonic() - loop_started_at
            await asyncio.sleep(max(0.0, self.interval - elapsed))

    async def _update_device(self, device_id: str, now_monotonic: float) -> None:
        nodes = self.device_nodes[device_id]
        state = self.states[device_id]

        if self._should_enter_offline(now_monotonic):
            duration = float(self.config.simulation.get("offline", {}).get("duration_seconds", 8))
            state.offline_until = now_monotonic + duration
            LOGGER.warning("Device %s enters simulated offline state for %.1fs.", device_id, duration)

        if now_monotonic < state.offline_until:
            return

        for point_name, variable_node in nodes.variables.items():
            point_cfg = self.config.points[point_name]
            value = self._next_value(nodes.device, state, point_name, point_cfg, now_monotonic)
            status_code = self._next_status_code()
            data_value = make_data_value(value, point_cfg["data_type"], status_code)
            await variable_node.write_value(data_value)

    def _initial_state(self, device: DeviceConfig) -> DeviceRuntimeState:
        values = {
            point_name: float(point_cfg.get("initial", 0.0))
            for point_name, point_cfg in self.config.points.items()
            if point_cfg["data_type"] == "Double"
        }
        now = time.monotonic()
        return DeviceRuntimeState(
            started_at_monotonic=now,
            values=values,
            speed_next_change_at=now,
        )

    def _next_value(
        self,
        device: DeviceConfig,
        state: DeviceRuntimeState,
        point_name: str,
        point_cfg: dict[str, Any],
        now_monotonic: float,
    ) -> Any:
        mode = point_cfg.get("mode")

        if mode == "device_clock":
            elapsed_seconds = now_monotonic - state.started_at_monotonic
            drift_seconds = elapsed_seconds / 60.0 * device.drift_seconds_per_minute
            return datetime.now(timezone.utc) + timedelta(
                seconds=device.clock_offset_seconds + drift_seconds
            )

        if mode == "random_walk":
            value = state.values[point_name]
            trend = float(point_cfg.get("trend", 0.0))
            step = float(point_cfg.get("step", 1.0))
            value += trend + self.random.uniform(-step, step)
            value = self._clamp(value, point_cfg)

        elif mode == "sine":
            elapsed_seconds = now_monotonic - state.started_at_monotonic
            period = float(point_cfg.get("period_seconds", 60))
            amplitude = float(point_cfg.get("amplitude", 1.0))
            midpoint = (float(point_cfg["min"]) + float(point_cfg["max"])) / 2.0
            noise = self.random.uniform(-float(point_cfg.get("noise", 0.0)), float(point_cfg.get("noise", 0.0)))
            value = midpoint + amplitude * math.sin(2 * math.pi * elapsed_seconds / period) + noise
            value = self._clamp(value, point_cfg)

        elif mode == "step":
            if now_monotonic >= state.speed_next_change_at:
                value = float(self.random.choice(point_cfg.get("step_values", [0.0])))
                state.speed_next_change_at = now_monotonic + self.random.uniform(
                    float(point_cfg.get("min_hold_seconds", 8)),
                    float(point_cfg.get("max_hold_seconds", 25)),
                )
            else:
                value = state.values[point_name]
            value = self._clamp(value, point_cfg)

        elif mode == "correlated_current":
            speed = state.values.get("speed", 0.0)
            base = float(point_cfg.get("base", 0.0))
            speed_factor = float(point_cfg.get("speed_factor", 0.01))
            noise = self.random.uniform(-float(point_cfg.get("noise", 0.0)), float(point_cfg.get("noise", 0.0)))
            value = self._clamp(base + speed * speed_factor + noise, point_cfg)

        elif mode == "noise":
            initial = float(point_cfg.get("initial", 0.0))
            noise = self.random.uniform(-float(point_cfg.get("noise", 0.0)), float(point_cfg.get("noise", 0.0)))
            value = self._clamp(initial + noise, point_cfg)

        else:
            value = state.values.get(point_name, float(point_cfg.get("initial", 0.0)))

        value = self._maybe_spike(value)
        value = round(self._clamp(value, point_cfg), 4)
        state.values[point_name] = value
        return value

    def _maybe_spike(self, value: float) -> float:
        spike_cfg = self.config.simulation.get("spike", {})
        if not spike_cfg.get("enabled", False):
            return value
        if self.random.random() >= float(spike_cfg.get("probability", 0.0)):
            return value
        multiplier = self.random.uniform(
            float(spike_cfg.get("multiplier_min", 1.15)),
            float(spike_cfg.get("multiplier_max", 1.45)),
        )
        return value * multiplier

    def _next_status_code(self) -> int:
        quality_cfg = self.config.simulation.get("quality", {})
        chance = self.random.random()
        bad_probability = float(quality_cfg.get("bad_probability", 0.0))
        uncertain_probability = float(quality_cfg.get("uncertain_probability", 0.0))
        if chance < bad_probability:
            return ua.StatusCodes.Bad
        if chance < bad_probability + uncertain_probability:
            return ua.StatusCodes.Uncertain
        return ua.StatusCodes.Good

    def _should_enter_offline(self, now_monotonic: float) -> bool:
        offline_cfg = self.config.simulation.get("offline", {})
        if not offline_cfg.get("enabled", False):
            return False
        probability = float(offline_cfg.get("probability_per_cycle", 0.0))
        return self.random.random() < probability

    @staticmethod
    def _clamp(value: float, point_cfg: dict[str, Any]) -> float:
        minimum = float(point_cfg.get("min", value))
        maximum = float(point_cfg.get("max", value))
        return min(maximum, max(minimum, value))
