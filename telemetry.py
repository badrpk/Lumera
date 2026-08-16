from __future__ import annotations

from dataclasses import dataclass, asdict
from hashlib import sha256
import json
from statistics import fmean
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Reading:
    meter_id: str
    ts_ms: int
    watts: float
    voltage: Optional[float] = None
    frequency_hz: Optional[float] = None


@dataclass(frozen=True)
class Alert:
    meter_id: str
    ts_ms: int
    kind: str
    value: float
    threshold: float


class TelemetryStream:
    """Dependency-light live energy telemetry core.

    The stream keeps readings ordered, deduplicates by meter/timestamp,
    supports rolling-window summaries and emits only explicitly configured
    threshold/anomaly alerts. It performs no billing or grid-control actions.
    """

    def __init__(self, *, retention_per_meter: int = 1000) -> None:
        if retention_per_meter <= 0:
            raise ValueError("retention_per_meter must be positive")
        self.retention_per_meter = retention_per_meter
        self._readings: Dict[str, List[Reading]] = {}
        self._seen: set[Tuple[str, int]] = set()
        self._thresholds: Dict[str, Dict[str, float]] = {}

    def set_thresholds(
        self,
        meter_id: str,
        *,
        high_watts: Optional[float] = None,
        low_voltage: Optional[float] = None,
        high_voltage: Optional[float] = None,
        max_delta_watts: Optional[float] = None,
    ) -> None:
        vals = {
            "high_watts": high_watts,
            "low_voltage": low_voltage,
            "high_voltage": high_voltage,
            "max_delta_watts": max_delta_watts,
        }
        for key, value in vals.items():
            if value is not None and value < 0:
                raise ValueError(f"{key} must be non-negative")
        self._thresholds[meter_id] = {k: float(v) for k, v in vals.items() if v is not None}

    def ingest(self, reading: Reading) -> Tuple[bool, Tuple[Alert, ...]]:
        if not reading.meter_id.strip():
            raise ValueError("meter_id is required")
        if reading.ts_ms < 0:
            raise ValueError("ts_ms must be non-negative")
        if reading.watts < 0:
            raise ValueError("watts must be non-negative")
        if reading.voltage is not None and reading.voltage < 0:
            raise ValueError("voltage must be non-negative")
        key = (reading.meter_id, reading.ts_ms)
        if key in self._seen:
            return False, ()

        series = self._readings.setdefault(reading.meter_id, [])
        previous = series[-1] if series else None
        series.append(reading)
        series.sort(key=lambda r: r.ts_ms)
        if len(series) > self.retention_per_meter:
            removed = series[:-self.retention_per_meter]
            series[:] = series[-self.retention_per_meter:]
            for item in removed:
                self._seen.discard((item.meter_id, item.ts_ms))
        self._seen.add(key)

        alerts: List[Alert] = []
        limits = self._thresholds.get(reading.meter_id, {})
        if "high_watts" in limits and reading.watts > limits["high_watts"]:
            alerts.append(Alert(reading.meter_id, reading.ts_ms, "high_watts", reading.watts, limits["high_watts"]))
        if reading.voltage is not None:
            if "low_voltage" in limits and reading.voltage < limits["low_voltage"]:
                alerts.append(Alert(reading.meter_id, reading.ts_ms, "low_voltage", reading.voltage, limits["low_voltage"]))
            if "high_voltage" in limits and reading.voltage > limits["high_voltage"]:
                alerts.append(Alert(reading.meter_id, reading.ts_ms, "high_voltage", reading.voltage, limits["high_voltage"]))
        if previous is not None and "max_delta_watts" in limits:
            delta = abs(reading.watts - previous.watts)
            if delta > limits["max_delta_watts"]:
                alerts.append(Alert(reading.meter_id, reading.ts_ms, "delta_watts", delta, limits["max_delta_watts"]))
        return True, tuple(sorted(alerts, key=lambda a: a.kind))

    def latest(self, meter_id: str) -> Optional[Reading]:
        series = self._readings.get(meter_id, [])
        return series[-1] if series else None

    def is_stale(self, meter_id: str, *, now_ms: int, max_age_ms: int) -> bool:
        if max_age_ms < 0:
            raise ValueError("max_age_ms must be non-negative")
        latest = self.latest(meter_id)
        return latest is None or now_ms - latest.ts_ms > max_age_ms

    def window(self, meter_id: str, *, since_ms: int, until_ms: Optional[int] = None) -> List[Reading]:
        until_ms = float("inf") if until_ms is None else until_ms
        return [r for r in self._readings.get(meter_id, []) if since_ms <= r.ts_ms <= until_ms]

    def summary(self, meter_id: str, *, since_ms: int = 0) -> dict:
        rows = self.window(meter_id, since_ms=since_ms)
        if not rows:
            return {"meter_id": meter_id, "count": 0, "avg_watts": None, "min_watts": None, "max_watts": None}
        watts = [r.watts for r in rows]
        return {
            "meter_id": meter_id,
            "count": len(rows),
            "avg_watts": fmean(watts),
            "min_watts": min(watts),
            "max_watts": max(watts),
            "first_ts_ms": rows[0].ts_ms,
            "last_ts_ms": rows[-1].ts_ms,
        }

    def snapshot(self) -> dict:
        meters = {
            meter: [asdict(r) for r in rows]
            for meter, rows in sorted(self._readings.items())
        }
        payload = {"meters": meters, "thresholds": {k: self._thresholds[k] for k in sorted(self._thresholds)}}
        payload["snapshot_hash"] = sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return payload
