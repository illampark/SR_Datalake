"""SimState — 모든 시뮬레이터가 참조하는 공유 설비 상태.

같은 순간에 OPC-UA / Modbus / MQTT / DB / File / API 에서 관측되는 값이 일치하도록
설계. 스레드 안전 (RLock).
"""
from __future__ import annotations

import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque, Dict, List


@dataclass
class Snapshot:
    ts: str
    asset_id: str
    temperature: float
    humidity: float
    pressure: float
    mode: str
    total_cycle: int
    last_error: str
    firmware_ver: str


class SimState:
    def __init__(self, asset_id: str = "PRESS-01", history_max: int = 3600):
        self._lock = threading.RLock()
        self.asset_id = asset_id
        self.temperature = 22.0
        self.humidity = 60.0
        self.pressure = 1013.0
        self.mode = "Auto"
        self.total_cycle = 0
        self.last_error = ""
        self.firmware_ver = "1.2.3"
        self._history: Deque[Snapshot] = deque(maxlen=history_max)
        self._mode_last_change = time.time()

    def tick(self) -> Snapshot:
        with self._lock:
            self.temperature = round(self.temperature + random.uniform(-0.3, 0.3), 2)
            self.humidity = round(max(0.0, min(100.0, self.humidity + random.uniform(-0.5, 0.5))), 2)
            self.pressure = round(self.pressure + random.uniform(-0.4, 0.4), 2)
            self.total_cycle += 1

            # 모드 전이 (5분 = 300s 마다 확률로 변경)
            now = time.time()
            if now - self._mode_last_change > 300:
                self.mode = random.choice(["Auto", "Manual", "Stop"])
                self._mode_last_change = now

            # 오류 이벤트 확률 (0.5%)
            if random.random() < 0.005:
                self.last_error = random.choice(["E101", "E203", "W504"])
            elif random.random() < 0.05:
                self.last_error = ""

            snap = self.snapshot_locked()
            self._history.append(snap)
            return snap

    def snapshot(self) -> Snapshot:
        with self._lock:
            return self.snapshot_locked()

    def snapshot_locked(self) -> Snapshot:
        return Snapshot(
            ts=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            asset_id=self.asset_id,
            temperature=self.temperature,
            humidity=self.humidity,
            pressure=self.pressure,
            mode=self.mode,
            total_cycle=self.total_cycle,
            last_error=self.last_error,
            firmware_ver=self.firmware_ver,
        )

    def history(self, minutes: int = 10) -> List[Snapshot]:
        with self._lock:
            return list(self._history)[-minutes * 60 :]

    def to_dict(self) -> Dict:
        s = self.snapshot()
        return {
            "asset_id": s.asset_id,
            "timestamp": s.ts,
            "temperature": s.temperature,
            "humidity": s.humidity,
            "pressure": s.pressure,
            "mode": s.mode,
            "total_cycle": s.total_cycle,
            "last_error": s.last_error,
            "firmware_ver": s.firmware_ver,
        }
