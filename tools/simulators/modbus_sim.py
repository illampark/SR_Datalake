"""Modbus TCP slave simulator — staging only.

Listens on 0.0.0.0:5020 and serves Holding Registers / Input Registers / Coils
with values that vary over time (sin wave + counter) to make polling visible.

Run:
  python tools/simulators/modbus_sim.py
"""
import math
import os
import threading
import time

from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
)
from pymodbus.server import StartTcpServer


HOST = os.environ.get("MODBUS_SIM_HOST", "0.0.0.0")
PORT = int(os.environ.get("MODBUS_SIM_PORT", "5020"))


def _build_context():
    # 100 coils, 100 discrete inputs, 200 holding registers, 200 input registers
    device_ctx = ModbusDeviceContext(
        co=ModbusSequentialDataBlock(0, [False] * 100),
        di=ModbusSequentialDataBlock(0, [False] * 100),
        hr=ModbusSequentialDataBlock(0, [0] * 200),
        ir=ModbusSequentialDataBlock(0, [0] * 200),
    )
    # single-device context (fallback for any slave/unit id)
    return ModbusServerContext(devices=device_ctx, single=True)


def _updater(context, stop_evt):
    """매 1초 register 값을 흔들어 폴링 결과가 변하도록 한다."""
    t0 = time.time()
    counter = 0
    while not stop_evt.is_set():
        elapsed = time.time() - t0
        # HR 100 = int16 temperature 20 + 5 * sin(t)
        temp = int(20 + 5 * math.sin(elapsed))
        context[0x00].setValues(3, 100, [temp])
        # HR 101 = int16 pressure base 1013 + ramp
        pressure = (1000 + counter % 50) & 0xFFFF
        context[0x00].setValues(3, 101, [pressure])
        # HR 200..201 = float32 flow rate (big-endian)
        import struct
        flow = 12.5 + math.sin(elapsed / 2.0) * 0.5
        b = struct.pack(">f", flow)
        hi, lo = struct.unpack(">HH", b)
        context[0x00].setValues(3, 200, [hi, lo])
        # Coil 0: heartbeat
        context[0x00].setValues(1, 0, [counter % 2 == 0])
        counter += 1
        stop_evt.wait(1.0)


def main():
    ctx = _build_context()
    stop_evt = threading.Event()
    t = threading.Thread(target=_updater, args=(ctx, stop_evt), daemon=True)
    t.start()

    print(f"[modbus-sim] starting on {HOST}:{PORT}")
    try:
        StartTcpServer(context=ctx, address=(HOST, PORT))
    except KeyboardInterrupt:
        pass
    finally:
        stop_evt.set()


if __name__ == "__main__":
    main()
