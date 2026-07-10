"""Modbus TCP 서버 시뮬레이터.

pymodbus 3.x async server + 주기적 register 값 업데이트.

Register map (HR, slave=1):
  addr 100 : temperature * 10 (int16, °C x10)
  addr 101 : humidity * 10    (int16, %RH x10)
  addr 102 : pressure         (int16, hPa)
  addr 103 : total_cycle low  (uint16)
  addr 104 : total_cycle high (uint16)
  addr 105 : mode code        (0=Auto, 1=Manual, 2=Stop)
  addr 106 : error code       (0=none, 101=E101, 203=E203, 504=W504)
"""
import asyncio
import logging

from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext, ModbusDeviceContext
from pymodbus.server import StartAsyncTcpServer

log = logging.getLogger("sim.modbus")

_MODE_CODE = {"Auto": 0, "Manual": 1, "Stop": 2}
_ERROR_CODE = {"": 0, "E101": 101, "E203": 203, "W504": 504}


def _pack_state(state):
    snap = state.snapshot()
    regs = [0] * 200
    regs[100] = int(round(snap.temperature * 10))
    regs[101] = int(round(snap.humidity * 10))
    regs[102] = int(round(snap.pressure))
    regs[103] = snap.total_cycle & 0xFFFF
    regs[104] = (snap.total_cycle >> 16) & 0xFFFF
    regs[105] = _MODE_CODE.get(snap.mode, 0)
    regs[106] = _ERROR_CODE.get(snap.last_error, 0)
    return regs


async def run(state, cfg, tick_interval_sec: float):
    if not cfg.get("enabled", True):
        log.info("modbus disabled")
        return

    host = cfg.get("host", "0.0.0.0")
    port = int(cfg.get("port", 5020))

    hr = ModbusSequentialDataBlock(0, [0] * 300)
    device = ModbusDeviceContext(hr=hr, ir=hr)
    ctx = ModbusServerContext(devices=device, single=True)

    async def updater():
        while True:
            regs = _pack_state(state)
            device.setValues(3, 0, regs)     # HR
            device.setValues(4, 0, regs)     # IR
            await asyncio.sleep(tick_interval_sec)

    log.info("Modbus TCP server starting at %s:%d", host, port)
    updater_task = asyncio.create_task(updater())
    try:
        await StartAsyncTcpServer(ctx, address=(host, port))
    finally:
        updater_task.cancel()
