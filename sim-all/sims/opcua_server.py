"""OPC-UA 서버 시뮬레이터.

asyncua Server + 주기적 노드 값 업데이트 (SimState).
"""
import asyncio
import logging

from asyncua import Server, ua

log = logging.getLogger("sim.opcua")


async def run(state, cfg, tick_interval_sec: float):
    if not cfg.get("enabled", True):
        log.info("opcua disabled")
        return

    server = Server()
    await server.init()
    endpoint = f"opc.tcp://{cfg.get('host', '0.0.0.0')}:{cfg.get('port', 4840)}/"
    server.set_endpoint(endpoint)
    server.set_server_name("SR DataLake sim-all OPC-UA")

    uri = cfg.get("namespace", "urn:sdl:sim")
    idx = await server.register_namespace(uri)

    objects = server.get_objects_node()
    press = await objects.add_object(idx, state.asset_id)

    n_temp = await press.add_variable(idx, "Temperature", 22.0, ua.VariantType.Double)
    n_hum = await press.add_variable(idx, "Humidity", 60.0, ua.VariantType.Double)
    n_pres = await press.add_variable(idx, "Pressure", 1013.0, ua.VariantType.Double)
    n_mode = await press.add_variable(idx, "Mode", "Auto", ua.VariantType.String)
    n_cycle = await press.add_variable(idx, "TotalCycle", 0, ua.VariantType.Int64)
    n_err = await press.add_variable(idx, "LastError", "", ua.VariantType.String)
    n_fw = await press.add_variable(idx, "FirmwareVersion", state.firmware_ver, ua.VariantType.String)

    async with server:
        log.info("OPC-UA server started at %s (namespace idx=%d)", endpoint, idx)
        while True:
            snap = state.snapshot()
            await n_temp.write_value(float(snap.temperature))
            await n_hum.write_value(float(snap.humidity))
            await n_pres.write_value(float(snap.pressure))
            await n_mode.write_value(snap.mode)
            await n_cycle.write_value(int(snap.total_cycle))
            await n_err.write_value(snap.last_error)
            await asyncio.sleep(tick_interval_sec)
