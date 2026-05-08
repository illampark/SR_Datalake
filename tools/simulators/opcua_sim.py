"""OPC-UA server simulator — staging only.

Endpoint: opc.tcp://0.0.0.0:4840/sdl/server/
Namespace: http://sdl.local/sim (index 2)

Nodes:
  - ns=2;s=Channel1.Device1.Temperature  (Float, sin wave)
  - ns=2;s=Channel1.Device1.Pressure     (Float, ramp)
  - ns=2;s=Channel1.Device1.Status       (String)
  - ns=2;s=Channel1.Device1.Counter      (UInt32)

Run:
  python tools/simulators/opcua_sim.py
"""
import asyncio
import math
import os
import time

from asyncua import Server, ua


ENDPOINT = os.environ.get("OPCUA_SIM_ENDPOINT", "opc.tcp://0.0.0.0:4840/sdl/server/")
NS_URI = "http://sdl.local/sim"


async def main():
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name("SDL OPC-UA Simulator")

    # anonymous policy for simplicity
    server.set_security_policy(
        [ua.SecurityPolicyType.NoSecurity]
    )

    idx = await server.register_namespace(NS_URI)

    objects = server.nodes.objects
    dev = await objects.add_object(idx, "Device1")

    temp = await dev.add_variable(
        ua.NodeId("Channel1.Device1.Temperature", idx), "Temperature", 20.0, ua.VariantType.Double,
    )
    press = await dev.add_variable(
        ua.NodeId("Channel1.Device1.Pressure", idx), "Pressure", 1013.0, ua.VariantType.Double,
    )
    status = await dev.add_variable(
        ua.NodeId("Channel1.Device1.Status", idx), "Status", "OK", ua.VariantType.String,
    )
    counter = await dev.add_variable(
        ua.NodeId("Channel1.Device1.Counter", idx), "Counter", 0, ua.VariantType.UInt32,
    )

    async with server:
        print(f"[opcua-sim] running at {ENDPOINT} (ns={idx})")
        t0 = time.time()
        c = 0
        while True:
            elapsed = time.time() - t0
            await temp.write_value(ua.Variant(20.0 + 5.0 * math.sin(elapsed), ua.VariantType.Double))
            await press.write_value(ua.Variant(1013.0 + (c % 100) * 0.1, ua.VariantType.Double))
            await status.write_value(ua.Variant("OK" if c % 7 != 0 else "WARN", ua.VariantType.String))
            await counter.write_value(ua.Variant(c & 0xFFFFFFFF, ua.VariantType.UInt32))
            c += 1
            await asyncio.sleep(0.5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
