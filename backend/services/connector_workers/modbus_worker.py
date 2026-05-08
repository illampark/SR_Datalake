"""Modbus polling worker (TCP/Serial) — pymodbus."""
import struct

from pymodbus.client import ModbusSerialClient, ModbusTcpClient

from backend.models.collector import ModbusConnector

from .base import BaseConnectorWorker


_FC_METHOD = {
    1: "read_coils",
    2: "read_discrete_inputs",
    3: "read_holding_registers",
    4: "read_input_registers",
}


def _decode_registers(regs, dtype):
    """register list (uint16) → typed value. big-endian word order."""
    if not regs:
        return None
    dt = (dtype or "").lower()
    if dt in ("int16", "int", "short"):
        v = regs[0]
        return v - 0x10000 if v >= 0x8000 else v
    if dt in ("uint16", "uint", "word"):
        return regs[0]
    if len(regs) >= 2:
        b = struct.pack(">HH", regs[0], regs[1])
        if dt in ("int32", "dint", "long"):
            return struct.unpack(">i", b)[0]
        if dt in ("uint32", "udint", "dword"):
            return struct.unpack(">I", b)[0]
        if dt in ("float32", "real", "float"):
            return struct.unpack(">f", b)[0]
    return regs[0]


class ModbusWorker(BaseConnectorWorker):
    connector_type = "modbus"
    model = ModbusConnector

    def _snapshot_meta(self, c, tags):
        slaves_raw = (c.slave_ids or "1")
        slaves = [int(s) for s in str(slaves_raw).split(",") if s.strip().isdigit()] or [1]
        return {
            "modbus_type": c.modbus_type or "tcp",
            "host": c.host,
            "port": c.port,
            "serial_port": c.serial_port,
            "baudrate": c.baudrate,
            "data_bits": c.data_bits,
            "parity": c.parity,
            "stop_bits": c.stop_bits,
            "slave_ids": slaves,
            "polling_interval": c.polling_interval,
            "timeout": c.timeout or 3000,
            "tags": [
                {
                    "name": t.tag_name,
                    "fc": t.function_code or 3,
                    "addr": t.register_address,
                    "count": t.register_count or 1,
                    "dtype": t.data_type or "int16",
                }
                for t in tags
            ],
        }

    def _connect(self, meta):
        timeout_s = max(meta["timeout"] / 1000.0, 0.5)
        if meta["modbus_type"] == "tcp":
            client = ModbusTcpClient(meta["host"], port=meta["port"], timeout=timeout_s)
        else:
            client = ModbusSerialClient(
                port=meta["serial_port"],
                baudrate=meta["baudrate"],
                bytesize=meta["data_bits"],
                parity=meta["parity"],
                stopbits=meta["stop_bits"],
                timeout=timeout_s,
            )
        if not client.connect():
            try:
                client.close()
            except Exception:
                pass
            target = meta.get("host") if meta["modbus_type"] == "tcp" else meta.get("serial_port")
            raise ConnectionError(f"modbus connect failed: {target}")
        self._client = client

    def _read_all_tags(self, meta):
        if not self._client or not meta.get("tags"):
            return []
        out = []
        slaves = meta["slave_ids"]
        for tag in meta["tags"]:
            method_name = _FC_METHOD.get(tag["fc"], "read_holding_registers")
            method = getattr(self._client, method_name, None)
            if method is None:
                continue
            for slave in slaves:
                try:
                    rr = method(tag["addr"], count=tag["count"], device_id=slave)
                except TypeError:
                    # pymodbus < 3.10 fallback (slave kwarg)
                    try:
                        rr = method(tag["addr"], tag["count"], slave=slave)
                    except Exception:
                        continue
                except Exception:
                    continue
                if rr is None or rr.isError():
                    continue
                if tag["fc"] in (1, 2):
                    bits = getattr(rr, "bits", []) or []
                    if not bits:
                        continue
                    val = bool(bits[0])
                else:
                    val = _decode_registers(getattr(rr, "registers", []) or [], tag["dtype"])
                tag_name = (
                    f"{tag['name']}@s{slave}" if len(slaves) > 1 else tag["name"]
                )
                out.append((tag_name, val, tag["dtype"], "", 100))
        return out
