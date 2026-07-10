"""sim-all — 6종 커넥터 시뮬레이터 통합 진입점.

각 sim 을 threading.Thread (또는 asyncio task) 로 병렬 실행.
공유 SimState 를 참조해 데이터 일관성 유지.
"""
import asyncio
import logging
import os
import signal
import threading
import time

import yaml

from state import SimState
from sims import opcua_server, modbus_server, mqtt_publisher, api_server, db_writer, file_writer

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sim.main")


def _load_config():
    with open("/app/config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    # env override
    def envset(section, key, env, cast=str):
        v = os.environ.get(env)
        if v is None:
            return
        cfg.setdefault(section, {})[key] = cast(v)
    envset("mqtt", "broker_host", "MQTT_HOST")
    envset("mqtt", "broker_port", "MQTT_PORT", int)
    envset("mqtt", "topic", "MQTT_TOPIC")
    envset("db", "host", "DB_HOST")
    envset("db", "port", "DB_PORT", int)
    envset("db", "database", "DB_NAME")
    envset("db", "user", "DB_USER")
    envset("db", "password", "DB_PASS")
    envset("db", "schema", "DB_SCHEMA")
    envset("file", "minio_host", "MINIO_HOST")
    envset("file", "minio_sftp_port", "MINIO_SFTP_PORT", int)
    envset("file", "minio_user", "MINIO_USER")
    envset("file", "minio_password", "MINIO_PASS")
    envset("file", "bucket", "MINIO_BUCKET")
    if os.environ.get("ASSET_ID"):
        cfg["asset_id"] = os.environ["ASSET_ID"]
    if os.environ.get("TICK_INTERVAL_SEC"):
        cfg["tick_interval_sec"] = float(os.environ["TICK_INTERVAL_SEC"])
    return cfg


def _tick_loop(state, interval):
    """SimState 를 주기적으로 갱신 (모든 sim 이 이 값을 관측)."""
    while True:
        state.tick()
        time.sleep(interval)


async def _async_all(state, cfg):
    """OPC-UA + Modbus 는 asyncio 서버."""
    await asyncio.gather(
        opcua_server.run(state, cfg.get("opcua", {}), cfg.get("tick_interval_sec", 1.0)),
        modbus_server.run(state, cfg.get("modbus", {}), cfg.get("tick_interval_sec", 1.0)),
    )


def main():
    cfg = _load_config()
    asset_id = cfg.get("asset_id", "PRESS-01")
    tick_interval = float(cfg.get("tick_interval_sec", 1.0))
    log.info("sim-all starting (asset_id=%s, tick=%.1fs)", asset_id, tick_interval)

    state = SimState(asset_id=asset_id, history_max=int(cfg.get("api", {}).get("history_max_records", 3600)))

    # 상태 tick 스레드
    threading.Thread(target=_tick_loop, args=(state, tick_interval), daemon=True, name="tick").start()

    # 스레드 기반 sim
    threading.Thread(target=mqtt_publisher.run, args=(state, cfg.get("mqtt", {})), daemon=True, name="mqtt").start()
    threading.Thread(target=api_server.run, args=(state, cfg.get("api", {})), daemon=True, name="api").start()
    threading.Thread(target=db_writer.run, args=(state, cfg.get("db", {})), daemon=True, name="db").start()
    threading.Thread(target=file_writer.run, args=(state, cfg.get("file", {})), daemon=True, name="file").start()

    # asyncio 기반 sim (OPC-UA + Modbus)
    stop = threading.Event()

    def _sig_handler(signum, frame):
        log.info("signal %d received, exiting", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT, _sig_handler)

    try:
        asyncio.run(_async_all(state, cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
