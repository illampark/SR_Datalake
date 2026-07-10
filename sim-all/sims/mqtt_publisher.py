"""MQTT publisher 시뮬레이터.

paho-mqtt 로 sdl-mosquitto 에 주기적 publish.
Payload 는 SDL AAS 매핑을 감안한 nested 구조:
  {
    asset_id, timestamp,
    operational: {mode, total_cycle, last_error},
    timeSeries:  {temperature, humidity, pressure},
    software:    {firmware_ver}       # 첫 발행에만 (include_static=true 시)
  }
"""
import json
import logging
import threading
import time

import paho.mqtt.client as mqtt

log = logging.getLogger("sim.mqtt")


def run(state, cfg):
    if not cfg.get("enabled", True):
        log.info("mqtt disabled")
        return

    host = cfg.get("broker_host", "sdl-mosquitto")
    port = int(cfg.get("broker_port", 1883))
    topic = cfg.get("topic", "sdl/sim/press01/A")
    interval = float(cfg.get("publish_interval_sec", 2.0))
    include_static = bool(cfg.get("include_static", True))

    client = mqtt.Client(client_id="sim-all-mqtt", callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    while True:
        try:
            client.connect(host, port, keepalive=60)
            break
        except Exception as e:
            log.warning("mqtt connect fail (%s), retry in 3s", e)
            time.sleep(3)
    client.loop_start()
    log.info("MQTT publisher connected to %s:%d -> %s", host, port, topic)

    first = True
    while True:
        snap = state.snapshot()
        payload = {
            "asset_id": snap.asset_id,
            "timestamp": snap.ts,
            "operational": {
                "mode": snap.mode,
                "total_cycle": snap.total_cycle,
                "last_error": snap.last_error,
            },
            "timeSeries": {
                "temperature": snap.temperature,
                "humidity": snap.humidity,
                "pressure": snap.pressure,
            },
        }
        if first and include_static:
            payload["software"] = {"firmware_ver": snap.firmware_ver}
            first = False
        try:
            client.publish(topic, json.dumps(payload), qos=1)
        except Exception as e:
            log.warning("mqtt publish error: %s", e)
        time.sleep(interval)
