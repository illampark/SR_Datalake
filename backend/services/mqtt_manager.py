"""
MQTT Manager — Mosquitto 브로커 연결 관리, 토픽 구독/발행, 상태 모니터링

토픽 구조:
  sdl/raw/{connector_type}/{connector_id}/{tag_name}    — 커넥터에서 수집한 원본 데이터
  sdl/processed/{pipeline_id}/{tag_name}                 — 파이프라인 처리 완료 데이터
  sdl/system/status/{component}                          — 시스템 상태 메시지
"""

import json
import logging
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Module-level state ──
_client = None
_connected = False
_stop_event = threading.Event()
_lock = threading.Lock()
_subscriptions = {}          # topic_pattern → [callback, ...]
_stats = {
    "published": 0,
    "received": 0,
    "errors": 0,
    "connected_at": None,
    "last_message_at": None,
}


def _get_config():
    from backend.config import MQTT_DEFAULT_HOST, MQTT_DEFAULT_PORT
    return MQTT_DEFAULT_HOST, MQTT_DEFAULT_PORT


# ═══════════════════════════════════════════
# Connection Management
# ═══════════════════════════════════════════

def connect(host=None, port=None, client_id=None):
    """Mosquitto 브로커에 연결 (백그라운드 루프 시작)"""
    global _client, _connected
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        logger.error("paho-mqtt 패키지가 설치되지 않았습니다: pip install paho-mqtt")
        return False

    if _client and _connected:
        return True

    import os
    default_host, default_port = _get_config()
    host = host or default_host
    port = port or default_port
    if not client_id:
        client_id = f"sdl-pipeline-{os.getpid()}"

    with _lock:
        # paho-mqtt 2.x requires CallbackAPIVersion as first arg
        try:
            _client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=client_id, protocol=mqtt.MQTTv311)
        except (AttributeError, TypeError):
            # paho-mqtt 1.x fallback
            _client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        _client.on_connect = _on_connect
        _client.on_disconnect = _on_disconnect
        _client.on_message = _on_message

        try:
            _client.connect(host, port, keepalive=60)
            _client.loop_start()
            # 연결 대기 (최대 5초)
            for _ in range(50):
                if _connected:
                    break
                time.sleep(0.1)
            if _connected:
                logger.info("MQTT 브로커 연결 성공: %s:%s", host, port)
                _stats["connected_at"] = datetime.utcnow().isoformat()
            else:
                logger.warning("MQTT 브로커 연결 타임아웃: %s:%s", host, port)
            return _connected
        except Exception as e:
            logger.error("MQTT 브로커 연결 실패: %s — %s", host, e)
            _stats["errors"] += 1
            return False


def disconnect():
    """브로커 연결 해제"""
    global _client, _connected
    with _lock:
        if _client:
            _client.loop_stop()
            _client.disconnect()
            _client = None
            _connected = False
            logger.info("MQTT 브로커 연결 해제")


def is_connected():
    return _connected


def get_status():
    return {
        "connected": _connected,
        "stats": dict(_stats),
        "subscriptions": list(_subscriptions.keys()),
    }


# ═══════════════════════════════════════════
# Callbacks
# ═══════════════════════════════════════════

def _on_connect(client, userdata, flags, rc):
    global _connected
    if rc == 0:
        _connected = True
        # 기존 구독 복원
        for topic in _subscriptions:
            client.subscribe(topic, qos=1)
            logger.info("구독 복원: %s", topic)
    else:
        _connected = False
        logger.error("MQTT 연결 실패 rc=%s", rc)


def _on_disconnect(client, userdata, rc):
    global _connected
    _connected = False
    if rc != 0:
        logger.warning("MQTT 비정상 연결 끊김 rc=%s, 재연결 시도", rc)


def _on_message(client, userdata, msg):
    _stats["received"] += 1
    _stats["last_message_at"] = datetime.utcnow().isoformat()

    # 매칭되는 구독 콜백 호출
    for pattern, callbacks in _subscriptions.items():
        if _topic_matches(pattern, msg.topic):
            for cb in callbacks:
                try:
                    payload = msg.payload.decode("utf-8", errors="replace")
                    cb(msg.topic, payload)
                except Exception as e:
                    logger.error("메시지 콜백 오류 [%s]: %s", msg.topic, e)
                    _stats["errors"] += 1


def _topic_matches(pattern, topic):
    """MQTT 토픽 와일드카드 매칭 (# 과 + 지원)"""
    p_parts = pattern.split("/")
    t_parts = topic.split("/")
    for i, pp in enumerate(p_parts):
        if pp == "#":
            return True
        if i >= len(t_parts):
            return False
        if pp != "+" and pp != t_parts[i]:
            return False
    return len(p_parts) == len(t_parts)


# ═══════════════════════════════════════════
# Publish / Subscribe
# ═══════════════════════════════════════════

def publish(topic, payload, qos=1, retain=False):
    """메시지 발행"""
    if not _client or not _connected:
        logger.warning("MQTT 미연결 상태에서 publish 시도: %s", topic)
        return False
    try:
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, default=str)
        info = _client.publish(topic, payload, qos=qos, retain=retain)
        _stats["published"] += 1
        return info.rc == 0
    except Exception as e:
        logger.error("MQTT publish 오류 [%s]: %s", topic, e)
        _stats["errors"] += 1
        return False


def subscribe(topic_pattern, callback, qos=1):
    """토픽 구독 + 콜백 등록"""
    if topic_pattern not in _subscriptions:
        _subscriptions[topic_pattern] = []
    _subscriptions[topic_pattern].append(callback)

    if _client and _connected:
        _client.subscribe(topic_pattern, qos=qos)
        logger.info("토픽 구독: %s", topic_pattern)
    return True


def unsubscribe(topic_pattern):
    """토픽 구독 해제"""
    _subscriptions.pop(topic_pattern, None)
    if _client and _connected:
        _client.unsubscribe(topic_pattern)
        logger.info("토픽 구독 해제: %s", topic_pattern)


# ═══════════════════════════════════════════
# Topic Helpers
# ═══════════════════════════════════════════

def build_raw_topic(connector_type, connector_id, tag_name):
    """Raw 데이터 토픽 생성: sdl/raw/{type}/{id}/{tag}"""
    return f"sdl/raw/{connector_type}/{connector_id}/{tag_name}"


def build_processed_topic(pipeline_id, tag_name):
    """처리 완료 토픽 생성: sdl/processed/{pipeline_id}/{tag}"""
    return f"sdl/processed/{pipeline_id}/{tag_name}"


def build_raw_message(connector_type, connector_id, tag_name, value, data_type="float", unit="", quality=100):
    """표준 Raw 메시지 포맷 생성"""
    return {
        "source": {
            "connectorType": connector_type,
            "connectorId": connector_id,
            "tagName": tag_name,
        },
        "value": value,
        "dataType": data_type,
        "unit": unit,
        "quality": quality,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


def publish_raw(connector_type, connector_id, tag_name, value, **kwargs):
    """Raw 데이터를 표준 토픽에 발행"""
    topic = build_raw_topic(connector_type, connector_id, tag_name)
    msg = build_raw_message(connector_type, connector_id, tag_name, value, **kwargs)
    return publish(topic, msg)
