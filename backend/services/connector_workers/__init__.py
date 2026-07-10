"""커넥터 백그라운드 워커 — Modbus / OPC-UA 실수집 thread 관리

각 running 커넥터마다 dedicated thread 가 폴링 루프를 돌리며
mqtt_manager.publish_raw() 로 sdl/raw/{type}/{id}/{tag} 토픽에 발행한다.

상태(_workers) 는 gunicorn worker 별 메모리에 보관 — 시작 요청을 처리한
gunicorn worker 가 그 thread 의 lifecycle 도 담당한다 (pipeline_engine 과
동일 패턴).
"""
import logging
import threading

from .modbus_worker import ModbusWorker
from .opcua_worker import OpcuaWorker

logger = logging.getLogger(__name__)

_WORKER_CLASSES = {
    "modbus": ModbusWorker,
    "opcua": OpcuaWorker,
}

_workers = {}                 # {(type, id): WorkerThread}
_worker_versions = {}         # {(type, id): int}  reconciler 가 사용
_lock = threading.Lock()


def _load_config_version(connector_type, connector_id):
    """DB 에서 커넥터의 config_version 값을 조회 (없으면 1)."""
    try:
        from backend.database import SessionLocal
        from backend.models.collector import OpcuaConnector, ModbusConnector, MqttConnector
        model = {"opcua": OpcuaConnector, "modbus": ModbusConnector,
                 "mqtt": MqttConnector}.get(connector_type)
        if model is None:
            return 1
        db = SessionLocal()
        try:
            row = db.query(model).get(int(connector_id))
            return int(getattr(row, "config_version", 1) or 1) if row else 1
        finally:
            db.close()
    except Exception:
        return 1


def start_worker(connector_type, connector_id):
    if connector_type not in _WORKER_CLASSES:
        return False, f"unsupported connector type: {connector_type}"
    key = (connector_type, int(connector_id))
    with _lock:
        existing = _workers.get(key)
        if existing and existing.is_alive():
            return False, "이미 실행 중"
        cls = _WORKER_CLASSES[connector_type]
        try:
            w = cls(int(connector_id))
            w.start()
        except Exception as e:
            logger.exception("worker start failed: %s/%s", connector_type, connector_id)
            return False, str(e)
        _workers[key] = w
        _worker_versions[key] = _load_config_version(connector_type, connector_id)
    return True, ""


def stop_worker(connector_type, connector_id, join_timeout=5.0):
    key = (connector_type, int(connector_id))
    with _lock:
        w = _workers.pop(key, None)
        _worker_versions.pop(key, None)
    if not w:
        return True, "이미 중지됨"
    try:
        w.stop()
        w.join(timeout=join_timeout)
    except Exception as e:
        logger.exception("worker stop error: %s", key)
        return False, str(e)
    return True, ""


def is_running(connector_type, connector_id):
    key = (connector_type, int(connector_id))
    w = _workers.get(key)
    return bool(w and w.is_alive())


def list_running():
    return [
        {"type": k[0], "id": k[1], "thread": w.name}
        for k, w in list(_workers.items())
        if w.is_alive()
    ]
