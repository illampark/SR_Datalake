"""Multi-worker 환경 상태 정정 (reconciler).

gunicorn N-worker 각 프로세스는 자기만의 module-level state 를 갖는다
(``connector_workers._workers``, ``pipeline_engine._running_pipelines``).
따라서 stop 요청이 다른 worker 로 라우팅되면 대상 프로세스의 스레드는 살아있고
DB status 만 stopped 로 바뀌어 실제로는 폴링·publish 가 계속되는 회귀가 있다.

해결: 각 프로세스에서 daemon 스레드 하나가 주기적으로 DB 를 조회해
자기 로컬 상태와 대조 → 로컬 워커가 있는데 DB status != 'running' 이면 로컬만 종료.

- Start 는 종전대로 명시 API 요청 (라우팅된 worker 가 처리)
- Stop 은 API 가 DB 만 갱신하면 5초 이내 모든 worker 프로세스가 감지·수렴
"""
import logging
import sys
import threading
import time

# reconciler 는 python logging root config 이 없으면 로그가 사라지므로
# 자체 stderr StreamHandler 를 부여 (gunicorn error-logfile 로 흘러감).
logger = logging.getLogger("reconciler")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s reconciler %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)
    logger.propagate = False

INTERVAL = 5.0
_started = False
_start_lock = threading.Lock()


def _reconcile_connectors():
    from backend.database import SessionLocal
    from backend.models.collector import OpcuaConnector, ModbusConnector, MqttConnector
    from backend.services import connector_workers as cw
    with cw._lock:
        local_keys = list(cw._workers.keys())
        local_versions = dict(cw._worker_versions)
    if not local_keys:
        return
    db = SessionLocal()
    try:
        for ctype, cid in local_keys:
            model = {
                "opcua": OpcuaConnector,
                "modbus": ModbusConnector,
                "mqtt": MqttConnector,
            }.get(ctype)
            if model is None:
                continue
            row = db.query(model).get(cid)
            db_status = getattr(row, "status", None) if row else None
            if db_status != "running":
                cw.stop_worker(ctype, cid)
                logger.info(
                    "reconciler stopped local worker %s/%d (db_status=%s)",
                    ctype, cid, db_status,
                )
                continue
            # 버전 불일치 → 자동 재로드 (stop → start)
            db_ver = int(getattr(row, "config_version", 1) or 1)
            local_ver = int(local_versions.get((ctype, cid), 1))
            if db_ver != local_ver:
                cw.stop_worker(ctype, cid)
                ok, err = cw.start_worker(ctype, cid)
                logger.info(
                    "reconciler reloaded worker %s/%d (v%d -> v%d, %s)",
                    ctype, cid, local_ver, db_ver,
                    "ok" if ok else f"restart error: {err}",
                )
    finally:
        db.close()


def _reconcile_pipelines():
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline
    from backend.services import pipeline_engine as pe
    # pipeline_engine 은 별도 lock 이 없으므로 shallow copy 로 snapshot.
    try:
        snapshot = dict(pe._running_pipelines)
    except RuntimeError:
        return  # 다음 loop 에서 재시도
    local_ids = list(snapshot.keys())
    if not local_ids:
        return
    db = SessionLocal()
    try:
        for pid in local_ids:
            row = db.query(Pipeline).get(pid)
            db_status = getattr(row, "status", None) if row else None
            if db_status != "running":
                try:
                    pe.stop_pipeline(pid)
                    logger.info(
                        "reconciler stopped local pipeline %d (db_status=%s)",
                        pid, db_status,
                    )
                except Exception as e:
                    logger.exception("stop_pipeline(%d) failed: %s", pid, e)
                continue
            # 버전 불일치 → 자동 재로드
            db_ver = int(getattr(row, "config_version", 1) or 1)
            local_info = snapshot.get(pid) or {}
            local_ver = int(local_info.get("config_version", 1))
            if db_ver != local_ver:
                try:
                    pe.stop_pipeline(pid)
                    ok = pe.start_pipeline(pid)
                    logger.info(
                        "reconciler reloaded pipeline %d (v%d -> v%d, %s)",
                        pid, local_ver, db_ver,
                        "ok" if ok else "start returned False",
                    )
                except Exception as e:
                    logger.exception("reload pipeline %d failed: %s", pid, e)
    finally:
        db.close()


def _loop():
    while True:
        try:
            _reconcile_connectors()
        except Exception as e:
            logger.exception("reconcile connectors error: %s", e)
        try:
            _reconcile_pipelines()
        except Exception as e:
            logger.exception("reconcile pipelines error: %s", e)
        time.sleep(INTERVAL)


def start_once():
    """gunicorn worker 프로세스 하나당 한 번만 reconciler 스레드를 기동."""
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
    t = threading.Thread(target=_loop, name="reconciler", daemon=True)
    t.start()
    logger.info("reconciler started (interval=%.1fs)", INTERVAL)
