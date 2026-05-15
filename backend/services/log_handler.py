"""DB 로깅 핸들러 — Python logging → PostgreSQL 자동 적재

버퍼링(50건 또는 5초)으로 DB I/O 최소화.
순환 로그 방지: 자체 flush 중 발생하는 로그는 무시.
"""

import atexit
import logging
import socket
import threading
from datetime import datetime

_HOSTNAME = socket.gethostname()

# ── 컴포넌트 이름 매핑 ──
_NAME_MAP = {
    "backend.routes.monitoring": "모니터링 API",
    "backend.routes.alarm": "알람 API",
    "backend.routes.pipeline": "파이프라인 API",
    "backend.routes.metadata": "메타데이터 API",
    "backend.routes.catalog": "카탈로그 API",
    "backend.routes.integration": "연동 API",
    "backend.routes.storage_tsdb": "TSDB API",
    "backend.routes.storage_rdbms": "RDBMS API",
    "backend.routes.storage_file": "파일스토리지 API",
    "backend.routes.storage_retention": "보관정책 API",
    "backend.routes.collector_opcua": "OPC-UA API",
    "backend.routes.collector_modbus": "Modbus API",
    "backend.routes.collector_mqtt": "MQTT API",
    "backend.routes.collector_api": "REST API 커넥터",
    "backend.routes.collector_file_watch": "파일수집 API",
    "backend.routes.collector_db": "DB커넥터 API",
    "backend.routes.engine_status": "수집엔진 상태",
    "backend.routes.engine_batch": "수집엔진 배치",
    "backend.routes.engine_buffer": "수집엔진 버퍼",
    "backend.routes.engine_performance": "수집엔진 성능",
    "backend.services.alarm_engine": "알람 엔진",
    "backend.services.pipeline_engine": "파이프라인 엔진",
    "backend.services.pipeline_modules": "파이프라인 모듈",
    "backend.services.mqtt_manager": "MQTT 매니저",
    "backend.services.retention_scheduler": "보관 스케줄러",
    "backend.services.retention_executor": "보관 실행기",
    "backend.services.metadata_tracker": "메타데이터 트래커",
    "backend.services.benthos_manager": "Benthos 매니저",
    "backend.services.file_scanner": "파일 스캐너",
}


def _component_name(logger_name):
    """logger name → 한글 컴포넌트 표시명"""
    if logger_name in _NAME_MAP:
        return _NAME_MAP[logger_name]
    # 부분 매칭: backend.routes.collector_opcua.xxx → OPC-UA API
    for key, val in _NAME_MAP.items():
        if logger_name.startswith(key):
            return val
    # 접두사 기반 기본 매핑
    if logger_name.startswith("backend.routes."):
        return logger_name.replace("backend.routes.", "").replace("_", " ").title()
    if logger_name.startswith("backend.services."):
        return logger_name.replace("backend.services.", "").replace("_", " ").title()
    if logger_name.startswith("backend."):
        return logger_name.replace("backend.", "")
    return logger_name


class DatabaseLogHandler(logging.Handler):
    """Python logging.Handler → PostgreSQL bulk insert (버퍼링)"""

    def __init__(self, buffer_size=50, flush_interval=5.0):
        super().__init__()
        self._buffer = []
        self._buffer_size = buffer_size
        self._flush_interval = flush_interval
        self._lock = threading.Lock()
        self._flushing = False          # 순환 방지 플래그
        self._timer = None
        self._start_timer()
        atexit.register(self._flush)

    def _start_timer(self):
        self._timer = threading.Timer(self._flush_interval, self._timer_flush)
        self._timer.daemon = True
        self._timer.start()

    def _timer_flush(self):
        self._flush()
        self._start_timer()

    def emit(self, record):
        # 순환 방지: flush 중 발생한 로그는 무시
        if self._flushing:
            return
        try:
            extra = {}
            if record.pathname:
                extra["pathname"] = record.pathname
            if record.lineno:
                extra["lineno"] = record.lineno
            if record.funcName:
                extra["funcName"] = record.funcName
            if record.exc_text:
                extra["traceback"] = record.exc_text
            elif record.exc_info and record.exc_info[1]:
                import traceback
                extra["traceback"] = "".join(
                    traceback.format_exception(*record.exc_info))
            # 호출 측의 logger.xxx(..., extra={...}) 로 들어온 structured 필드
            # (구조화된 필터링용 — system_log.extra JSON 에 그대로 보존)
            for key in ("pipeline_id", "step_id", "step_type",
                        "exc_class", "connector_id", "connector_type",
                        "tag_name"):
                val = getattr(record, key, None)
                if val is not None:
                    extra[key] = val

            entry = {
                "timestamp": datetime.utcfromtimestamp(record.created),
                "level": record.levelname,
                "component": _component_name(record.name),
                "logger_name": record.name,
                "message": self.format(record),
                "host": _HOSTNAME,
                "extra": extra if extra else None,
            }

            with self._lock:
                self._buffer.append(entry)
                should_flush = len(self._buffer) >= self._buffer_size
            if should_flush:
                self._flush()
        except Exception:
            self.handleError(record)

    def _flush(self):
        with self._lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()

        self._flushing = True
        try:
            from backend.database import SessionLocal
            from backend.models.system_log import SystemLog
            db = SessionLocal()
            try:
                db.bulk_insert_mappings(SystemLog, batch)
                db.commit()
            except Exception:
                db.rollback()
            finally:
                db.close()
        finally:
            self._flushing = False

    def close(self):
        if self._timer:
            self._timer.cancel()
        self._flush()
        super().close()


def setup_db_logging(level=logging.DEBUG):
    """backend.* 로거에 DatabaseLogHandler를 부착"""
    handler = DatabaseLogHandler()
    handler.setLevel(level)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    # backend 네임스페이스 전체에 적용
    backend_logger = logging.getLogger("backend")
    backend_logger.addHandler(handler)
    # 레벨이 설정되지 않은 경우 DEBUG로
    if backend_logger.level == logging.NOTSET:
        backend_logger.setLevel(level)
