"""엔진 배치 설정 — 커넥터 폴링 주기 + 파이프라인 싱크 배치 크기 관리 API"""

import logging
from flask import Blueprint, jsonify, request
from sqlalchemy.orm.attributes import flag_modified

from backend.database import SessionLocal
from backend.models.collector import (
    OpcuaConnector, ModbusConnector,
    MqttConnector, ApiConnector, FileCollector, DbConnector,
)
from backend.models.pipeline import Pipeline, PipelineStep

logger = logging.getLogger(__name__)

engine_batch_bp = Blueprint("engine_batch", __name__, url_prefix="/api/engine")


# ── 커넥터 타입 정의 ─────────────────────────
CONNECTOR_TYPES = [
    ("opcua",  OpcuaConnector,  "OPC-UA"),
    ("modbus", ModbusConnector, "Modbus"),
    ("mqtt",   MqttConnector,   "MQTT"),
    ("api",    ApiConnector,    "API"),
    ("file",   FileCollector,   "File"),
    ("db",     DbConnector,     "DB"),
]

SINK_LABELS = {
    "internal_tsdb_sink": "시계열DB",
    "internal_rdbms_sink": "관계형DB",
    "internal_file_sink": "파일스토리지",
}

SINK_TYPES = set(SINK_LABELS.keys())

DEFAULT_SINK_BATCH = {
    "internal_tsdb_sink": 100,
    "internal_rdbms_sink": 100,
    "internal_file_sink": 50,
}


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ── 헬퍼 함수 ─────────────────────────

def _get_polling_interval(type_key, model):
    if type_key in ("opcua", "modbus"):
        return model.polling_interval
    elif type_key == "file":
        return model.poll_interval
    elif type_key == "db":
        cfg = model.config or {}
        return cfg.get("pollingInterval", 60)
    elif type_key == "api":
        return model.schedule
    elif type_key == "mqtt":
        return None
    return None


def _get_polling_unit(type_key):
    if type_key in ("opcua", "modbus"):
        return "ms"
    elif type_key in ("file", "db"):
        return "초"
    elif type_key == "api":
        return "cron"
    return None


def _get_batch_size(type_key, model):
    if type_key == "db":
        cfg = model.config or {}
        return cfg.get("batchSize", 1000)
    return None


def _get_max_in_flight(type_key):
    return 1 if type_key == "db" else 64


def _find_sink_step(pipeline):
    if not pipeline.steps:
        return None
    for step in pipeline.steps:
        if step.module_type in SINK_TYPES:
            return step
    return None


# ══════════════════════════════════════════════════
# GET /api/engine/batch — 전체 배치 설정 조회
# ══════════════════════════════════════════════════
@engine_batch_bp.route("/batch", methods=["GET"])
def get_batch_settings():
    db = SessionLocal()
    try:
        # 1) 커넥터 배치 설정
        connectors = []
        for type_key, model_cls, label in CONNECTOR_TYPES:
            try:
                rows = db.query(model_cls).all()
                for r in rows:
                    d = r.to_dict()
                    connectors.append({
                        "id": d.get("id"),
                        "connectorType": type_key,
                        "connectorTypeLabel": label,
                        "name": d.get("name", ""),
                        "status": d.get("status", "stopped"),
                        "pollingInterval": _get_polling_interval(type_key, r),
                        "pollingUnit": _get_polling_unit(type_key),
                        "batchSize": _get_batch_size(type_key, r),
                        "maxInFlight": _get_max_in_flight(type_key),
                        "retries": 3,
                        "retryPeriod": "1s",
                    })
            except Exception as e:
                logger.warning("배치 설정 조회 - 커넥터 %s 오류: %s", type_key, e)

        # 2) 파이프라인 싱크 배치 설정
        pipelines = []
        try:
            for p in db.query(Pipeline).order_by(Pipeline.id).all():
                sink_step = _find_sink_step(p)
                if sink_step:
                    sc = sink_step.config or {}
                    pipelines.append({
                        "id": p.id,
                        "name": p.name,
                        "status": p.status or "stopped",
                        "sinkType": sink_step.module_type,
                        "sinkLabel": SINK_LABELS.get(sink_step.module_type, sink_step.module_type),
                        "batchSize": sc.get("batchSize", DEFAULT_SINK_BATCH.get(sink_step.module_type, 100)),
                        "writeMode": sc.get("writeMode", "batch"),
                    })
        except Exception as e:
            logger.warning("배치 설정 조회 - 파이프라인 오류: %s", e)

        return _ok({
            "connectors": connectors,
            "pipelines": pipelines,
            "defaults": {
                "pollingInterval": {
                    "opcua": 1000, "modbus": 1000,
                    "file": 30, "db": 60,
                },
                "sinkBatchSize": DEFAULT_SINK_BATCH,
                "maxInFlight": 64,
                "retries": 3,
                "retryPeriod": "1s",
            },
        })
    except Exception as e:
        logger.error("배치 설정 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ══════════════════════════════════════════════════
# PUT /api/engine/batch/connector/<id> — 커넥터 폴링 주기 수정
# ══════════════════════════════════════════════════
@engine_batch_bp.route("/batch/connector/<int:connector_id>", methods=["PUT"])
def update_connector_batch(connector_id):
    body = request.get_json(force=True)
    conn_type = body.get("connectorType", "")
    polling_val = body.get("pollingInterval")
    batch_val = body.get("batchSize")

    if not conn_type:
        return _err("connectorType 필수")

    # 타입별 모델 매핑
    model_map = {k: cls for k, cls, _ in CONNECTOR_TYPES}
    model_cls = model_map.get(conn_type)
    if not model_cls:
        return _err(f"알 수 없는 커넥터 타입: {conn_type}")

    db = SessionLocal()
    try:
        row = db.query(model_cls).filter(model_cls.id == connector_id).first()
        if not row:
            return _err(f"커넥터를 찾을 수 없습니다 (type={conn_type}, id={connector_id})", status=404)

        updated_fields = []

        if polling_val is not None:
            if conn_type in ("opcua", "modbus"):
                row.polling_interval = int(polling_val)
                updated_fields.append(f"pollingInterval={polling_val}ms")
            elif conn_type == "file":
                row.poll_interval = int(polling_val)
                updated_fields.append(f"pollInterval={polling_val}초")
            elif conn_type == "db":
                cfg = dict(row.config or {})
                cfg["pollingInterval"] = int(polling_val)
                row.config = cfg
                flag_modified(row, "config")
                updated_fields.append(f"pollingInterval={polling_val}초")
            elif conn_type == "api":
                row.schedule = str(polling_val)
                updated_fields.append(f"schedule={polling_val}")

        if batch_val is not None and conn_type == "db":
            cfg = dict(row.config or {})
            cfg["batchSize"] = int(batch_val)
            row.config = cfg
            flag_modified(row, "config")
            updated_fields.append(f"batchSize={batch_val}")

        db.commit()

        msg = f"{row.name} 배치 설정 수정: {', '.join(updated_fields)}"
        logger.info(msg)

        # 실행 중인 Benthos 스트림 재시작 안내
        warning = ""
        if (row.status or "").lower() == "running":
            warning = "커넥터가 실행 중입니다. 변경된 폴링 주기를 적용하려면 커넥터를 재시작해주세요."

        return _ok({
            "message": msg,
            "warning": warning,
            "connector": row.to_dict(),
        })
    except Exception as e:
        db.rollback()
        logger.error("커넥터 배치 설정 수정 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ══════════════════════════════════════════════════
# PUT /api/engine/batch/pipeline/<id> — 파이프라인 싱크 배치 크기 수정
# ══════════════════════════════════════════════════
@engine_batch_bp.route("/batch/pipeline/<int:pipeline_id>", methods=["PUT"])
def update_pipeline_batch(pipeline_id):
    body = request.get_json(force=True)
    batch_val = body.get("batchSize")
    write_mode = body.get("writeMode")

    if batch_val is None and write_mode is None:
        return _err("batchSize 또는 writeMode 필수")

    db = SessionLocal()
    try:
        pipeline = db.query(Pipeline).filter(Pipeline.id == pipeline_id).first()
        if not pipeline:
            return _err(f"파이프라인을 찾을 수 없습니다 (id={pipeline_id})", status=404)

        sink_step = _find_sink_step(pipeline)
        if not sink_step:
            return _err("싱크 스텝이 없습니다")

        cfg = dict(sink_step.config or {})
        updated_fields = []

        if batch_val is not None:
            cfg["batchSize"] = int(batch_val)
            updated_fields.append(f"batchSize={batch_val}")

        if write_mode is not None:
            cfg["writeMode"] = write_mode
            updated_fields.append(f"writeMode={write_mode}")

        sink_step.config = cfg
        flag_modified(sink_step, "config")
        db.commit()

        msg = f"{pipeline.name} 싱크 배치 설정 수정: {', '.join(updated_fields)}"
        logger.info(msg)

        warning = ""
        if (pipeline.status or "").lower() == "running":
            warning = "파이프라인이 실행 중입니다. 변경 사항은 다음 재시작 시 적용됩니다."

        return _ok({
            "message": msg,
            "warning": warning,
            "pipeline": pipeline.to_dict(),
        })
    except Exception as e:
        db.rollback()
        logger.error("파이프라인 배치 설정 수정 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()
