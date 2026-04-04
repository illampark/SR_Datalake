"""
Pipeline Engine — MQTT 기반 실시간 데이터 파이프라인 엔진

흐름:
  1. 파이프라인 시작 → PipelineBinding에 따라 MQTT 토픽 구독
  2. 메시지 수신 → PipelineStep 순서대로 모듈 체인 실행
  3. 처리 결과 → output 토픽에 발행 + 메타데이터 갱신
"""

import json
import logging
import time
import copy
from datetime import datetime

from backend.services import mqtt_manager
from backend.services.pipeline_modules import MODULE_REGISTRY, SINK_REGISTRY, process_message, flush_all_sink_buffers
from backend.services import metadata_tracker

logger = logging.getLogger(__name__)

# 실행 중인 파이프라인 상태
_running_pipelines = {}  # pipeline_id → {"steps": [...], "bindings": [...], "stats": {...}}

# ── 커넥터명 캐시 ──
_connector_name_cache = {}


def _resolve_connector_name(connector_type, connector_id):
    """커넥터 타입+ID로 커넥터 이름 조회 (캐시)"""
    key = (connector_type, connector_id)
    if key in _connector_name_cache:
        return _connector_name_cache[key]

    from backend.database import SessionLocal
    from backend.models.collector import (
        OpcuaConnector, OpcdaConnector, ModbusConnector,
        MqttConnector, ApiConnector, FileCollector, DbConnector,
    )
    MODEL_MAP = {
        "opcua": OpcuaConnector, "opcda": OpcdaConnector,
        "modbus": ModbusConnector, "mqtt": MqttConnector,
        "api": ApiConnector, "file": FileCollector, "db": DbConnector,
    }
    model_cls = MODEL_MAP.get(connector_type)
    name = ""
    if model_cls:
        db = SessionLocal()
        try:
            row = db.query(model_cls.name).filter(model_cls.id == connector_id).first()
            name = row[0] if row else ""
        finally:
            db.close()
    _connector_name_cache[key] = name
    return name


def start_pipeline(pipeline_id):
    """파이프라인 시작 — DB에서 설정 로드 후 MQTT 구독"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline, PipelineStep, PipelineBinding

    if pipeline_id in _running_pipelines:
        logger.info("파이프라인 %s 이미 실행 중", pipeline_id)
        return True

    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if not p:
            logger.error("파이프라인 %s 없음", pipeline_id)
            return False

        steps = db.query(PipelineStep).filter_by(
            pipeline_id=pipeline_id
        ).order_by(PipelineStep.step_order).all()

        bindings = db.query(PipelineBinding).filter_by(
            pipeline_id=pipeline_id, enabled=True
        ).all()

        if not bindings:
            logger.warning("파이프라인 %s: 바인딩 없음", pipeline_id)
            return False

        # MQTT 연결 확인
        if not mqtt_manager.is_connected():
            mqtt_manager.connect()
            if not mqtt_manager.is_connected():
                logger.error("MQTT 브로커 연결 불가")
                return False

        # 스텝 정보 캐시 (처리 모듈과 싱크 분리)
        step_configs = []
        sink_configs = []
        for s in steps:
            if s.enabled:
                cfg = dict(s.config or {})
                cfg["_pipeline_id"] = pipeline_id
                entry = {"module_type": s.module_type, "config": cfg}
                if s.module_type in SINK_REGISTRY:
                    sink_configs.append(entry)
                else:
                    step_configs.append(entry)

        _running_pipelines[pipeline_id] = {
            "name": p.name,
            "steps": step_configs,
            "sinks": sink_configs,
            "bindings": [{"type": b.connector_type, "id": b.connector_id, "filter": b.tag_filter} for b in bindings],
            "stats": {
                "processed": 0,
                "errors": 0,
                "dropped": 0,
                "started_at": datetime.utcnow().isoformat(),
            },
        }

        # 바인딩별 MQTT 토픽 구독
        for b in bindings:
            topic = f"sdl/raw/{b.connector_type}/{b.connector_id}/#"
            if b.tag_filter and b.tag_filter != "*":
                topic = f"sdl/raw/{b.connector_type}/{b.connector_id}/{b.tag_filter}"

            def make_handler(pid):
                def handler(t, payload):
                    _handle_message(pid, t, payload)
                return handler

            mqtt_manager.subscribe(topic, make_handler(pipeline_id))
            logger.info("파이프라인 %s: 구독 %s", pipeline_id, topic)

        logger.info("파이프라인 %s (%s) 시작 — %d 스텝, %d 바인딩",
                     pipeline_id, p.name, len(step_configs), len(bindings))
        return True
    except Exception as e:
        logger.error("파이프라인 %s 시작 실패: %s", pipeline_id, e)
        return False
    finally:
        db.close()


def stop_pipeline(pipeline_id):
    """파이프라인 정지 — MQTT 구독 해제"""
    info = _running_pipelines.pop(pipeline_id, None)
    if not info:
        return

    # 싱크 버퍼 플러시 (배치 모드 잔여 데이터 기록)
    try:
        flush_all_sink_buffers()
    except Exception as e:
        logger.warning("파이프라인 %s: 싱크 버퍼 플러시 실패 — %s", pipeline_id, e)

    # 바인딩별 토픽 구독 해제
    for b in info.get("bindings", []):
        topic = f"sdl/raw/{b['type']}/{b['id']}/#"
        mqtt_manager.unsubscribe(topic)

    logger.info("파이프라인 %s 정지", pipeline_id)

    # DB 상태 갱신
    _update_pipeline_db(pipeline_id, "stopped", info["stats"])


def get_pipeline_status(pipeline_id):
    """파이프라인 런타임 상태"""
    info = _running_pipelines.get(pipeline_id)
    if not info:
        return {"running": False}
    return {
        "running": True,
        "name": info["name"],
        "stats": info["stats"],
        "stepCount": len(info["steps"]),
        "bindingCount": len(info["bindings"]),
    }


def get_all_status():
    """모든 실행 중 파이프라인 상태"""
    result = {}
    for pid, info in _running_pipelines.items():
        result[pid] = {
            "name": info["name"],
            "stats": info["stats"],
        }
    return result


# ══════════════════════════════════════════════
# Internal: Message Processing
# ══════════════════════════════════════════════

def _handle_message(pipeline_id, topic, payload):
    """MQTT 메시지 수신 → 모듈 체인 실행"""
    info = _running_pipelines.get(pipeline_id)
    if not info:
        return

    start_time = time.time()

    try:
        # 페이로드 파싱
        if isinstance(payload, str):
            message = json.loads(payload)
        elif isinstance(payload, bytes):
            message = json.loads(payload.decode("utf-8"))
        else:
            message = payload

        # 원본 보존
        original = copy.deepcopy(message)

        # 모듈 체인 실행
        steps_applied = []
        for step in info["steps"]:
            module_type = step["module_type"]
            config = step["config"]

            message = process_message(message, module_type, config)
            if message is None:
                # 메시지 드롭
                info["stats"]["dropped"] += 1
                return

            steps_applied.append(module_type)

        # 처리 완료 → output 토픽 발행
        source = message.get("source", {})
        tag_name = source.get("tagName", "unknown")
        output_topic = mqtt_manager.build_processed_topic(pipeline_id, tag_name)
        mqtt_manager.publish(output_topic, message)

        # 싱크 실행 (처리 완료된 메시지를 각 싱크로 전달)
        for sink_step in info.get("sinks", []):
            try:
                sink_func = SINK_REGISTRY.get(sink_step["module_type"])
                if sink_func:
                    sink_func(copy.deepcopy(message), sink_step["config"])
            except Exception as se:
                logger.warning("파이프라인 %s: 싱크 %s 오류 — %s",
                               pipeline_id, sink_step["module_type"], se)

        # 통계 갱신
        info["stats"]["processed"] += 1
        elapsed_ms = (time.time() - start_time) * 1000

        # 메타데이터 갱신 (비동기적)
        try:
            metadata_tracker.upsert_tag_metadata(
                connector_type=source.get("connectorType", ""),
                connector_id=source.get("connectorId", 0),
                connector_name=_resolve_connector_name(
                    source.get("connectorType", ""),
                    source.get("connectorId", 0),
                ),
                tag_name=tag_name,
                value=message.get("value"),
                data_type=message.get("dataType", "float"),
                unit=message.get("unit", ""),
            )

            # 계보 기록 (100건 배치 단위)
            if info["stats"]["processed"] % 100 == 0:
                metadata_tracker.record_lineage(
                    source_connector_type=source.get("connectorType", ""),
                    source_connector_id=source.get("connectorId", 0),
                    source_tag=tag_name,
                    pipeline_id=pipeline_id,
                    pipeline_name=info["name"],
                    steps_applied=steps_applied,
                    destination_type="mqtt",
                    destination_target=output_topic,
                    record_count=100,
                    processing_ms=elapsed_ms,
                )
        except Exception as e:
            logger.warning("메타데이터 갱신 실패: %s", e)

    except json.JSONDecodeError:
        info["stats"]["errors"] += 1
        logger.warning("파이프라인 %s: JSON 파싱 오류 [%s]", pipeline_id, topic)
    except Exception as e:
        info["stats"]["errors"] += 1
        logger.error("파이프라인 %s: 처리 오류 [%s] — %s", pipeline_id, topic, e)

        # DB에 에러 기록
        _update_pipeline_error(pipeline_id, str(e))


def _update_pipeline_db(pipeline_id, status, stats):
    """파이프라인 DB 상태 갱신"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline
    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if p:
            p.status = status
            p.processed_count = stats.get("processed", 0)
            p.error_count = stats.get("errors", 0)
            p.last_processed_at = datetime.utcnow()
            db.commit()
    except Exception as e:
        db.rollback()
        logger.error("파이프라인 DB 갱신 실패: %s", e)
    finally:
        db.close()


def _update_pipeline_error(pipeline_id, error_msg):
    """파이프라인 에러 기록"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline
    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if p:
            p.last_error = error_msg
            p.error_count = (p.error_count or 0) + 1
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()
