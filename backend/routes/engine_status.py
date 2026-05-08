"""엔진 현황 — 통합 상태 API

모든 서브시스템(커넥터 7종, Benthos, MQTT, 파이프라인, 파일 스캐너)을
한 번의 호출로 집계하여 반환한다.
"""

import logging
from datetime import datetime

from flask import Blueprint, jsonify
from backend.database import SessionLocal
from backend.models.collector import (
    OpcuaConnector, ModbusConnector,
    MqttConnector, ApiConnector, FileCollector, DbConnector,
)
from backend.models.pipeline import Pipeline
from backend.services import benthos_manager as bm
from backend.services import mqtt_manager
from backend.services import pipeline_engine
from backend.services import file_scanner

logger = logging.getLogger(__name__)

engine_status_bp = Blueprint("engine_status", __name__, url_prefix="/api/engine")


# ── 커넥터 타입별 설정 ─────────────────────────
# (type_key, ModelClass, label, metric_field_in_dict, metric_label)
CONNECTOR_MODELS = [
    ("opcua",  OpcuaConnector,  "OPC-UA",  "pointCount",   "포인트"),
    ("modbus", ModbusConnector, "Modbus",  "pointCount",   "포인트"),
    ("mqtt",   MqttConnector,   "MQTT",    "messageCount", "수신 메시지"),
    ("api",    ApiConnector,    "API",     "requestCount", "요청 수"),
    ("file",   FileCollector,   "File",    "fileCount",    "수집 파일"),
    ("db",     DbConnector,     "DB",      "rowCount",     "수집 행"),
]


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ══════════════════════════════════════════════════
# GET /api/engine/status — 통합 상태
# ══════════════════════════════════════════════════
@engine_status_bp.route("/status", methods=["GET"])
def engine_status():
    db = SessionLocal()
    try:
        # ── 1) Benthos 스트림 (한 번만 조회) ──
        benthos_streams = {}
        try:
            benthos_streams = bm.list_streams() or {}
        except Exception as e:
            logger.warning("Benthos 스트림 조회 실패: %s", e)

        # ── 2) 커넥터 전체 ──
        connectors = []
        summary = {
            "totalConnectors": 0,
            "runningConnectors": 0,
            "stoppedConnectors": 0,
            "errorConnectors": 0,
        }

        for type_key, model_cls, label, metric_field, metric_label in CONNECTOR_MODELS:
            try:
                rows = db.query(model_cls).all()
                for r in rows:
                    d = r.to_dict()
                    sid = r.benthos_stream_id()
                    bs = benthos_streams.get(sid, {})

                    entry = {
                        "id": d.get("id"),
                        "connectorType": type_key,
                        "connectorTypeLabel": label,
                        "name": d.get("name", ""),
                        "status": d.get("status", "stopped"),
                        "enabled": d.get("enabled", True),
                        "tagCount": d.get("tagCount", 0),
                        "errorCount": d.get("errorCount", 0),
                        "lastCollectedAt": d.get("lastCollectedAt")
                                           or d.get("lastMessageAt")
                                           or d.get("lastCalledAt")
                                           or d.get("lastFileAt"),
                        "lastError": d.get("lastError", ""),
                        "metric": {
                            "label": metric_label,
                            "value": d.get(metric_field, 0) or 0,
                        },
                        "benthosStreamId": sid,
                        "benthosActive": bs.get("active", False),
                        "benthosUptime": bs.get("uptime_str", ""),
                    }
                    connectors.append(entry)

                    summary["totalConnectors"] += 1
                    st = (d.get("status") or "stopped").lower()
                    if st == "running":
                        summary["runningConnectors"] += 1
                    elif st == "error":
                        summary["errorConnectors"] += 1
                    else:
                        summary["stoppedConnectors"] += 1
            except Exception as e:
                logger.warning("커넥터 %s 조회 실패: %s", type_key, e)

        # ── 3) MQTT 브로커 상태 ──
        mqtt_status = {"connected": False, "stats": {}, "subscriptions": []}
        try:
            mqtt_status = mqtt_manager.get_status()
        except Exception as e:
            logger.warning("MQTT 상태 조회 실패: %s", e)

        summary["mqttConnected"] = mqtt_status.get("connected", False)

        # ── 4) 파이프라인 ──
        pipelines_out = []
        try:
            runtime = pipeline_engine.get_all_status()
            all_pipelines = db.query(Pipeline).order_by(Pipeline.id).all()
            for p in all_pipelines:
                rt = runtime.get(p.id, {})
                stats = rt.get("stats", {})
                pipelines_out.append({
                    "id": p.id,
                    "name": p.name,
                    "status": "running" if rt else (p.status or "stopped"),
                    "enabled": p.enabled,
                    "processedCount": stats.get("processed", p.processed_count or 0),
                    "errorCount": stats.get("errors", p.error_count or 0),
                    "droppedCount": stats.get("dropped", 0),
                    "stepCount": len(p.steps) if p.steps else 0,
                    "bindingCount": len(p.bindings) if p.bindings else 0,
                    "startedAt": stats.get("started_at", ""),
                })
        except Exception as e:
            logger.warning("파이프라인 조회 실패: %s", e)

        active_pipelines = sum(1 for p in pipelines_out if p["status"] == "running")
        summary["totalPipelines"] = len(pipelines_out)
        summary["activePipelines"] = active_pipelines

        # ── 5) 파일 스캐너 ──
        file_scanners_out = []
        try:
            file_collectors = db.query(FileCollector).all()
            for fc in file_collectors:
                fs_status = file_scanner.get_scanner_status(fc.id)
                file_scanners_out.append({
                    "collectorId": fc.id,
                    "collectorName": fc.name or f"FileCollector-{fc.id}",
                    "running": fs_status.get("running", False),
                    "trackedFiles": fs_status.get("trackedFiles", 0),
                })
        except Exception as e:
            logger.warning("파일 스캐너 조회 실패: %s", e)

        # ── 6) Benthos 요약 ──
        total_benthos = len(benthos_streams)
        active_benthos = sum(1 for v in benthos_streams.values()
                            if isinstance(v, dict) and v.get("active"))
        summary["totalBenthosStreams"] = total_benthos
        summary["activeBenthosStreams"] = active_benthos

        return _ok({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "summary": summary,
            "connectors": connectors,
            "benthosStreams": benthos_streams,
            "mqttBroker": mqtt_status,
            "pipelines": pipelines_out,
            "fileScanners": file_scanners_out,
        })

    except Exception as e:
        logger.error("엔진 상태 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()
