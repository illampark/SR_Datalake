"""엔진 성능 모니터 — 수집/처리 성능 메트릭 집계 API"""

import logging
from datetime import datetime, timedelta

from flask import Blueprint, jsonify
from sqlalchemy import func

from backend.database import SessionLocal
from backend.models.collector import (
    OpcuaConnector, ModbusConnector,
    MqttConnector, ApiConnector, FileCollector, DbConnector,
)
from backend.models.pipeline import Pipeline
from backend.models.metadata import DataLineage, TagMetadata
from backend.services import mqtt_manager
from backend.services import pipeline_engine

logger = logging.getLogger(__name__)

engine_perf_bp = Blueprint("engine_performance", __name__, url_prefix="/api/engine")


# ── 커넥터 타입별 설정 (engine_status.py 패턴 재사용) ──
CONNECTOR_MODELS = [
    ("opcua",  OpcuaConnector,  "OPC-UA",  "point_count",   "pointCount",   "포인트",    "last_collected_at"),
    ("modbus", ModbusConnector, "Modbus",  "point_count",   "pointCount",   "포인트",    "last_collected_at"),
    ("mqtt",   MqttConnector,   "MQTT",    "message_count", "messageCount", "수신 메시지", "last_message_at"),
    ("api",    ApiConnector,    "API",     "request_count", "requestCount", "요청 수",   "last_called_at"),
    ("file",   FileCollector,   "File",    "file_count",    "fileCount",    "수집 파일", "last_file_at"),
    ("db",     DbConnector,     "DB",      "row_count",     "rowCount",     "수집 행",   "last_collected_at"),
]


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ══════════════════════════════════════════════════
# GET /api/engine/performance — 성능 메트릭 스냅샷
# ══════════════════════════════════════════════════
@engine_perf_bp.route("/performance", methods=["GET"])
def get_performance():
    db = SessionLocal()
    try:
        total_collected = 0
        total_errors = 0
        active_connectors = 0
        connectors_out = []

        # ── 1) 커넥터별 성능 ──
        for type_key, model_cls, label, metric_col, metric_dict_key, metric_label, last_at_col in CONNECTOR_MODELS:
            try:
                rows = db.query(model_cls).all()
                for r in rows:
                    mc = getattr(r, metric_col, 0) or 0
                    ec = getattr(r, "error_count", 0) or 0
                    last_at = getattr(r, last_at_col, None)
                    total_collected += mc
                    total_errors += ec
                    if (getattr(r, "status", "") or "").lower() == "running":
                        active_connectors += 1

                    entry = {
                        "id": r.id,
                        "name": r.name,
                        "connectorType": type_key,
                        "connectorTypeLabel": label,
                        "status": getattr(r, "status", "stopped") or "stopped",
                        "metricCount": mc,
                        "metricLabel": metric_label,
                        "errorCount": ec,
                        "lastCollectedAt": last_at.isoformat() if last_at else None,
                        "lastError": getattr(r, "last_error", "") or "",
                    }
                    # API 커넥터는 avgResponseMs 추가
                    if type_key == "api":
                        entry["avgResponseMs"] = getattr(r, "avg_response_ms", 0) or 0
                    connectors_out.append(entry)
            except Exception as e:
                logger.warning("커넥터 %s 성능 조회 실패: %s", type_key, e)

        # ── 2) 파이프라인 성능 ──
        pipelines_out = []
        active_pipelines = 0
        try:
            runtime = pipeline_engine.get_all_status()
            all_pipelines = db.query(Pipeline).order_by(Pipeline.id).all()
            for p in all_pipelines:
                rt = runtime.get(p.id, {})
                stats = rt.get("stats", {})
                is_running = bool(rt)
                if is_running:
                    active_pipelines += 1

                processed = stats.get("processed", p.processed_count or 0)
                errors = stats.get("errors", p.error_count or 0)
                dropped = stats.get("dropped", 0)
                total_errors += errors

                pipelines_out.append({
                    "id": p.id,
                    "name": p.name,
                    "status": "running" if is_running else (p.status or "stopped"),
                    "processed": processed,
                    "errors": errors,
                    "dropped": dropped,
                    "startedAt": stats.get("started_at", ""),
                    "lastError": p.last_error or "",
                })
        except Exception as e:
            logger.warning("파이프라인 성능 조회 실패: %s", e)

        # ── 3) MQTT 통계 ──
        mqtt_stats = {}
        try:
            status = mqtt_manager.get_status()
            mqtt_stats = status.get("stats", {})
        except Exception as e:
            logger.warning("MQTT 통계 조회 실패: %s", e)

        # ── 4) 처리 지연시간 (DataLineage 최근 1시간) ──
        latency = {"avg": 0, "min": 0, "max": 0, "sampleCount": 0}
        try:
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            result = db.query(
                func.avg(DataLineage.processing_ms),
                func.min(DataLineage.processing_ms),
                func.max(DataLineage.processing_ms),
                func.count(DataLineage.id),
            ).filter(DataLineage.created_at >= one_hour_ago).first()

            if result and result[3] > 0:
                latency = {
                    "avg": round(result[0] or 0, 2),
                    "min": round(result[1] or 0, 2),
                    "max": round(result[2] or 0, 2),
                    "sampleCount": result[3],
                }
        except Exception as e:
            logger.warning("지연시간 조회 실패: %s", e)

        # ── 5) 데이터 품질 (TagMetadata 집계) ──
        quality = {"avgScore": 0, "avgNullRatio": 0, "avgAnomalyRatio": 0, "activeTagCount": 0}
        try:
            result = db.query(
                func.avg(TagMetadata.quality_score),
                func.avg(TagMetadata.null_ratio),
                func.avg(TagMetadata.anomaly_ratio),
                func.count(TagMetadata.id),
            ).filter(TagMetadata.is_active == True).first()  # noqa: E712

            if result and result[3] > 0:
                quality = {
                    "avgScore": round(result[0] or 0, 1),
                    "avgNullRatio": round(result[1] or 0, 4),
                    "avgAnomalyRatio": round(result[2] or 0, 4),
                    "activeTagCount": result[3],
                }
        except Exception as e:
            logger.warning("데이터 품질 조회 실패: %s", e)

        # ── 6) 최근 오류 목록 ──
        recent_errors = []
        for c in connectors_out:
            if c["lastError"]:
                recent_errors.append({
                    "source": c["name"],
                    "sourceType": "connector",
                    "connectorType": c["connectorType"],
                    "connectorTypeLabel": c["connectorTypeLabel"],
                    "error": c["lastError"],
                    "timestamp": c["lastCollectedAt"],
                })
        for p in pipelines_out:
            if p["lastError"]:
                recent_errors.append({
                    "source": p["name"],
                    "sourceType": "pipeline",
                    "connectorType": "pipeline",
                    "connectorTypeLabel": "파이프라인",
                    "error": p["lastError"],
                    "timestamp": p["startedAt"],
                })
        # 최대 20건, timestamp 기준 정렬 (None은 뒤로)
        recent_errors.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
        recent_errors = recent_errors[:20]

        # ── 7) 요약 ──
        total_all = total_collected + total_errors
        error_rate = round((total_errors / total_all * 100) if total_all > 0 else 0, 2)

        summary = {
            "totalCollected": total_collected,
            "totalErrors": total_errors,
            "errorRate": error_rate,
            "avgProcessingMs": latency["avg"],
            "avgQualityScore": quality["avgScore"],
            "mqttReceived": mqtt_stats.get("received", 0),
            "mqttPublished": mqtt_stats.get("published", 0),
            "activePipelines": active_pipelines,
            "activeConnectors": active_connectors,
        }

        return _ok({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "summary": summary,
            "connectors": connectors_out,
            "pipelines": pipelines_out,
            "mqttStats": mqtt_stats,
            "latency": latency,
            "quality": quality,
            "recentErrors": recent_errors,
        })

    except Exception as e:
        logger.error("성능 메트릭 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()
