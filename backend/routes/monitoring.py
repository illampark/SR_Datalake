"""통합 모니터링 API — 운영 대시보드 + 헬스체크

시스템 리소스(psutil) + 커넥터/파이프라인/MQTT 상태 + 성능 메트릭을
운영 대시보드와 헬스체크 엔드포인트에서 반환한다.
"""

import logging
import os
import time
from datetime import datetime, timedelta

import psutil
from flask import Blueprint, jsonify, request
from sqlalchemy import func, text

from backend.database import SessionLocal
from backend.models.collector import (
    OpcuaConnector, OpcdaConnector, ModbusConnector,
    MqttConnector, ApiConnector, FileCollector, DbConnector,
    ImportCollector,
)
from backend.models.pipeline import Pipeline
from backend.models.metadata import DataLineage, TagMetadata
from backend.services import mqtt_manager
from backend.services import pipeline_engine

logger = logging.getLogger(__name__)

monitoring_bp = Blueprint("monitoring", __name__, url_prefix="/api/monitoring")

# 커넥터 타입 설정 (engine_status.py 패턴 재사용)
_CONN = [
    ("opcua",  OpcuaConnector,  "OPC-UA",  "point_count"),
    ("opcda",  OpcdaConnector,  "OPC-DA",  "point_count"),
    ("modbus", ModbusConnector, "Modbus",  "point_count"),
    ("mqtt",   MqttConnector,   "MQTT",    "message_count"),
    ("api",    ApiConnector,    "API",     "request_count"),
    ("file",   FileCollector,   "File",    "file_count"),
    ("db",     DbConnector,     "DB",      "row_count"),
    ("import", ImportCollector, "Import",  "imported_rows"),
]


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ══════════════════════════════════════════════════
# GET /api/monitoring/dashboard
# ══════════════════════════════════════════════════
@monitoring_bp.route("/dashboard", methods=["GET"])
def dashboard():
    db = SessionLocal()
    try:
        return _ok({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "system": _get_system_metrics(),
            "connectors": _get_connector_summary(db),
            "pipelines": _get_pipeline_summary(db),
            "mqtt": _get_mqtt_summary(),
            "performance": _get_performance(db),
        })
    except Exception as e:
        logger.error("대시보드 API 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# 디스크 메트릭이 가리킬 경로 — SDL 데이터가 적재되는 파티션을 우선.
# 컨테이너 내부에서 /app/static/uploads 가 호스트 DATA_ROOT/sdl-uploads 로 bind 됨.
# 그 경로의 underlying filesystem 통계를 보면 데이터 적재 가용량을 정확히 표시.
_DISK_PATH_CANDIDATES = (
    "/app/static/uploads",   # bind mount target (DATA_ROOT/sdl-uploads)
    "/data",                 # host data dir (스테이징 등)
    "/",                     # 최후 fallback
)


def _resolve_disk_path():
    for p in _DISK_PATH_CANDIDATES:
        if os.path.exists(p):
            return p
    return "/"


# ── 시스템 메트릭 (psutil) ──
def _get_system_metrics():
    try:
        mem = psutil.virtual_memory()
        disk_path = _resolve_disk_path()
        disk = psutil.disk_usage(disk_path)
        net = psutil.net_io_counters()
        boot = psutil.boot_time()
        uptime = time.time() - boot

        return {
            "cpu": {
                "percent": psutil.cpu_percent(interval=0.3),
                "count": psutil.cpu_count(),
            },
            "memory": {
                "percent": round(mem.percent, 1),
                "used_gb": round(mem.used / (1024 ** 3), 1),
                "total_gb": round(mem.total / (1024 ** 3), 1),
            },
            "disk": {
                "percent": round(disk.percent, 1),
                "used_gb": round(disk.used / (1024 ** 3), 1),
                "total_gb": round(disk.total / (1024 ** 3), 1),
                "path": disk_path,
            },
            "network": {
                "bytes_sent": net.bytes_sent,
                "bytes_recv": net.bytes_recv,
            },
            "uptime_seconds": int(uptime),
        }
    except Exception as e:
        logger.warning("시스템 메트릭 수집 실패: %s", e)
        return {"cpu": {}, "memory": {}, "disk": {}, "network": {}, "uptime_seconds": 0}


# ── 커넥터 요약 ──
def _get_connector_summary(db):
    total = running = stopped = error = 0
    by_type = []

    for type_key, model_cls, label, metric_col in _CONN:
        try:
            rows = db.query(model_cls).all()
            t = len(rows)
            r = sum(1 for x in rows if (getattr(x, "status", "") or "").lower() == "running")
            e = sum(1 for x in rows if (getattr(x, "status", "") or "").lower() == "error")
            s = t - r - e
            collected = sum(getattr(x, metric_col, 0) or 0 for x in rows)

            total += t
            running += r
            error += e
            stopped += s

            by_type.append({
                "type": type_key,
                "label": label,
                "total": t,
                "running": r,
                "stopped": s,
                "error": e,
                "collected": collected,
            })
        except Exception as ex:
            logger.warning("커넥터 %s 요약 실패: %s", type_key, ex)

    return {
        "total": total, "running": running, "stopped": stopped, "error": error,
        "byType": by_type,
    }


# ── 파이프라인 요약 ──
def _get_pipeline_summary(db):
    items = []
    total_processed = total_errors = total_dropped = 0

    try:
        runtime = pipeline_engine.get_all_status()
        all_p = db.query(Pipeline).order_by(Pipeline.id).all()
        for p in all_p:
            rt = runtime.get(p.id, {})
            stats = rt.get("stats", {})
            is_running = bool(rt)

            processed = stats.get("processed", p.processed_count or 0)
            errors = stats.get("errors", p.error_count or 0)
            dropped = stats.get("dropped", 0)
            total_processed += processed
            total_errors += errors
            total_dropped += dropped

            items.append({
                "id": p.id,
                "name": p.name,
                "status": "running" if is_running else (p.status or "stopped"),
                "processed": processed,
                "errors": errors,
                "dropped": dropped,
                "stepCount": len(p.steps) if p.steps else 0,
                "lastError": p.last_error or "",
            })
    except Exception as e:
        logger.warning("파이프라인 요약 실패: %s", e)

    running_count = sum(1 for x in items if x["status"] == "running")
    return {
        "total": len(items),
        "running": running_count,
        "totalProcessed": total_processed,
        "totalErrors": total_errors,
        "totalDropped": total_dropped,
        "items": items,
    }


# ── MQTT 요약 ──
def _get_mqtt_summary():
    try:
        return mqtt_manager.get_status()
    except Exception as e:
        logger.warning("MQTT 요약 실패: %s", e)
        return {"connected": False, "stats": {}, "subscriptions": []}


# ── 성능 + 품질 + 최근 에러 ──
def _get_performance(db):
    # 처리 지연시간 (최근 1시간)
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

    # 데이터 품질
    quality = {"avgScore": 0, "activeTagCount": 0}
    try:
        result = db.query(
            func.avg(TagMetadata.quality_score),
            func.count(TagMetadata.id),
        ).filter(TagMetadata.is_active == True).first()  # noqa: E712

        if result and result[1] > 0:
            quality = {
                "avgScore": round(result[0] or 0, 1),
                "activeTagCount": result[1],
            }
    except Exception as e:
        logger.warning("품질 조회 실패: %s", e)

    # 최근 에러 (커넥터 + 파이프라인)
    recent_errors = []
    try:
        for type_key, model_cls, label, _ in _CONN:
            rows = db.query(model_cls).filter(
                model_cls.last_error.isnot(None),
                model_cls.last_error != "",
            ).all()
            for r in rows:
                last_at = None
                for col in ("last_collected_at", "last_message_at", "last_called_at", "last_file_at", "last_imported_at"):
                    v = getattr(r, col, None)
                    if v:
                        last_at = v.isoformat()
                        break
                recent_errors.append({
                    "source": r.name,
                    "sourceType": label,
                    "error": r.last_error,
                    "timestamp": last_at,
                })

        pipelines = db.query(Pipeline).filter(
            Pipeline.last_error.isnot(None),
            Pipeline.last_error != "",
        ).all()
        for p in pipelines:
            recent_errors.append({
                "source": p.name,
                "sourceType": "파이프라인",
                "error": p.last_error,
                "timestamp": p.last_processed_at.isoformat() if p.last_processed_at else None,
            })
    except Exception as e:
        logger.warning("에러 목록 조회 실패: %s", e)

    recent_errors.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    recent_errors = recent_errors[:10]

    return {
        "latency": latency,
        "quality": quality,
        "recentErrors": recent_errors,
    }


# ══════════════════════════════════════════════════
# GET /api/monitoring/healthcheck
# POST /api/monitoring/healthcheck/run
# ══════════════════════════════════════════════════

def _run_healthcheck_raw(db):
    """컴포넌트 상태 리스트만 반환 (알람 엔진용, DB 세션 외부에서 제공)"""
    components = []
    components.append(_check_flask())
    components.append(_check_postgresql(db))
    components.append(_check_mqtt())
    components.append(_check_minio())
    components.append(_check_system_resources())
    components.extend(_check_connectors(db))
    components.extend(_check_pipelines(db))
    return components


def _run_healthcheck():
    """전체 헬스체크 수행 후 결과 반환"""
    start = time.time()
    db = SessionLocal()
    try:
        components = _run_healthcheck_raw(db)

        healthy = sum(1 for c in components if c["status"] == "healthy")
        degraded = sum(1 for c in components if c["status"] == "degraded")
        down = sum(1 for c in components if c["status"] == "down")
        stopped = sum(1 for c in components if c["status"] == "stopped")

        overall = "down" if down > 0 else "degraded" if degraded > 0 else "healthy"

        return _ok({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "overall": overall,
            "summary": {
                "healthy": healthy,
                "degraded": degraded,
                "down": down,
                "stopped": stopped,
                "total": len(components),
            },
            "elapsed_ms": round((time.time() - start) * 1000, 1),
            "components": components,
        })
    except Exception as e:
        logger.error("헬스체크 API 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@monitoring_bp.route("/healthcheck", methods=["GET"])
def healthcheck():
    return _run_healthcheck()


@monitoring_bp.route("/healthcheck/run", methods=["POST"])
def healthcheck_run():
    return _run_healthcheck()


# ── 헬스체크 개별 함수 ──

_CONN_ICONS = {
    "opcua": "fa-plug-circle-bolt", "opcda": "fa-plug",
    "modbus": "fa-microchip", "mqtt": "fa-satellite-dish",
    "api": "fa-code", "file": "fa-file-import", "db": "fa-database",
    "import": "fa-file-arrow-up",
}


def _check_flask():
    return {
        "name": "웹 서비스 서버",
        "type": "infra",
        "icon": "fa-flask",
        "status": "healthy",
        "response_ms": 0,
        "detail": "Port 5001 · PID %d" % os.getpid(),
    }


def _check_postgresql(db):
    t0 = time.time()
    try:
        db.execute(text("SELECT 1"))
        ms = round((time.time() - t0) * 1000, 1)
        from backend.config import DATABASE_URL
        # 표시용: 패스워드 숨김
        display = DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else DATABASE_URL
        return {
            "name": "PostgreSQL",
            "type": "infra",
            "icon": "fa-database",
            "status": "healthy",
            "response_ms": ms,
            "detail": display,
        }
    except Exception as e:
        ms = round((time.time() - t0) * 1000, 1)
        return {
            "name": "PostgreSQL",
            "type": "infra",
            "icon": "fa-database",
            "status": "down",
            "response_ms": ms,
            "detail": str(e)[:100],
        }


def _check_mqtt():
    try:
        connected = mqtt_manager.is_connected()
        status_info = mqtt_manager.get_status()
        stats = status_info.get("stats", {})
        subs = status_info.get("subscriptions", [])

        if connected:
            detail = "연결됨 · 구독 %d개 · 수신 %s · 발행 %s" % (
                len(subs), _fmt_num(stats.get("received", 0)), _fmt_num(stats.get("published", 0)))
            return {
                "name": "MQTT 브로커 (Mosquitto)",
                "type": "infra",
                "icon": "fa-satellite-dish",
                "status": "healthy",
                "response_ms": 0,
                "detail": detail,
            }
        else:
            return {
                "name": "MQTT 브로커 (Mosquitto)",
                "type": "infra",
                "icon": "fa-satellite-dish",
                "status": "down",
                "response_ms": 0,
                "detail": "미연결",
            }
    except Exception as e:
        return {
            "name": "MQTT 브로커 (Mosquitto)",
            "type": "infra",
            "icon": "fa-satellite-dish",
            "status": "down",
            "response_ms": 0,
            "detail": str(e)[:100],
        }


def _check_minio():
    t0 = time.time()
    try:
        from backend.config import MINIO_BUCKETS
        from backend.services.minio_client import get_minio_client, get_minio_config
        from backend.database import SessionLocal
        db = SessionLocal()
        cfg = get_minio_config(db)
        client = get_minio_client(db)
        db.close()
        ok_count = 0
        for bucket in MINIO_BUCKETS:
            if client.bucket_exists(bucket):
                ok_count += 1
        ms = round((time.time() - t0) * 1000, 1)
        total = len(MINIO_BUCKETS)

        if ok_count == total:
            status = "healthy"
        elif ok_count > 0:
            status = "degraded"
        else:
            status = "down"

        return {
            "name": "MinIO 오브젝트 스토리지",
            "type": "infra",
            "icon": "fa-box-archive",
            "status": status,
            "response_ms": ms,
            "detail": "%s:%d/%d 버킷 정상" % (cfg["endpoint"], ok_count, total),
        }
    except Exception as e:
        ms = round((time.time() - t0) * 1000, 1)
        return {
            "name": "MinIO 오브젝트 스토리지",
            "type": "infra",
            "icon": "fa-box-archive",
            "status": "down",
            "response_ms": ms,
            "detail": str(e)[:100],
        }


def _check_system_resources():
    try:
        cpu = psutil.cpu_percent(interval=0.3)
        mem = psutil.virtual_memory().percent
        disk = psutil.disk_usage("/").percent

        warnings = []
        if cpu > 90:
            warnings.append("CPU")
        if mem > 90:
            warnings.append("메모리")
        if disk > 90:
            warnings.append("디스크")

        status = "degraded" if warnings else "healthy"
        detail = "CPU %.0f%% · 메모리 %.0f%% · 디스크 %.0f%%" % (cpu, mem, disk)
        if warnings:
            detail += " · %s 임계값 초과" % "/".join(warnings)

        return {
            "name": "시스템 리소스",
            "type": "infra",
            "icon": "fa-server",
            "status": status,
            "response_ms": 0,
            "detail": detail,
        }
    except Exception as e:
        return {
            "name": "시스템 리소스",
            "type": "infra",
            "icon": "fa-server",
            "status": "down",
            "response_ms": 0,
            "detail": str(e)[:100],
        }


def _check_connectors(db):
    results = []
    for type_key, model_cls, label, metric_col in _CONN:
        try:
            rows = db.query(model_cls).all()
            t = len(rows)
            if t == 0:
                continue
            r = sum(1 for x in rows if (getattr(x, "status", "") or "").lower() == "running")
            e = sum(1 for x in rows if (getattr(x, "status", "") or "").lower() == "error")
            s = t - r - e
            collected = sum(getattr(x, metric_col, 0) or 0 for x in rows)

            if e > 0:
                status = "degraded"
            elif r > 0:
                status = "healthy"
            else:
                status = "stopped"

            detail = "%d대 중 %d 실행" % (t, r)
            if e > 0:
                detail += " · %d 오류" % e
            if collected > 0:
                detail += " · 수집 %s건" % _fmt_num(collected)

            results.append({
                "name": "%s 커넥터" % label,
                "type": "connector",
                "icon": _CONN_ICONS.get(type_key, "fa-plug"),
                "status": status,
                "response_ms": 0,
                "detail": detail,
            })
        except Exception as ex:
            logger.warning("헬스체크 커넥터 %s 실패: %s", type_key, ex)
    return results


def _check_pipelines(db):
    results = []
    try:
        runtime = pipeline_engine.get_all_status()
        all_p = db.query(Pipeline).order_by(Pipeline.id).all()
        for p in all_p:
            rt = runtime.get(p.id, {})
            stats = rt.get("stats", {})
            is_running = bool(rt)

            processed = stats.get("processed", p.processed_count or 0)
            errors = stats.get("errors", p.error_count or 0)
            step_count = len(p.steps) if p.steps else 0
            p_status = p.status or "stopped"

            if is_running:
                status = "healthy"
                detail = "실행 중 · 처리 %s건 · %d스텝" % (_fmt_num(processed), step_count)
            elif p_status == "error" or (p.last_error and errors > 0):
                status = "degraded"
                detail = "오류 · %s" % (p.last_error or "")[:60]
            else:
                status = "stopped"
                detail = "중지됨 · 처리 %s건" % _fmt_num(processed)

            results.append({
                "name": p.name,
                "type": "pipeline",
                "icon": "fa-diagram-project",
                "status": status,
                "response_ms": 0,
                "detail": detail,
            })
    except Exception as e:
        logger.warning("헬스체크 파이프라인 실패: %s", e)
    return results


def _fmt_num(n):
    """숫자를 천 단위 콤마 포맷"""
    try:
        return "{:,}".format(int(n))
    except (ValueError, TypeError):
        return str(n)


# ══════════════════════════════════════════════════
# 시스템 로그 API
# ══════════════════════════════════════════════════

from math import ceil
from backend.models.system_log import SystemLog


@monitoring_bp.route("/syslog", methods=["GET"])
def syslog_list():
    """시스템 로그 조회 (필터 + 페이지네이션)"""
    db = SessionLocal()
    try:
        level = request.args.get("level", "").strip()
        component = request.args.get("component", "").strip()
        search = request.args.get("search", "").strip()
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()
        page = max(1, int(request.args.get("page", 1)))
        size = min(200, max(10, int(request.args.get("size", 50))))

        q = db.query(SystemLog).order_by(SystemLog.timestamp.desc())

        if level:
            q = q.filter(SystemLog.level == level.upper())
        if component:
            q = q.filter(SystemLog.component == component)
        if search:
            q = q.filter(SystemLog.message.ilike("%" + search + "%"))
        if from_dt:
            try:
                q = q.filter(SystemLog.timestamp >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                # 종료일의 끝까지 포함
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                q = q.filter(SystemLog.timestamp <= to_parsed)
            except ValueError:
                pass

        total = q.count()
        logs = q.offset((page - 1) * size).limit(size).all()

        items = []
        for log in logs:
            items.append({
                "id": log.id,
                "timestamp": log.timestamp.strftime("%Y-%m-%d %H:%M:%S.") +
                             "%03d" % (log.timestamp.microsecond // 1000)
                             if log.timestamp else "",
                "level": log.level,
                "component": log.component,
                "message": log.message,
                "host": log.host or "",
                "extra": log.extra,
            })

        return _ok({
            "logs": items,
            "total": total,
            "page": page,
            "size": size,
            "totalPages": max(1, ceil(total / size)),
        })
    except Exception as e:
        logger.error("시스템 로그 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@monitoring_bp.route("/syslog/stats", methods=["GET"])
def syslog_stats():
    """시스템 로그 통계 — 레벨 분포 + 시간별 + 컴포넌트 목록"""
    db = SessionLocal()
    try:
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()

        base_q = db.query(SystemLog)
        if from_dt:
            try:
                base_q = base_q.filter(SystemLog.timestamp >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                base_q = base_q.filter(SystemLog.timestamp <= to_parsed)
            except ValueError:
                pass

        # 레벨 분포
        level_rows = base_q.with_entities(
            SystemLog.level, func.count(SystemLog.id)
        ).group_by(SystemLog.level).all()
        level_dist = {r[0]: r[1] for r in level_rows}

        # 시간별 로그량 (hour 단위)
        hour_expr = func.date_trunc("hour", SystemLog.timestamp)
        timeline_rows = base_q.with_entities(
            hour_expr.label("hour"), func.count(SystemLog.id)
        ).group_by("hour").order_by("hour").all()

        timeline = []
        for row in timeline_rows:
            h = row[0]
            timeline.append({
                "hour": h.strftime("%H:%M") if h else "00:00",
                "date": h.strftime("%Y-%m-%d") if h else "",
                "count": row[1],
            })

        # 컴포넌트 목록
        comp_rows = base_q.with_entities(
            SystemLog.component
        ).distinct().order_by(SystemLog.component).all()
        components = [r[0] for r in comp_rows]

        # 총 건수
        total_count = base_q.count()

        return _ok({
            "levelDist": level_dist,
            "timeline": timeline,
            "components": components,
            "totalCount": total_count,
        })
    except Exception as e:
        logger.error("시스템 로그 통계 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@monitoring_bp.route("/syslog/cleanup", methods=["DELETE"])
def syslog_cleanup():
    """N일 이전 로그 삭제"""
    db = SessionLocal()
    try:
        days = max(1, int(request.args.get("days", 30)))
        cutoff = datetime.utcnow() - timedelta(days=days)
        deleted = db.query(SystemLog).filter(SystemLog.timestamp < cutoff).delete()
        db.commit()
        return _ok({"deleted": deleted, "cutoff": cutoff.isoformat() + "Z"})
    except Exception as e:
        db.rollback()
        logger.error("시스템 로그 정리 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@monitoring_bp.route("/syslog/export", methods=["GET"])
def syslog_export():
    """시스템 로그 CSV 내보내기"""
    import csv
    import io
    from flask import Response

    db = SessionLocal()
    try:
        level = request.args.get("level", "").strip()
        component = request.args.get("component", "").strip()
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()

        q = db.query(SystemLog).order_by(SystemLog.timestamp.desc())
        if level:
            q = q.filter(SystemLog.level == level.upper())
        if component:
            q = q.filter(SystemLog.component == component)
        if from_dt:
            try:
                q = q.filter(SystemLog.timestamp >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                q = q.filter(SystemLog.timestamp <= to_parsed)
            except ValueError:
                pass

        logs = q.limit(10000).all()

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["타임스탬프", "레벨", "컴포넌트", "메시지", "호스트"])
        for log in logs:
            writer.writerow([
                log.timestamp.strftime("%Y-%m-%d %H:%M:%S") if log.timestamp else "",
                log.level,
                log.component,
                log.message,
                log.host or "",
            ])

        filename = "syslog_%s.csv" % datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return Response(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=%s" % filename},
        )
    except Exception as e:
        logger.error("시스템 로그 내보내기 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ══════════════════════════════════════════════════
# 감사 로그 API
# ══════════════════════════════════════════════════

from backend.models.audit import AuditLog


# ──────────────────────────────────────────────
# AUDIT-01: GET /api/monitoring/audit — 감사 로그 목록
# ──────────────────────────────────────────────
@monitoring_bp.route("/audit", methods=["GET"])
def audit_list():
    """감사 로그 조회 (필터 + 페이지네이션)"""
    db = SessionLocal()
    try:
        username = request.args.get("username", "").strip()
        action_type = request.args.get("action_type", "").strip()
        result_filter = request.args.get("result", "").strip()
        search = request.args.get("search", "").strip()
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()
        page = max(1, int(request.args.get("page", 1)))
        size = min(200, max(10, int(request.args.get("size", 50))))

        q = db.query(AuditLog).order_by(AuditLog.timestamp.desc())

        if username:
            q = q.filter(AuditLog.username == username)
        if action_type:
            q = q.filter(AuditLog.action_type == action_type)
        if result_filter:
            q = q.filter(AuditLog.result == result_filter)
        if search:
            like = "%" + search + "%"
            q = q.filter(
                (AuditLog.target_name.ilike(like)) |
                (AuditLog.action.ilike(like))
            )
        if from_dt:
            try:
                q = q.filter(AuditLog.timestamp >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                q = q.filter(AuditLog.timestamp <= to_parsed)
            except ValueError:
                pass

        total = q.count()
        logs = q.offset((page - 1) * size).limit(size).all()

        return _ok({
            "logs": [log.to_dict() for log in logs],
            "total": total,
            "page": page,
            "size": size,
            "totalPages": max(1, ceil(total / size)),
        })
    except Exception as e:
        logger.error("감사 로그 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# AUDIT-02: GET /api/monitoring/audit/stats — 감사 로그 통계
# ──────────────────────────────────────────────
@monitoring_bp.route("/audit/stats", methods=["GET"])
def audit_stats():
    """감사 로그 통계 — 총건수, 성공/실패, 활성 사용자, action_type 분포, 타임라인"""
    db = SessionLocal()
    try:
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()

        base_q = db.query(AuditLog)
        if from_dt:
            try:
                base_q = base_q.filter(AuditLog.timestamp >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                base_q = base_q.filter(AuditLog.timestamp <= to_parsed)
            except ValueError:
                pass

        # 총 건수
        total_count = base_q.count()

        # 결과별 집계
        result_rows = base_q.with_entities(
            AuditLog.result, func.count(AuditLog.id)
        ).group_by(AuditLog.result).all()
        result_dist = {r[0]: r[1] for r in result_rows}

        # 활성 사용자 수
        active_users = base_q.with_entities(
            func.count(func.distinct(AuditLog.username))
        ).scalar() or 0

        # action_type 분포
        type_rows = base_q.with_entities(
            AuditLog.action_type, func.count(AuditLog.id)
        ).group_by(AuditLog.action_type).all()
        type_dist = {r[0]: r[1] for r in type_rows}

        # 시간별 타임라인
        hour_expr = func.date_trunc("hour", AuditLog.timestamp)
        timeline_rows = base_q.with_entities(
            hour_expr.label("hour"), func.count(AuditLog.id)
        ).group_by("hour").order_by("hour").all()
        timeline = []
        for row in timeline_rows:
            h = row[0]
            timeline.append({
                "hour": h.strftime("%H:%M") if h else "00:00",
                "date": h.strftime("%Y-%m-%d") if h else "",
                "count": row[1],
            })

        # 사용자 목록 (필터 드롭다운용)
        user_rows = base_q.with_entities(
            AuditLog.username
        ).distinct().order_by(AuditLog.username).all()
        users = [r[0] for r in user_rows]

        return _ok({
            "totalCount": total_count,
            "success": result_dist.get("success", 0),
            "failure": result_dist.get("failure", 0),
            "activeUsers": active_users,
            "typeDist": type_dist,
            "timeline": timeline,
            "users": users,
        })
    except Exception as e:
        logger.error("감사 로그 통계 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# AUDIT-03: GET /api/monitoring/audit/export — CSV 내보내기
# ──────────────────────────────────────────────
@monitoring_bp.route("/audit/export", methods=["GET"])
def audit_export():
    """감사 로그 CSV 내보내기"""
    import csv as csv_mod
    import io as io_mod
    from flask import Response as FlaskResponse

    db = SessionLocal()
    try:
        username = request.args.get("username", "").strip()
        action_type = request.args.get("action_type", "").strip()
        result_filter = request.args.get("result", "").strip()
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()

        q = db.query(AuditLog).order_by(AuditLog.timestamp.desc())
        if username:
            q = q.filter(AuditLog.username == username)
        if action_type:
            q = q.filter(AuditLog.action_type == action_type)
        if result_filter:
            q = q.filter(AuditLog.result == result_filter)
        if from_dt:
            try:
                q = q.filter(AuditLog.timestamp >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                q = q.filter(AuditLog.timestamp <= to_parsed)
            except ValueError:
                pass

        logs = q.limit(10000).all()

        buf = io_mod.StringIO()
        writer = csv_mod.writer(buf)
        writer.writerow(["타임스탬프", "사용자", "작업유형", "작업", "대상유형", "대상", "IP주소", "결과"])
        for log in logs:
            writer.writerow([
                log.timestamp.strftime("%Y-%m-%d %H:%M:%S") if log.timestamp else "",
                log.username,
                log.action_type,
                log.action,
                log.target_type,
                log.target_name,
                log.ip_address,
                log.result,
            ])

        filename = "audit_log_%s.csv" % datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return FlaskResponse(
            buf.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=%s" % filename},
        )
    except Exception as e:
        logger.error("감사 로그 내보내기 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# AUDIT-04: DELETE /api/monitoring/audit/cleanup — 보관 기간 지난 로그 삭제
# ──────────────────────────────────────────────
@monitoring_bp.route("/audit/cleanup", methods=["DELETE"])
def audit_cleanup():
    """N일 이전 감사 로그 삭제"""
    db = SessionLocal()
    try:
        days = max(1, int(request.args.get("days", 90)))
        cutoff = datetime.utcnow() - timedelta(days=days)
        deleted = db.query(AuditLog).filter(AuditLog.timestamp < cutoff).delete()
        db.commit()
        return _ok({"deleted": deleted, "cutoff": cutoff.isoformat() + "Z"})
    except Exception as e:
        db.rollback()
        logger.error("감사 로그 정리 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()
