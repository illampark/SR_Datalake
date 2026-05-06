"""API Gateway 관리 라우트"""

import logging
import secrets
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import func, desc, case, Integer as SAInt
from sqlalchemy.sql import cast

from backend.database import SessionLocal
from backend.models.gateway import ApiAccessLog, ApiKey
from backend.models.user import AdminSetting
from backend.services.api_access_logger import cleanup_old_logs
from backend.services.system_settings import get_default_page_size

logger = logging.getLogger(__name__)

gateway_bp = Blueprint("gateway", __name__, url_prefix="/api/gateway")

# Blueprint 한글 라벨
_BP_LABELS = {
    "tsdb": "시계열 DB",
    "rdbms": "RDBMS",
    "file_storage": "파일 스토리지",
    "mqtt": "MQTT 커넥터",
    "db_connector": "DB 커넥터",
    "file_watch": "파일 수집기",
    "opcua": "OPC-UA 커넥터",
    "opcda": "OPC-DA 커넥터",
    "modbus": "Modbus 커넥터",
    "api_connector": "API 커넥터",
    "retention": "보관 정책",
    "pipeline": "파이프라인",
    "metadata": "메타데이터",
    "catalog": "데이터 카탈로그",
    "engine_status": "엔진 상태",
    "engine_batch": "배치 처리",
    "engine_buffer": "버퍼 관리",
    "engine_perf": "엔진 성능",
    "integration": "외부 연동",
    "monitoring": "모니터링",
    "alarm": "알람",
    "admin": "시스템 관리",
    "backup": "백업/복구",
    "gateway": "API Gateway",
}

# Gateway 설정 키
_GW_SETTINGS = {
    "corsOrigins": "gateway.cors_origins",
    "accessLogRetentionDays": "gateway.access_log_retention_days",
}
_GW_DEFAULTS = {
    "gateway.cors_origins": "*",
    "gateway.access_log_retention_days": "7",
}


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ──────────────────────────────────────────────
#  UC-1: API 엔드포인트 현황
# ──────────────────────────────────────────────

@gateway_bp.route("/routes", methods=["GET"])
def list_routes():
    """Flask url_map 에서 /api/* 엔드포인트 자동 추출"""
    routes = []
    for rule in current_app.url_map.iter_rules():
        if not rule.rule.startswith("/api/"):
            continue
        methods = sorted(m for m in rule.methods if m not in ("HEAD", "OPTIONS"))
        bp = rule.endpoint.split(".")[0] if "." in rule.endpoint else ""
        routes.append({
            "path": rule.rule,
            "methods": methods,
            "endpoint": rule.endpoint,
            "blueprint": bp,
            "blueprintLabel": _BP_LABELS.get(bp, bp),
            "hasParams": bool(rule.arguments),
        })
    routes.sort(key=lambda r: r["path"])
    return _ok({"routes": routes, "total": len(routes)})


# ──────────────────────────────────────────────
#  UC-2: 접근 로그
# ──────────────────────────────────────────────

@gateway_bp.route("/access-log", methods=["GET"])
def get_access_log():
    """접근 로그 페이지네이션 조회"""
    page = request.args.get("page", 1, type=int)
    size = request.args.get("size", get_default_page_size(), type=int)
    method_filter = request.args.get("method", "")
    status_filter = request.args.get("status", "")
    path_filter = request.args.get("path", "")

    db = SessionLocal()
    try:
        q = db.query(ApiAccessLog)
        if method_filter:
            q = q.filter(ApiAccessLog.method == method_filter.upper())
        if status_filter == "success":
            q = q.filter(ApiAccessLog.status_code < 400)
        elif status_filter == "error":
            q = q.filter(ApiAccessLog.status_code >= 400)
        if path_filter:
            q = q.filter(ApiAccessLog.path.ilike(f"%{path_filter}%"))

        total = q.count()
        rows = (q.order_by(desc(ApiAccessLog.timestamp))
                 .offset((page - 1) * size).limit(size).all())
        return _ok({
            "items": [r.to_dict() for r in rows],
            "total": total, "page": page, "size": size,
        })
    finally:
        db.close()


@gateway_bp.route("/stats", methods=["GET"])
def get_traffic_stats():
    """시간대별 트래픽 통계"""
    period = request.args.get("period", "24h")
    hours = {"6h": 6, "12h": 12, "24h": 24}.get(period, 24)
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    db = SessionLocal()
    try:
        # 시간대별 집계 (PostgreSQL date_trunc)
        hour_col = func.date_trunc("hour", ApiAccessLog.timestamp).label("hb")
        error_case = case(
            (ApiAccessLog.status_code >= 400, 1), else_=0
        )
        hourly = (
            db.query(
                hour_col,
                func.count(ApiAccessLog.id).label("total"),
                func.sum(error_case).label("errors"),
                func.avg(ApiAccessLog.response_time_ms).label("avg_rt"),
            )
            .filter(ApiAccessLog.timestamp >= cutoff)
            .group_by(hour_col)
            .order_by(hour_col)
            .all()
        )
        hourly_data = []
        for row in hourly:
            hourly_data.append({
                "hour": row.hb.strftime("%H:%M") if row.hb else "",
                "total": row.total or 0,
                "errors": int(row.errors or 0),
                "avgResponseTime": round(float(row.avg_rt or 0), 2),
            })

        # 전체 통계
        total_req = db.query(func.count(ApiAccessLog.id)).filter(
            ApiAccessLog.timestamp >= cutoff).scalar() or 0
        err_cnt = db.query(func.count(ApiAccessLog.id)).filter(
            ApiAccessLog.timestamp >= cutoff,
            ApiAccessLog.status_code >= 400).scalar() or 0
        avg_rt = db.query(func.avg(ApiAccessLog.response_time_ms)).filter(
            ApiAccessLog.timestamp >= cutoff).scalar() or 0

        # Top 10 경로
        top_paths = (
            db.query(ApiAccessLog.path, func.count(ApiAccessLog.id).label("cnt"))
            .filter(ApiAccessLog.timestamp >= cutoff)
            .group_by(ApiAccessLog.path)
            .order_by(desc("cnt"))
            .limit(10).all()
        )

        return _ok({
            "period": period,
            "totalRequests": total_req,
            "errorCount": err_cnt,
            "errorRate": round(err_cnt / total_req * 100, 2) if total_req else 0,
            "avgResponseTime": round(float(avg_rt), 2),
            "hourly": hourly_data,
            "topPaths": [{"path": p, "count": c} for p, c in top_paths],
        })
    finally:
        db.close()


@gateway_bp.route("/access-log/cleanup", methods=["POST"])
def run_log_cleanup():
    """접근 로그 수동 정리"""
    db = SessionLocal()
    try:
        row = db.query(AdminSetting).filter_by(
            key="gateway.access_log_retention_days").first()
        days = int(row.value) if row else 7
    finally:
        db.close()
    deleted = cleanup_old_logs(days)
    return _ok({"deleted": deleted, "retentionDays": days})


# ──────────────────────────────────────────────
#  UC-3: API 키 관리
# ──────────────────────────────────────────────

@gateway_bp.route("/keys", methods=["GET"])
def list_keys():
    db = SessionLocal()
    try:
        keys = db.query(ApiKey).order_by(desc(ApiKey.created_at)).all()
        return _ok([k.to_dict() for k in keys])
    finally:
        db.close()


@gateway_bp.route("/keys", methods=["POST"])
def create_key():
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return _err("키 이름은 필수입니다.")

    expires_at = None
    if body.get("expiresAt"):
        try:
            expires_at = datetime.strptime(body["expiresAt"], "%Y-%m-%d")
        except ValueError:
            return _err("만료일 형식이 올바르지 않습니다 (YYYY-MM-DD).")

    db = SessionLocal()
    try:
        key = ApiKey(
            name=name,
            key_value=secrets.token_hex(32),
            description=body.get("description", ""),
            allowed_paths=body.get("allowedPaths", "*"),
            expires_at=expires_at,
        )
        db.add(key)
        db.commit()
        db.refresh(key)
        result = key.to_dict(mask_key=False)   # 생성 직후만 전체 키 노출
        return jsonify({"success": True, "data": result, "error": None}), 201
    except Exception as e:
        db.rollback()
        return _err(f"키 생성 실패: {e}")
    finally:
        db.close()


@gateway_bp.route("/keys/<int:key_id>", methods=["PUT"])
def update_key(key_id):
    body = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        key = db.query(ApiKey).get(key_id)
        if not key:
            return _err("API 키를 찾을 수 없습니다.", status=404)
        if "name" in body:
            key.name = body["name"]
        if "description" in body:
            key.description = body["description"]
        if "allowedPaths" in body:
            key.allowed_paths = body["allowedPaths"]
        if "isActive" in body:
            key.is_active = body["isActive"]
        if "expiresAt" in body:
            if body["expiresAt"]:
                try:
                    key.expires_at = datetime.strptime(body["expiresAt"], "%Y-%m-%d")
                except ValueError:
                    return _err("만료일 형식이 올바르지 않습니다.")
            else:
                key.expires_at = None
        db.commit()
        return _ok(key.to_dict())
    except Exception as e:
        db.rollback()
        return _err(f"키 수정 실패: {e}")
    finally:
        db.close()


@gateway_bp.route("/keys/<int:key_id>", methods=["DELETE"])
def delete_key(key_id):
    db = SessionLocal()
    try:
        key = db.query(ApiKey).get(key_id)
        if not key:
            return _err("API 키를 찾을 수 없습니다.", status=404)
        db.delete(key)
        db.commit()
        return _ok({"deleted": key_id})
    except Exception as e:
        db.rollback()
        return _err(f"키 삭제 실패: {e}")
    finally:
        db.close()


# ──────────────────────────────────────────────
#  Gateway 설정 (CORS, 로그 보관)
# ──────────────────────────────────────────────

@gateway_bp.route("/settings", methods=["GET"])
def get_settings():
    db = SessionLocal()
    try:
        result = {}
        for camel, key in _GW_SETTINGS.items():
            row = db.query(AdminSetting).filter_by(key=key).first()
            result[camel] = row.value if row else _GW_DEFAULTS.get(key, "")
        return _ok(result)
    finally:
        db.close()


@gateway_bp.route("/settings", methods=["PUT"])
def save_settings():
    body = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        for camel, key in _GW_SETTINGS.items():
            if camel in body:
                row = db.query(AdminSetting).filter_by(key=key).first()
                if row:
                    row.value = str(body[camel])
                else:
                    db.add(AdminSetting(key=key, value=str(body[camel])))
        db.commit()
        return _ok("saved")
    except Exception as e:
        db.rollback()
        return _err(f"설정 저장 실패: {e}")
    finally:
        db.close()
