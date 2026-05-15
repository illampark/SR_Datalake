import threading
from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.storage import TsdbConfig, RdbmsConfig, RetentionPolicy, RetentionExecutionLog
from backend.config import MINIO_BUCKETS
from backend.services.minio_client import get_minio_client
from backend.services.system_settings import get_default_page_size

retention_bp = Blueprint("storage_retention", __name__, url_prefix="/api/storage/retention")


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


def _fmt_bytes(b):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


# 정책이 없을 때 반환하는 기본값
_DEFAULTS = {
    "id": None,
    "hot_retention_days": 30,
    "hot_resolution": "1초 (Raw)",
    "hot_storage_type": "TimescaleDB (PostgreSQL)",
    "hot_capacity_gb": 500.0,
    "warm_retention_months": 12,
    "warm_resolution": "집계/메타데이터",
    "warm_storage_type": "PostgreSQL (RDBMS)",
    "warm_capacity_gb": 2048.0,
    "cold_retention_years": 3,
    "cold_resolution": "아카이브 파일",
    "cold_storage_type": "MinIO (S3)",
    "cold_capacity_gb": 10240.0,
    "hot_to_warm_schedule": "매일 새벽 3시",
    "warm_to_cold_schedule": "매주 일요일 새벽 4시",
    "expiry_cleanup_schedule": "매주",
    "auto_delete_enabled": True,
    "alert_threshold_enabled": True,
    "alert_threshold_percent": 80.0,
    "created_at": None,
    "updated_at": None,
}

# PUT 에서 허용하는 필드 목록
_ALLOWED_FIELDS = [
    "hot_retention_days", "hot_resolution", "hot_storage_type", "hot_capacity_gb",
    "warm_retention_months", "warm_resolution", "warm_storage_type", "warm_capacity_gb",
    "cold_retention_years", "cold_resolution", "cold_storage_type", "cold_capacity_gb",
    "hot_to_warm_schedule", "warm_to_cold_schedule", "expiry_cleanup_schedule",
    "auto_delete_enabled", "alert_threshold_enabled", "alert_threshold_percent",
]

# task_type → tier_transition 매핑
_TASK_MAP = {
    "downsampling": "TimescaleDB → RDBMS",
    "archiving": "RDBMS → MinIO",
    "delete": "MinIO (만료)",
}


# ──────────────────────────────────────────────
# RT-001: GET /api/storage/retention  — 정책 조회
# ──────────────────────────────────────────────
@retention_bp.route("", methods=["GET"])
def get_policy():
    db = _db()
    try:
        policy = db.query(RetentionPolicy).first()
        if not policy:
            return _ok(dict(_DEFAULTS))
        return _ok(policy.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# RT-002: PUT /api/storage/retention  — 정책 저장
# ──────────────────────────────────────────────
@retention_bp.route("", methods=["PUT"])
def update_policy():
    db = _db()
    try:
        body = request.get_json(force=True)

        # 검증
        hd = body.get("hot_retention_days")
        if hd is not None and (int(hd) < 1 or int(hd) > 365):
            return _err("Hot 보관 기간은 1~365일 사이여야 합니다.", "VALIDATION")
        wm = body.get("warm_retention_months")
        if wm is not None and (int(wm) < 1 or int(wm) > 60):
            return _err("Warm 보관 기간은 1~60개월 사이여야 합니다.", "VALIDATION")
        cy = body.get("cold_retention_years")
        if cy is not None and (int(cy) < 1 or int(cy) > 10):
            return _err("Cold 보관 기간은 1~10년 사이여야 합니다.", "VALIDATION")
        ap = body.get("alert_threshold_percent")
        if ap is not None and (float(ap) < 0 or float(ap) > 100):
            return _err("알림 임계치는 0~100% 사이여야 합니다.", "VALIDATION")

        policy = db.query(RetentionPolicy).first()
        if not policy:
            policy = RetentionPolicy()
            db.add(policy)

        for field in _ALLOWED_FIELDS:
            if field in body:
                val = body[field]
                if field.endswith("_days") or field.endswith("_months") or field.endswith("_years"):
                    val = int(val)
                elif field.endswith("_gb") or field.endswith("_percent"):
                    val = float(val)
                elif field.endswith("_enabled"):
                    val = bool(val)
                setattr(policy, field, val)

        policy.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(policy)
        return _ok(policy.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# RT-003: GET /api/storage/retention/tiers  — 계층 현황
# ──────────────────────────────────────────────
@retention_bp.route("/tiers", methods=["GET"])
def get_tiers():
    db = _db()
    try:
        policy = db.query(RetentionPolicy).first()
        p = policy.to_dict() if policy else dict(_DEFAULTS)

        hot_used = _get_tsdb_size(db)
        warm_used = _get_rdbms_size(db)
        cold_used = _get_minio_size()

        hot_days = p["hot_retention_days"] or 30
        warm_months = p["warm_retention_months"] or 12

        def _tier(storage_type, resolution, retention_label, capacity_gb, used_bytes, daily_label):
            cap_bytes = capacity_gb * 1024 ** 3
            pct = round((used_bytes / cap_bytes) * 100, 1) if cap_bytes > 0 else 0
            return {
                "storage_type": storage_type,
                "resolution": resolution,
                "retention_label": retention_label,
                "capacity_gb": capacity_gb,
                "used_bytes": used_bytes,
                "used_display": _fmt_bytes(used_bytes),
                "usage_percent": min(pct, 100.0),
                "daily_growth_display": daily_label,
            }

        hot_daily = _fmt_bytes(hot_used / hot_days) if hot_days > 0 and hot_used > 0 else "~0 B"
        warm_daily_days = warm_months * 30
        warm_daily = _fmt_bytes(warm_used / warm_daily_days) if warm_daily_days > 0 and warm_used > 0 else "~0 B"
        cold_daily_days = p["cold_retention_years"] * 365
        cold_daily = _fmt_bytes(cold_used / cold_daily_days) if cold_daily_days > 0 and cold_used > 0 else "~0 B"

        return _ok({
            "hot": _tier(
                p["hot_storage_type"], p["hot_resolution"],
                f"{p['hot_retention_days']}일", p["hot_capacity_gb"],
                hot_used, f"~{hot_daily}",
            ),
            "warm": _tier(
                p["warm_storage_type"], p["warm_resolution"],
                f"{p['warm_retention_months']}개월", p["warm_capacity_gb"],
                warm_used, f"~{warm_daily}",
            ),
            "cold": _tier(
                p["cold_storage_type"], p["cold_resolution"],
                f"{p['cold_retention_years']}년", p["cold_capacity_gb"],
                cold_used, f"~{cold_daily}",
            ),
        })
    except Exception as e:
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


def _get_tsdb_size(db):
    """TimescaleDB (시계열 DB) 사이즈 조회 (bytes). TsdbConfig 접속 정보 사용."""
    try:
        tsdb = db.query(TsdbConfig).first()
        if not tsdb:
            return 0
        import psycopg2
        conn = psycopg2.connect(
            host=tsdb.host, port=tsdb.port,
            dbname=tsdb.database_name or "postgres",
            user="sdl_user", password="sdl_password_2025",
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT pg_database_size(current_database());")
        size = cur.fetchone()[0]
        cur.close()
        conn.close()
        return size
    except Exception:
        return 0


def _get_rdbms_size(db):
    """PostgreSQL RDBMS 사이즈 조회 (bytes). RdbmsConfig 접속 정보 사용."""
    try:
        rdbms = db.query(RdbmsConfig).first()
        if not rdbms:
            return 0
        import psycopg2
        conn = psycopg2.connect(
            host=rdbms.host, port=rdbms.port,
            dbname=rdbms.database_name or "postgres",
            user=rdbms.username or "sdl_user",
            password=rdbms.password or "sdl_password_2025",
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT pg_database_size(current_database());")
        size = cur.fetchone()[0]
        cur.close()
        conn.close()
        return size
    except Exception:
        return 0


def _get_minio_size():
    """MinIO 파일 스토리지 전체 버킷 사이즈 합산 (bytes).

    storage_file._minio_bucket_summary_cached 의 파일 캐시(/tmp/sdl_minio_cache.json)
    를 재사용. 직접 list_objects(recursive=True) 호출하면 NFS 위 163k 객체에서
    5~10분 + gunicorn worker timeout 위험. 캐시 miss 면 0 반환 (워머가 채우는 중).
    """
    try:
        from backend.routes.storage_file import _minio_bucket_summary_cached
        summary = _minio_bucket_summary_cached(blocking=False)
        if summary is None:
            return 0
        return int(summary.get("total_size", 0))
    except Exception:
        return 0


# ──────────────────────────────────────────────
# RT-004: GET /api/storage/retention/history  — 실행 이력
# ──────────────────────────────────────────────
@retention_bp.route("/history", methods=["GET"])
def get_history():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        total = db.query(func.count(RetentionExecutionLog.id)).scalar()
        rows = (
            db.query(RetentionExecutionLog)
            .order_by(RetentionExecutionLog.execution_time.desc())
            .offset((page - 1) * size)
            .limit(size)
            .all()
        )
        return _ok(
            [r.to_dict() for r in rows],
            {"page": page, "size": size, "total": total},
        )
    finally:
        db.close()


# ──────────────────────────────────────────────
# RT-005: POST /api/storage/retention/execute  — 수동 실행
# ──────────────────────────────────────────────
@retention_bp.route("/execute", methods=["POST"])
def manual_execute():
    db = _db()
    try:
        body = request.get_json(force=True)
        task_type = body.get("task_type", "").strip()

        if task_type not in _TASK_MAP:
            return _err(
                f"유효하지 않은 작업 유형입니다. ({', '.join(_TASK_MAP.keys())})",
                "VALIDATION",
            )

        from backend.services.retention_executor import (
            execute_downsampling, execute_archiving, execute_cold_expiry,
        )

        _EXECUTOR_MAP = {
            "downsampling": execute_downsampling,
            "archiving": execute_archiving,
            "delete": execute_cold_expiry,
        }

        func = _EXECUTOR_MAP[task_type]
        thread = threading.Thread(target=func, daemon=True,
                                  name=f"retention-exec-{task_type}")
        thread.start()

        return _ok({
            "message": f"{task_type} 작업이 시작되었습니다.",
            "task_type": task_type,
            "tier_transition": _TASK_MAP[task_type],
            "started_at": datetime.utcnow().isoformat(),
        }), 202
    except Exception as e:
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# RT-006: GET /api/storage/retention/summary  — 요약 통계
# ──────────────────────────────────────────────
@retention_bp.route("/summary", methods=["GET"])
def get_summary():
    db = _db()
    try:
        total = db.query(func.count(RetentionExecutionLog.id)).scalar() or 0
        success = db.query(func.count(RetentionExecutionLog.id)).filter(
            RetentionExecutionLog.status == "success"
        ).scalar() or 0
        failed = db.query(func.count(RetentionExecutionLog.id)).filter(
            RetentionExecutionLog.status == "failed"
        ).scalar() or 0
        total_bytes = int(db.query(func.sum(RetentionExecutionLog.processed_bytes)).scalar() or 0)
        last_exec = db.query(func.max(RetentionExecutionLog.execution_time)).scalar()

        policy = db.query(RetentionPolicy).first()

        return _ok({
            "total_executions": total,
            "success_count": success,
            "failure_count": failed,
            "partial_count": total - success - failed,
            "total_processed_bytes": total_bytes,
            "total_processed_display": _fmt_bytes(total_bytes),
            "last_execution_time": last_exec.isoformat() if last_exec else None,
            "policy_active": policy is not None,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# RT-007: GET /api/storage/retention/scheduler/status  — 스케줄러 상태
# ──────────────────────────────────────────────
@retention_bp.route("/scheduler/status", methods=["GET"])
def scheduler_status():
    from backend.services.retention_scheduler import get_scheduler_status
    return _ok(get_scheduler_status())
