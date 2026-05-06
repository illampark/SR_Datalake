"""백업/복구 API"""

import logging
import threading

from flask import Blueprint, jsonify, request
from sqlalchemy import desc

from backend.database import SessionLocal
from backend.models.user import AdminSetting
from backend.models.backup import BackupHistory
from backend.services.backup_executor import (
    execute_backup, execute_restore,
    get_backup_storage_info, delete_backup_objects,
)
from backend.services.backup_scheduler import (
    get_scheduler_status, get_schedule_options,
)

logger = logging.getLogger(__name__)

backup_bp = Blueprint("backup", __name__, url_prefix="/api/backup")


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ── AdminSetting KV 매핑 ───────────────────────────

_BACKUP_SETTINGS_MAP = {
    "schedule":      "backup.schedule",
    "targets":       "backup.targets",
    "retentionDays": "backup.retention_days",
    "compression":   "backup.compression",
}

_BACKUP_DEFAULTS = {
    "backup.schedule":       "매일 새벽 3시",
    "backup.targets":        "postgresql,config",
    "backup.retention_days": "30",
    "backup.compression":    "true",
}


# ── Settings ────────────────────────────────────────


@backup_bp.route("/settings", methods=["GET"])
def get_settings():
    """백업 설정 조회."""
    db = SessionLocal()
    try:
        settings = {}
        for camel, db_key in _BACKUP_SETTINGS_MAP.items():
            row = db.query(AdminSetting).filter(AdminSetting.key == db_key).first()
            settings[camel] = row.value if row else _BACKUP_DEFAULTS.get(db_key, "")
        settings["scheduleOptions"] = get_schedule_options()
        return _ok(settings)
    finally:
        db.close()


@backup_bp.route("/settings", methods=["PUT"])
def update_settings():
    """백업 설정 저장."""
    db = SessionLocal()
    try:
        body = request.get_json(force=True)
        # MinIO 백업은 비활성화 상태 — 저장값에서 제거 (Option A)
        if "targets" in body:
            t = body["targets"]
            if isinstance(t, list):
                t = [x for x in t if x != "minio"]
                body["targets"] = ",".join(t)
            elif isinstance(t, str):
                body["targets"] = ",".join(
                    x for x in t.split(",") if x.strip() and x.strip() != "minio"
                )
        updated = []
        for camel, db_key in _BACKUP_SETTINGS_MAP.items():
            if camel in body:
                val = str(body[camel]).strip()
                row = db.query(AdminSetting).filter(AdminSetting.key == db_key).first()
                if row:
                    row.value = val
                else:
                    db.add(AdminSetting(key=db_key, value=val))
                updated.append(camel)
        db.commit()
        logger.info("백업 설정 수정: %s", ", ".join(updated))
        return _ok({"updated": updated})
    except Exception as e:
        db.rollback()
        logger.error("백업 설정 수정 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ── History ─────────────────────────────────────────


@backup_bp.route("/history", methods=["GET"])
def list_history():
    """백업 이력 목록."""
    db = SessionLocal()
    try:
        page = max(1, request.args.get("page", 1, type=int))
        size = min(100, max(1, request.args.get("size", 20, type=int)))
        status_filter = request.args.get("status", "")
        op_filter = request.args.get("operation", "")

        q = db.query(BackupHistory)
        if status_filter:
            q = q.filter(BackupHistory.status == status_filter)
        if op_filter:
            q = q.filter(BackupHistory.operation == op_filter)

        total = q.count()
        rows = (q.order_by(desc(BackupHistory.execution_time))
                .offset((page - 1) * size)
                .limit(size)
                .all())

        return _ok({
            "history": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
        })
    finally:
        db.close()


@backup_bp.route("/history/<int:hid>", methods=["DELETE"])
def delete_history(hid):
    """백업 이력 및 MinIO 오브젝트 삭제."""
    db = SessionLocal()
    try:
        row = db.query(BackupHistory).filter(BackupHistory.id == hid).first()
        if not row:
            return _err("이력을 찾을 수 없습니다.", "NOT_FOUND", 404)

        # MinIO 오브젝트 삭제
        if row.storage_key and row.operation == "backup":
            deleted = delete_backup_objects(row.storage_key)
            logger.info("백업 오브젝트 %d개 삭제: %s", deleted, row.storage_key)

        db.delete(row)
        db.commit()
        logger.info("백업 이력 삭제: id=%d", hid)
        return _ok({"deleted": hid})
    except Exception as e:
        db.rollback()
        logger.error("백업 이력 삭제 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ── Execute / Restore ───────────────────────────────


@backup_bp.route("/execute", methods=["POST"])
def manual_execute():
    """수동 백업 실행 (백그라운드)."""
    # 진행 중인 백업 확인
    db = SessionLocal()
    try:
        running = db.query(BackupHistory).filter(
            BackupHistory.status == "running",
            BackupHistory.operation == "backup",
        ).first()
        if running:
            return _err("백업이 이미 진행 중입니다.")
    finally:
        db.close()

    body = request.get_json(force=True) if request.is_json else {}
    targets = body.get("targets", ["postgresql", "config"])

    # MinIO 백업은 동일 인스턴스에 저장되어 OOM 위험 + 가짜 백업이라 제외 (Option A)
    valid_targets = {"postgresql", "config"}
    for t in targets:
        if t == "minio":
            return _err("MinIO 백업은 비활성화되었습니다. 외부 미러링(mc mirror) 또는 파일시스템 스냅샷을 사용하세요.",
                        "MINIO_BACKUP_DISABLED")
        if t not in valid_targets:
            return _err(f"유효하지 않은 백업 대상: {t}")

    thread = threading.Thread(
        target=execute_backup,
        args=(targets, "manual"),
        daemon=True,
        name="backup-manual",
    )
    thread.start()

    return _ok({"message": "백업이 시작되었습니다.", "targets": targets}), 202


@backup_bp.route("/restore", methods=["POST"])
def restore():
    """복구 실행 (백그라운드)."""
    body = request.get_json(force=True)
    backup_id = body.get("backupId")
    targets = body.get("targets", [])

    if not backup_id:
        return _err("복구할 백업을 선택하세요.")

    # 백업 존재 확인
    db = SessionLocal()
    try:
        src = db.query(BackupHistory).filter(
            BackupHistory.id == backup_id,
            BackupHistory.status == "success",
            BackupHistory.operation == "backup",
        ).first()
        if not src:
            return _err("유효한 백업을 찾을 수 없습니다.", "NOT_FOUND", 404)
    finally:
        db.close()

    thread = threading.Thread(
        target=execute_restore,
        args=(backup_id, targets),
        daemon=True,
        name="backup-restore",
    )
    thread.start()

    return _ok({"message": "복구가 시작되었습니다."}), 202


# ── Status ──────────────────────────────────────────


@backup_bp.route("/status", methods=["GET"])
def status():
    """현재 백업 상태 종합."""
    db = SessionLocal()
    try:
        # 마지막 성공 백업
        last = (db.query(BackupHistory)
                .filter(BackupHistory.operation == "backup",
                        BackupHistory.status == "success")
                .order_by(desc(BackupHistory.execution_time))
                .first())

        # 진행 중 확인
        running = (db.query(BackupHistory)
                   .filter(BackupHistory.status == "running")
                   .first())

        # 스케줄러
        sched = get_scheduler_status()

        # 스토리지
        storage = get_backup_storage_info()

        return _ok({
            "lastBackup": last.to_dict() if last else None,
            "isRunning": running is not None,
            "runningOperation": running.to_dict() if running else None,
            "scheduler": sched,
            "storage": storage,
        })
    finally:
        db.close()
