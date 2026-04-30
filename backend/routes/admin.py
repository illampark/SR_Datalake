"""사용자 관리 + 시스템 설정 API"""

import logging
import os
import platform
import re
import socket
import sys
from datetime import datetime

import flask
from datetime import timedelta
from flask import Blueprint, jsonify, request, session
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.user import User, LoginHistory, AdminSetting
from backend.config import (
    DATABASE_URL, MINIO_ENDPOINT, MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY, MINIO_SECURE,
    BENTHOS_API, MQTT_DEFAULT_HOST, MQTT_DEFAULT_PORT,
)

from backend.services.audit_logger import log_audit

logger = logging.getLogger(__name__)

admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")

_SERVER_START_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


def _get_setting_val(db, key, fallback=""):
    """admin_setting 테이블에서 값 조회, 없으면 fallback."""
    row = db.query(AdminSetting).filter_by(key=key).first()
    return row.value if row else fallback


# ──────────────────────────────────────────────
# 역할 정의 (고정 4종)
# ──────────────────────────────────────────────

_ROLES = [
    {"key": "admin",    "name": "시스템 관리자", "badge": "danger",
     "desc": "전체 시스템 관리 및 설정 권한. 사용자 관리, 보안 설정, 인프라 관리 포함."},
    {"key": "engineer", "name": "엔지니어",     "badge": "primary",
     "desc": "데이터 수집, 파이프라인 관리, 커넥터 설정 및 모니터링 권한."},
    {"key": "operator", "name": "운영자",       "badge": "success",
     "desc": "모니터링, 알람 관리, 로그 조회 및 기본 운영 권한."},
    {"key": "viewer",   "name": "뷰어",         "badge": "warning",
     "desc": "대시보드 및 모니터링 데이터 조회만 가능. 설정 변경 불가."},
]

# 기본 로그인 정책
_DEFAULT_POLICY = {
    "login.session_timeout_min": "480",
    "login.max_fail_count": "5",
    "login.lockout_duration_min": "30",
    "login.password_min_length": "8",
}

# 기본 시스템 설정
_DEFAULT_SETTINGS = {
    "system.name": "SR DataLake",
    "system.timezone": "Asia/Seoul",
    "system.date_format": "YYYY-MM-DD HH:mm:ss",
    "system.admin_email": "admin@sdm-factory.co.kr",
    "system.page_size": "20",
    "log.level": "INFO",
    "log.retention_days": "30",
    "log.max_file_size_mb": "100",
    "log.max_file_count": "10",
    "log.send_to_db": "true",
    "log.console_output": "false",
    "backup.schedule": "매일 새벽 3시",
    "backup.targets": "postgresql,config",
    "backup.retention_days": "30",
    "backup.compression": "true",
    "gateway.cors_origins": "*",
    "gateway.access_log_retention_days": "7",
    "system.default_language": "ko",
    "minio.endpoint": MINIO_ENDPOINT,
    "minio.access_key": MINIO_ACCESS_KEY,
    "minio.secret_key": MINIO_SECRET_KEY,
    "minio.secure": str(MINIO_SECURE).lower(),
}


# ──────────────────────────────────────────────
# 초기 데이터 Seed
# ──────────────────────────────────────────────

def seed_default_users():
    """기본 사용자 및 로그인 정책 시드"""
    db = SessionLocal()
    try:
        # 사용자 시드
        if db.query(User).count() == 0:
            defaults = [
                User(
                    username="admin",
                    display_name="관리자",
                    email="admin@sdm-factory.co.kr",
                    password_hash=generate_password_hash("admin1234"),
                    role="admin",
                ),
                User(
                    username="viewer",
                    display_name="뷰어",
                    email="viewer@sdm-factory.co.kr",
                    password_hash=generate_password_hash("viewer1234"),
                    role="viewer",
                ),
            ]
            db.add_all(defaults)
            db.commit()
            logger.info("기본 사용자 2명 생성 완료")

        # 로그인 정책 + 시스템 설정 시드
        all_defaults = {**_DEFAULT_POLICY, **_DEFAULT_SETTINGS}
        for k, v in all_defaults.items():
            exists = db.query(AdminSetting).filter(AdminSetting.key == k).first()
            if not exists:
                db.add(AdminSetting(key=k, value=v))
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("초기 데이터 시드 실패: %s", e)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 사용자 CRUD
# ──────────────────────────────────────────────

@admin_bp.route("/users", methods=["GET"])
def list_users():
    """전체 사용자 목록"""
    db = SessionLocal()
    try:
        users = db.query(User).order_by(User.created_at).all()
        return _ok({"users": [u.to_dict() for u in users]})
    finally:
        db.close()


@admin_bp.route("/users", methods=["POST"])
def create_user():
    """사용자 생성"""
    db = SessionLocal()
    try:
        body = request.get_json(force=True)
        username = (body.get("username") or "").strip()
        display_name = (body.get("displayName") or "").strip()
        email = (body.get("email") or "").strip()
        password = (body.get("password") or "").strip()
        role = (body.get("role") or "viewer").strip()

        if not username or not display_name:
            return _err("사용자 ID와 이름은 필수입니다.")
        if not password:
            return _err("비밀번호는 필수입니다.")
        if role not in [r["key"] for r in _ROLES]:
            return _err("유효하지 않은 역할입니다.")

        # 중복 체크
        exists = db.query(User).filter(User.username == username).first()
        if exists:
            return _err("이미 존재하는 사용자 ID입니다.")

        # 비밀번호 정책 체크
        min_len = _get_policy_int(db, "login.password_min_length", 8)
        if len(password) < min_len:
            return _err("비밀번호는 최소 %d자 이상이어야 합니다." % min_len)

        user = User(
            username=username,
            display_name=display_name,
            email=email,
            password_hash=generate_password_hash(password),
            role=role,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        logger.info("사용자 생성: %s (%s)", username, role)
        log_audit("user", "user.create", "user", username,
                  detail={"role": role, "displayName": display_name})
        return _ok(user.to_dict())
    except Exception as e:
        db.rollback()
        logger.error("사용자 생성 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@admin_bp.route("/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    """사용자 수정"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return _err("사용자를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        if "displayName" in body:
            user.display_name = body["displayName"].strip()
        if "email" in body:
            user.email = body["email"].strip()
        if "role" in body:
            role = body["role"].strip()
            if role in [r["key"] for r in _ROLES]:
                user.role = role
        if "enabled" in body:
            user.enabled = bool(body["enabled"])

        db.commit()
        db.refresh(user)
        logger.info("사용자 수정: %s", user.username)
        log_audit("user", "user.update", "user", user.username,
                  detail={k: body[k] for k in body if k in ("displayName", "email", "role", "enabled")})
        return _ok(user.to_dict())
    except Exception as e:
        db.rollback()
        logger.error("사용자 수정 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@admin_bp.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    """사용자 삭제"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return _err("사용자를 찾을 수 없습니다.", "NOT_FOUND", 404)
        username = user.username
        db.delete(user)
        db.commit()
        logger.info("사용자 삭제: %s", username)
        log_audit("user", "user.delete", "user", username)
        return _ok({"deleted": username})
    except Exception as e:
        db.rollback()
        logger.error("사용자 삭제 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@admin_bp.route("/users/<int:user_id>/toggle", methods=["POST"])
def toggle_user(user_id):
    """활성/비활성 토글"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return _err("사용자를 찾을 수 없습니다.", "NOT_FOUND", 404)
        user.enabled = not user.enabled
        db.commit()
        db.refresh(user)
        logger.info("사용자 %s: %s", "활성화" if user.enabled else "비활성화", user.username)
        log_audit("user", "user.toggle", "user", user.username,
                  detail={"enabled": user.enabled})
        return _ok(user.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@admin_bp.route("/users/<int:user_id>/reset-password", methods=["POST"])
def reset_password(user_id):
    """비밀번호 초기화"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return _err("사용자를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        new_pw = (body.get("password") or "").strip()
        if not new_pw:
            return _err("새 비밀번호를 입력하세요.")

        min_len = _get_policy_int(db, "login.password_min_length", 8)
        if len(new_pw) < min_len:
            return _err("비밀번호는 최소 %d자 이상이어야 합니다." % min_len)

        user.password_hash = generate_password_hash(new_pw)
        user.login_fail_count = 0
        user.locked_until = None
        db.commit()
        logger.info("비밀번호 초기화: %s", user.username)
        log_audit("user", "user.reset_password", "user", user.username)
        return _ok({"username": user.username})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@admin_bp.route("/users/<int:user_id>/unlock", methods=["POST"])
def unlock_user(user_id):
    """잠금 해제"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return _err("사용자를 찾을 수 없습니다.", "NOT_FOUND", 404)
        user.login_fail_count = 0
        user.locked_until = None
        db.commit()
        db.refresh(user)
        logger.info("사용자 잠금 해제: %s", user.username)
        log_audit("user", "user.unlock", "user", user.username)
        return _ok(user.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 역할 조회
# ──────────────────────────────────────────────

@admin_bp.route("/roles", methods=["GET"])
def list_roles():
    """역할 목록 + 역할별 사용자 수"""
    db = SessionLocal()
    try:
        counts = dict(
            db.query(User.role, func.count(User.id))
            .group_by(User.role).all()
        )
        roles = []
        for r in _ROLES:
            roles.append({
                "key": r["key"],
                "name": r["name"],
                "badge": r["badge"],
                "desc": r["desc"],
                "userCount": counts.get(r["key"], 0),
            })
        return _ok({"roles": roles})
    finally:
        db.close()


# ──────────────────────────────────────────────
# 로그인 이력
# ──────────────────────────────────────────────

@admin_bp.route("/login-history", methods=["GET"])
def login_history():
    """로그인 이벤트 목록"""
    db = SessionLocal()
    try:
        username = request.args.get("username", "").strip()
        event_type = request.args.get("eventType", "").strip()
        from_dt = request.args.get("from", "").strip()
        to_dt = request.args.get("to", "").strip()
        limit = min(500, max(10, int(request.args.get("limit", 100))))

        q = db.query(LoginHistory).order_by(LoginHistory.created_at.desc())

        if username:
            q = q.filter(LoginHistory.username == username)
        if event_type:
            q = q.filter(LoginHistory.event_type == event_type)
        if from_dt:
            try:
                q = q.filter(LoginHistory.created_at >= datetime.fromisoformat(from_dt))
            except ValueError:
                pass
        if to_dt:
            try:
                to_parsed = datetime.fromisoformat(to_dt)
                if to_parsed.hour == 0 and to_parsed.minute == 0:
                    to_parsed = to_parsed.replace(hour=23, minute=59, second=59)
                q = q.filter(LoginHistory.created_at <= to_parsed)
            except ValueError:
                pass

        events = q.limit(limit).all()
        return _ok({"events": [e.to_dict() for e in events]})
    except Exception as e:
        logger.error("로그인 이력 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 로그인 정책
# ──────────────────────────────────────────────

def _get_policy_int(db, key, default=0):
    """설정값을 int로 읽기"""
    row = db.query(AdminSetting).filter(AdminSetting.key == key).first()
    if row:
        try:
            return int(row.value)
        except (ValueError, TypeError):
            pass
    return default


@admin_bp.route("/login-policy", methods=["GET"])
def get_login_policy():
    """로그인 정책 조회"""
    db = SessionLocal()
    try:
        policy = {}
        for key in _DEFAULT_POLICY:
            row = db.query(AdminSetting).filter(AdminSetting.key == key).first()
            # key에서 prefix 제거하여 camelCase 변환
            short = key.replace("login.", "")
            parts = short.split("_")
            camel = parts[0] + "".join(p.capitalize() for p in parts[1:])
            policy[camel] = row.value if row else _DEFAULT_POLICY[key]
        return _ok({"policy": policy})
    finally:
        db.close()


@admin_bp.route("/login-policy", methods=["PUT"])
def update_login_policy():
    """로그인 정책 수정"""
    db = SessionLocal()
    try:
        body = request.get_json(force=True)

        # camelCase → DB key 매핑
        mapping = {
            "sessionTimeoutMin": "login.session_timeout_min",
            "maxFailCount": "login.max_fail_count",
            "lockoutDurationMin": "login.lockout_duration_min",
            "passwordMinLength": "login.password_min_length",
        }

        updated = []
        for camel, db_key in mapping.items():
            if camel in body:
                val = str(int(body[camel]))  # 정수 검증
                row = db.query(AdminSetting).filter(AdminSetting.key == db_key).first()
                if row:
                    row.value = val
                else:
                    db.add(AdminSetting(key=db_key, value=val))
                updated.append(camel)

        db.commit()
        logger.info("로그인 정책 수정: %s", ", ".join(updated))
        log_audit("config", "policy.update", "policy", "", detail={"updated": updated})
        return _ok({"updated": updated})
    except Exception as e:
        db.rollback()
        logger.error("로그인 정책 수정 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 시스템 설정
# ──────────────────────────────────────────────

# camelCase ↔ DB key 매핑
_SETTINGS_MAP = {
    "systemName":      "system.name",
    "timezone":        "system.timezone",
    "dateFormat":      "system.date_format",
    "adminEmail":      "system.admin_email",
    "pageSize":        "system.page_size",
    "logLevel":        "log.level",
    "logRetentionDays": "log.retention_days",
    "logMaxFileSizeMb": "log.max_file_size_mb",
    "logMaxFileCount":  "log.max_file_count",
    "logSendToDb":      "log.send_to_db",
    "logConsoleOutput": "log.console_output",
    "defaultLanguage":  "system.default_language",
}


@admin_bp.route("/settings", methods=["GET"])
def get_settings():
    """시스템 설정 조회"""
    db = SessionLocal()
    try:
        settings = {}
        for camel, db_key in _SETTINGS_MAP.items():
            row = db.query(AdminSetting).filter(AdminSetting.key == db_key).first()
            settings[camel] = row.value if row else _DEFAULT_SETTINGS.get(db_key, "")
        return _ok({"settings": settings})
    finally:
        db.close()


@admin_bp.route("/settings", methods=["PUT"])
def update_settings():
    """시스템 설정 저장"""
    db = SessionLocal()
    try:
        body = request.get_json(force=True)
        updated = []
        for camel, db_key in _SETTINGS_MAP.items():
            if camel in body:
                val = str(body[camel]).strip()
                row = db.query(AdminSetting).filter(AdminSetting.key == db_key).first()
                if row:
                    row.value = val
                else:
                    db.add(AdminSetting(key=db_key, value=val))
                updated.append(camel)
        db.commit()
        logger.info("시스템 설정 수정: %s", ", ".join(updated))
        log_audit("config", "settings.update", "setting", "", detail={"updated": updated})
        return _ok({"updated": updated})
    except Exception as e:
        db.rollback()
        logger.error("시스템 설정 수정 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


def _mask_db_url(url):
    """DB URL 비밀번호 마스킹"""
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:••••••••@", url)


@admin_bp.route("/system-info", methods=["GET"])
def system_info():
    """서버 런타임 정보 (읽기 전용)"""
    info = {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "pythonVersion": sys.version.split()[0],
        "framework": "Flask %s" % flask.__version__,
        "pid": os.getpid(),
        "startedAt": _SERVER_START_TIME,
        "database": {
            "url": _mask_db_url(DATABASE_URL),
            "driver": "psycopg2",
        },
        "services": {
            "mqtt": "%s:%s" % (MQTT_DEFAULT_HOST, MQTT_DEFAULT_PORT),
            "benthos": BENTHOS_API,
            "minio": _get_setting_val(db, "minio.endpoint", MINIO_ENDPOINT),
        },
        "environment": [
            {"key": "DATABASE_URL", "value": _mask_db_url(DATABASE_URL), "desc": "데이터베이스 연결"},
            {"key": "MINIO_ENDPOINT", "value": _get_setting_val(db, "minio.endpoint", MINIO_ENDPOINT), "desc": "MinIO 오브젝트 스토리지"},
            {"key": "MINIO_ACCESS_KEY", "value": _get_setting_val(db, "minio.access_key", MINIO_ACCESS_KEY), "desc": "MinIO 접근 키"},
            {"key": "MINIO_SECRET_KEY", "value": "••••••••", "desc": "MinIO 비밀 키 (마스킹)"},
            {"key": "BENTHOS_API", "value": BENTHOS_API, "desc": "Benthos 스트림 API"},
            {"key": "MQTT_DEFAULT_HOST", "value": MQTT_DEFAULT_HOST, "desc": "MQTT 브로커 호스트"},
            {"key": "MQTT_DEFAULT_PORT", "value": str(MQTT_DEFAULT_PORT), "desc": "MQTT 브로커 포트"},
        ],
    }
    return _ok(info)


# ──────────────────────────────────────────────
# 로고 업로드
# ──────────────────────────────────────────────

_LOGO_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "static", "uploads")
_ALLOWED_EXT = {"png", "jpg", "jpeg", "svg", "gif", "webp"}


@admin_bp.route("/logo", methods=["POST"])
def upload_logo():
    """로고 이미지 업로드"""
    if "file" not in request.files:
        return _err("파일이 전송되지 않았습니다.")

    f = request.files["file"]
    if not f.filename:
        return _err("파일명이 없습니다.")

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in _ALLOWED_EXT:
        return _err("허용되지 않는 파일 형식입니다. (PNG, JPG, SVG, GIF, WebP)")

    # 기존 로고 파일 삭제
    _remove_old_logo()

    filename = "logo." + ext
    filepath = os.path.join(os.path.abspath(_LOGO_DIR), filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    f.save(filepath)

    # DB에 경로 저장
    logo_url = "/static/uploads/" + filename
    db = SessionLocal()
    try:
        row = db.query(AdminSetting).filter(AdminSetting.key == "system.logo_path").first()
        if row:
            row.value = logo_url
        else:
            db.add(AdminSetting(key="system.logo_path", value=logo_url))
        db.commit()
    finally:
        db.close()

    logger.info("로고 업로드: %s", filename)
    return _ok({"logoUrl": logo_url})


@admin_bp.route("/logo", methods=["DELETE"])
def delete_logo():
    """로고 삭제 (기본 아이콘으로 복원)"""
    _remove_old_logo()

    db = SessionLocal()
    try:
        row = db.query(AdminSetting).filter(AdminSetting.key == "system.logo_path").first()
        if row:
            db.delete(row)
            db.commit()
    finally:
        db.close()

    logger.info("로고 삭제 (기본값 복원)")
    return _ok({"logoUrl": ""})


def _remove_old_logo():
    """기존 로고 파일 정리"""
    abs_dir = os.path.abspath(_LOGO_DIR)
    if not os.path.isdir(abs_dir):
        return
    for fname in os.listdir(abs_dir):
        if fname.startswith("logo."):
            try:
                os.remove(os.path.join(abs_dir, fname))
            except OSError:
                pass


@admin_bp.route("/logo", methods=["GET"])
def get_logo():
    """현재 로고 URL 조회"""
    db = SessionLocal()
    try:
        row = db.query(AdminSetting).filter(AdminSetting.key == "system.logo_path").first()
        return _ok({"logoUrl": row.value if row else ""})
    finally:
        db.close()


# ──────────────────────────────────────────────
# 인증 (로그인 / 로그아웃 / 현재 사용자)
# ──────────────────────────────────────────────

@admin_bp.route("/auth/login", methods=["POST"])
def auth_login():
    """사용자 로그인"""
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = (body.get("password") or "").strip()

    if not username or not password:
        return _err("사용자 ID와 비밀번호를 입력하세요.")

    ip = request.remote_addr or ""
    ua = (request.user_agent.string or "")[:500]

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == username).first()
        if not user:
            return _err("사용자 ID 또는 비밀번호가 올바르지 않습니다.", "AUTH_FAIL")

        # 계정 비활성 체크
        if not user.enabled:
            db.add(LoginHistory(username=username, event_type="fail",
                                ip_address=ip, user_agent=ua, detail="비활성 계정"))
            db.commit()
            log_audit("login", "auth.fail", "", "", result="failure",
                      detail={"reason": "disabled_account"}, username=username)
            return _err("비활성화된 계정입니다. 관리자에게 문의하세요.", "ACCOUNT_DISABLED")

        # 잠금 체크
        if user.locked_until and user.locked_until > datetime.utcnow():
            remaining = int((user.locked_until - datetime.utcnow()).total_seconds() / 60) + 1
            return _err("계정이 잠겨 있습니다. %d분 후 다시 시도하세요." % remaining, "ACCOUNT_LOCKED")

        # 비밀번호 검증
        if not check_password_hash(user.password_hash, password):
            max_fail = _get_policy_int(db, "login.max_fail_count", 5)
            user.login_fail_count = (user.login_fail_count or 0) + 1

            if user.login_fail_count >= max_fail:
                lockout_min = _get_policy_int(db, "login.lockout_duration_min", 30)
                user.locked_until = datetime.utcnow() + timedelta(minutes=lockout_min)
                db.add(LoginHistory(username=username, event_type="lock",
                                    ip_address=ip, user_agent=ua,
                                    detail="로그인 %d회 실패로 %d분간 잠금" % (max_fail, lockout_min)))
                db.commit()
                log_audit("login", "auth.lock", "", "", result="failure",
                          detail={"failCount": max_fail, "lockoutMin": lockout_min}, username=username)
                return _err("로그인 %d회 실패로 계정이 %d분간 잠겼습니다." % (max_fail, lockout_min), "ACCOUNT_LOCKED")

            db.add(LoginHistory(username=username, event_type="fail",
                                ip_address=ip, user_agent=ua,
                                detail="비밀번호 오류 (%d/%d)" % (user.login_fail_count, max_fail)))
            db.commit()
            log_audit("login", "auth.fail", "", "", result="failure",
                      detail={"failCount": user.login_fail_count, "maxFail": max_fail}, username=username)
            return _err("사용자 ID 또는 비밀번호가 올바르지 않습니다.", "AUTH_FAIL")

        # 로그인 성공
        user.login_fail_count = 0
        user.locked_until = None
        user.last_login_at = datetime.utcnow()
        db.add(LoginHistory(username=username, event_type="login",
                            ip_address=ip, user_agent=ua, detail="로그인 성공"))
        db.commit()

        # 세션 설정
        session.permanent = True
        session["user_id"] = user.id
        session["username"] = user.username
        session["display_name"] = user.display_name
        session["role"] = user.role

        log_audit("login", "auth.login", "", "", username=username)
        logger.info("로그인 성공: %s (%s) from %s", username, user.role, ip)
        resp = _ok({
            "username": user.username,
            "displayName": user.display_name,
            "role": user.role,
        })
        # 타 Flask 앱의 스테일 쿠키(Flask-Login 'session', 'remember_token') 만료
        for legacy_name in ("session", "remember_token"):
            for path in ("/", "/api", "/admin", "/login"):
                resp.headers.add(
                    "Set-Cookie",
                    f"{legacy_name}=; Path={path}; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT",
                )
        return resp
    except Exception as e:
        db.rollback()
        logger.error("로그인 처리 오류: %s", e)
        return _err("로그인 처리 중 오류가 발생했습니다.", "SERVER_ERROR", 500)
    finally:
        db.close()


@admin_bp.route("/auth/logout", methods=["POST"])
def auth_logout():
    """로그아웃"""
    username = session.get("username", "")
    if username:
        ip = request.remote_addr or ""
        ua = (request.user_agent.string or "")[:500]
        db = SessionLocal()
        try:
            db.add(LoginHistory(username=username, event_type="logout",
                                ip_address=ip, user_agent=ua, detail="로그아웃"))
            db.commit()
        except Exception:
            db.rollback()
        finally:
            db.close()
        log_audit("login", "auth.logout", "", "", username=username)
        logger.info("로그아웃: %s", username)

    session.clear()
    return _ok("로그아웃 되었습니다.")


@admin_bp.route("/auth/me", methods=["GET"])
def auth_me():
    """현재 로그인된 사용자 정보"""
    if "user_id" not in session:
        return _err("로그인이 필요합니다.", "UNAUTHORIZED", 401)
    return _ok({
        "userId": session["user_id"],
        "username": session["username"],
        "displayName": session["display_name"],
        "role": session["role"],
    })


@admin_bp.route("/lang", methods=["POST"])
def switch_language():
    """UI 언어 전환 (ko/en)"""
    body = request.get_json(silent=True) or {}
    lang = (body.get("lang") or "").strip()
    if lang not in ("ko", "en"):
        return _err("Unsupported language", "INVALID_LANG")
    if "user_id" in session:
        session["lang"] = lang
    resp = jsonify({"success": True, "data": {"lang": lang}, "error": None})
    resp.set_cookie("lang", lang, max_age=365 * 24 * 3600, samesite="Lax")
    return resp
