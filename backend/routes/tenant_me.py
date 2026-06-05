"""tenant_admin 콘솔 라우트 — Phase 7.

claudedocs/rbac-target-v1.md § 6 TENANT_ADMIN_ONLY 경로.

기능:
- /api/tenant/me — 자기 tenant 정보 + settings (tenant_admin 변경 가능)
- /api/tenant/me/members — 소속 사용자 목록 + 역할
- /api/tenant/me/members POST/PATCH/DELETE — 멤버 초대/역할변경/제거 (tenant_admin)
- /api/tenant/me/usage — 자기 tenant 사용량 (tenant_admin 조회 가능)
"""

from __future__ import annotations
import logging
from flask import Blueprint, request, session, g, jsonify
from werkzeug.security import generate_password_hash

from backend.database import SessionLocal
from backend.models.tenant import Tenant, TenantMembership, TENANT_ROLES
from backend.models.user import User
from backend.services.audit_logger import log_audit
from backend.services.rbac import (
    current_tenant_id, current_tenant_role, is_super, tenant_role_at_least,
)

logger = logging.getLogger(__name__)
tenant_me_bp = Blueprint("tenant_me", __name__, url_prefix="/api/tenant/me")


def _ok(data=None):
    return {"success": True, "data": data, "error": None}


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


def _require_tenant_admin():
    if not tenant_role_at_least("tenant_admin"):
        return _err("tenant_admin 권한 필요", "FORBIDDEN", 403)
    return None


# ─────────────────────── Tenant 자체 정보 ───────────────────────

@tenant_me_bp.route("", methods=["GET"])
def get_my_tenant():
    """현재 컨텍스트의 tenant 정보 (모든 멤버 조회 가능)."""
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(current_tenant_id())
        if not t:
            return _err("tenant 없음", "NOT_FOUND", 404)
        return _ok(t.to_dict())
    finally:
        db.close()


@tenant_me_bp.route("", methods=["PATCH"])
def update_my_tenant():
    """tenant 메타 변경 (name, settings) — tenant_admin only.

    status 변경은 super_admin 만 (/api/sys/tenants/<id> 로).
    """
    err = _require_tenant_admin()
    if err is not None:
        return err
    body = request.get_json(force=True) or {}
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(current_tenant_id())
        if not t:
            return _err("tenant 없음", "NOT_FOUND", 404)
        if "name" in body:
            t.name = body["name"]
        if "settings" in body and isinstance(body["settings"], dict):
            # merge instead of replace — 안전
            cur = dict(t.settings or {})
            cur.update(body["settings"])
            t.settings = cur
        db.commit()
        db.refresh(t)
        log_audit("tenant", "tenant.update", "tenant", str(t.id),
                  detail={k: body[k] for k in body if k in ("name", "settings")})
        return _ok(t.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ─────────────────────── Members ───────────────────────

@tenant_me_bp.route("/members", methods=["GET"])
def list_members():
    """tenant 멤버 목록 + 역할."""
    db = SessionLocal()
    try:
        rows = (
            db.query(TenantMembership, User)
              .join(User, User.id == TenantMembership.user_id)
              .filter(TenantMembership.tenant_id == current_tenant_id())
              .order_by(TenantMembership.id)
              .all()
        )
        items = [{
            "membershipId": m.id,
            "userId": u.id,
            "username": u.username,
            "displayName": u.display_name,
            "email": u.email,
            "role": m.role,
            "isSuper": bool(u.is_super),
            "enabled": u.enabled,
        } for m, u in rows]
        return _ok(items)
    finally:
        db.close()


@tenant_me_bp.route("/members", methods=["POST"])
def add_member():
    """기존 사용자에게 tenant 멤버십 부여 (tenant_admin).

    body: { user_id 또는 username, role }
    role: tenant_admin / tenant_editor / tenant_viewer
    """
    err = _require_tenant_admin()
    if err is not None:
        return err
    body = request.get_json(force=True) or {}
    role = body.get("role", "tenant_viewer")
    if role not in TENANT_ROLES:
        return _err(f"role 은 {TENANT_ROLES} 중 하나", "VALIDATION")

    db = SessionLocal()
    try:
        # username 또는 user_id 로 사용자 찾기
        u = None
        if body.get("user_id"):
            u = db.query(User).get(int(body["user_id"]))
        elif body.get("username"):
            u = db.query(User).filter_by(username=body["username"]).first()
        if not u:
            return _err("user 없음", "NOT_FOUND", 404)

        tid = current_tenant_id()
        existing = (db.query(TenantMembership)
                      .filter_by(user_id=u.id, tenant_id=tid).first())
        if existing:
            return _err("이미 멤버", "CONFLICT", 409)

        m = TenantMembership(user_id=u.id, tenant_id=tid, role=role)
        db.add(m)
        db.commit()
        db.refresh(m)
        log_audit("tenant", "member.add", "user", u.username,
                  detail={"role": role})
        return _ok({
            "membershipId": m.id, "userId": u.id, "username": u.username, "role": role,
        }), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@tenant_me_bp.route("/members/<int:mid>", methods=["PATCH"])
def update_member_role(mid):
    """멤버 역할 변경."""
    err = _require_tenant_admin()
    if err is not None:
        return err
    body = request.get_json(force=True) or {}
    role = body.get("role")
    if role not in TENANT_ROLES:
        return _err(f"role 은 {TENANT_ROLES} 중 하나", "VALIDATION")

    db = SessionLocal()
    try:
        m = db.query(TenantMembership).get(mid)
        if not m or m.tenant_id != current_tenant_id():
            return _err("멤버십 없음", "NOT_FOUND", 404)
        old = m.role
        m.role = role
        db.commit()
        log_audit("tenant", "member.role_change", "membership", str(mid),
                  detail={"from": old, "to": role})
        return _ok({"membershipId": mid, "role": role})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@tenant_me_bp.route("/members/<int:mid>", methods=["DELETE"])
def remove_member(mid):
    """멤버십 해제 (사용자 자체는 삭제 안 함)."""
    err = _require_tenant_admin()
    if err is not None:
        return err
    db = SessionLocal()
    try:
        m = db.query(TenantMembership).get(mid)
        if not m or m.tenant_id != current_tenant_id():
            return _err("멤버십 없음", "NOT_FOUND", 404)
        uid = m.user_id
        db.delete(m)
        db.commit()
        log_audit("tenant", "member.remove", "user", str(uid))
        return _ok({"removed": mid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ─────────────────────── 사용량 (자기 tenant) ───────────────────────

@tenant_me_bp.route("/usage", methods=["GET"])
def my_usage():
    """자기 tenant 의 사용량 (모든 멤버 조회 가능 — 운영 투명성)."""
    db = SessionLocal()
    try:
        from sqlalchemy import text as sql_text
        tid = current_tenant_id()
        counts = {}
        for table, key in [
            ("pipeline", "pipelines"),
            ("import_collector", "imports"),
            ("data_catalog", "catalogs"),
            ("mqtt_connector", "mqtt_connectors"),
            ("minio_object", "minio_objects"),
            ("audit_log", "audit_events"),
        ]:
            try:
                counts[key] = db.execute(sql_text(
                    f'SELECT COUNT(*) FROM "{table}" WHERE tenant_id = :tid'
                ), {"tid": tid}).scalar()
            except Exception:
                counts[key] = 0
        try:
            counts["minio_bytes"] = int(db.execute(sql_text(
                "SELECT COALESCE(SUM(size), 0) FROM minio_object WHERE tenant_id = :tid"
            ), {"tid": tid}).scalar() or 0)
        except Exception:
            counts["minio_bytes"] = 0
        return _ok({"tenant_id": tid, "counts": counts})
    finally:
        db.close()
