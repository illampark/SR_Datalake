"""super_admin 콘솔 라우트 — Phase 7.

claudedocs/multitenant-design-v1.md / rbac-target-v1.md § 9 (impersonate)
+ § 6 SYSTEM_ONLY 경로 분류.

기능:
- /api/sys/tenants — tenant CRUD (list/get/create/update/archive)
- /api/sys/impersonate/start|stop — super_admin 이 특정 tenant 로 진입/복귀
- /api/sys/usage/<tenant_id> — 사용량 미터링 (D15 빌링 미적용, 미터링만)

모든 라우트는 require_super 데코레이터로 super_admin 검증.
impersonate 중에도 super_admin 권한 유지 (rbac-target-v1.md § 5.2).
"""

from __future__ import annotations
import logging
from datetime import datetime
from flask import Blueprint, request, session, g
from backend.services.api_compat import normalize_camel_to_snake

from backend.database import SessionLocal
from backend.models.tenant import Tenant, TenantMembership
from backend.models.user import User
from backend.services.audit_logger import log_audit
from backend.services.rbac import is_super
from backend.services.tenant_provisioning import (
    create_tenant_schema, provision_tenant_minio,
)

logger = logging.getLogger(__name__)
sys_bp = Blueprint("sys", __name__, url_prefix="/api/sys")


def _ok(data=None, meta=None):
    out = {"success": True, "data": data, "error": None}
    if meta is not None:
        out["meta"] = meta
    return out


def _err(msg, code="ERROR", status=400):
    from flask import jsonify
    return jsonify({
        "success": False, "data": None,
        "error": {"code": code, "message": msg},
    }), status


def _require_super():
    if not is_super():
        return _err("super_admin only", "FORBIDDEN", 403)
    return None


# ─────────────────────── Tenant CRUD ───────────────────────

@sys_bp.route("/tenants", methods=["GET"])
def list_tenants():
    """tenant 목록 (super_admin only).

    Query:
        status: active / suspended / archived / all (기본: active)
    """
    err = _require_super()
    if err is not None:
        return err
    status_filter = (request.args.get("status") or "active").strip()
    db = SessionLocal()
    try:
        q = db.query(Tenant)
        if status_filter and status_filter != "all":
            q = q.filter(Tenant.status == status_filter)
        rows = q.order_by(Tenant.id).all()
        return _ok([t.to_dict() for t in rows])
    finally:
        db.close()


@sys_bp.route("/tenants/<int:tid>", methods=["GET"])
def get_tenant(tid):
    err = _require_super()
    if err is not None:
        return err
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(tid)
        if not t:
            return _err("tenant not found", "NOT_FOUND", 404)
        return _ok(t.to_dict())
    finally:
        db.close()


@sys_bp.route("/tenants", methods=["POST"])
def create_tenant():
    """신규 tenant 생성 + 자동 MinIO 버킷 프로비저닝.

    body: { slug, name, plan? }
    """
    err = _require_super()
    if err is not None:
        return err
    body = request.get_json(force=True) or {}
    body = normalize_camel_to_snake(body)
    slug = (body.get("slug") or "").strip()
    name = (body.get("name") or "").strip()
    plan = body.get("plan", "default")
    status = body.get("status", "active")
    settings = body.get("settings") or {}
    if not isinstance(settings, dict):
        return _err("settings는 dict 형식", "VALIDATION")
    if not slug or not name:
        return _err("slug, name 필수", "VALIDATION")
    if status not in ("active", "suspended", "archived"):
        return _err("status 잘못된 값", "VALIDATION")

    db = SessionLocal()
    try:
        if db.query(Tenant).filter_by(slug=slug).first():
            return _err(f"slug '{slug}' 이미 존재", "CONFLICT", 409)
        t = Tenant(slug=slug, name=name, status=status, plan=plan, settings=settings)
        db.add(t)
        db.flush()
        # Phase 5: MinIO 버킷 자동 생성
        try:
            buckets = provision_tenant_minio(t.id)
            logger.info("tenant %s buckets provisioned: %s", t.id, buckets)
        except Exception as e:
            logger.warning("tenant %s MinIO provision 실패: %s", t.id, e)
        # Phase 8 B-1: MinIO IAM 사용자·정책 자동 발급
        try:
            from backend.services.minio_iam import ensure_tenant_iam
            t.minio_username = ensure_tenant_iam(t.id)
            logger.info("tenant %s IAM user: %s", t.id, t.minio_username)
        except Exception as e:
            logger.warning("tenant %s IAM provision 실패: %s", t.id, e)
        db.commit()
        db.refresh(t)
        # Phase 8 Phase 1/2: PG schema/user + default Tsdb/Rdbms config 자동 발급.
        # outer db.commit() 이후에 호출 — ensure_tenant_default_storage 가 자체
        # SessionLocal() 로 commit 하는데, outer transaction 이 활성이면 우선
        # 순위 충돌로 row 가 보이지 않게 됨 (이전 사고). commit 후 별도 호출.
        try:
            from backend.services.tenant_pg import ensure_tenant_pg
            from backend.services.tenant_storage import ensure_tenant_default_storage
            pg_pw = ensure_tenant_pg(t.id)
            ensure_tenant_default_storage(t.id, pg_password=pg_pw)
            logger.info("tenant %s PG schema + default storage configured", t.id)
        except Exception as e:
            logger.warning("tenant %s PG/storage provision 실패: %s", t.id, e)
        log_audit("system", "tenant.create", "tenant", str(t.id),
                  detail={"slug": slug, "name": name})
        return _ok(t.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@sys_bp.route("/tenants/<int:tid>", methods=["PATCH"])
def update_tenant(tid):
    """tenant 메타 변경 (name, plan, settings, status)."""
    err = _require_super()
    if err is not None:
        return err
    body = request.get_json(force=True) or {}
    body = normalize_camel_to_snake(body)
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(tid)
        if not t:
            return _err("tenant not found", "NOT_FOUND", 404)
        if t.id == 0:
            return _err("system tenant 변경 불가", "FORBIDDEN", 403)
        # slug 변경 (시스템 tenant 0/1 보호)
        if "slug" in body and t.id not in (0, 1):
            new_slug = (body["slug"] or "").strip()
            if new_slug and new_slug != t.slug:
                exists = db.query(Tenant).filter(
                    Tenant.slug == new_slug, Tenant.id != t.id
                ).first()
                if exists:
                    return _err(
                        "slug {} already used".format(new_slug),
                        "CONFLICT", 409,
                    )
                t.slug = new_slug
        for k in ("name", "plan", "status"):
            if k in body:
                setattr(t, k, body[k])
        if "settings" in body and isinstance(body["settings"], dict):
            t.settings = body["settings"]
        db.commit()
        db.refresh(t)
        log_audit("system", "tenant.update", "tenant", str(tid),
                  detail={k: body[k] for k in body if k in ("name","plan","status","settings")})
        return _ok(t.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@sys_bp.route("/tenants/<int:tid>", methods=["DELETE"])
def archive_tenant(tid):
    """tenant soft-delete (archive). 실제 데이터·버킷 삭제는 별도 절차.

    tenant 0/1 은 보호.
    """
    err = _require_super()
    if err is not None:
        return err
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(tid)
        if not t:
            return _err("tenant not found", "NOT_FOUND", 404)
        if t.id in (0, 1):
            return _err(f"tenant {tid} 은 보호 — 삭제 불가", "FORBIDDEN", 403)
        t.status = "archived"
        # Phase 8 B-1: IAM 사용자 비활성화 (삭제 X — 복구 가능)
        try:
            from backend.services.minio_iam import disable_tenant_iam
            disable_tenant_iam(t.id)
        except Exception as e:
            logger.warning("tenant %s IAM disable 실패: %s", t.id, e)
        # Phase 8 Phase 1: PG user 비활성화 (NOLOGIN — schema/데이터 보존)
        try:
            from backend.services.tenant_pg import disable_tenant_pg
            disable_tenant_pg(t.id)
        except Exception as e:
            logger.warning("tenant %s PG disable 실패: %s", t.id, e)
        db.commit()
        log_audit("system", "tenant.archive", "tenant", str(tid))
        return _ok({"id": tid, "status": "archived"})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@sys_bp.route("/tenants/<int:tid>/purge", methods=["DELETE"])
def purge_tenant(tid):
    """tenant 완전 제거 — App + MinIO + PG 모두 cleanup. 복구 불가.

    archive 와 달리 모든 자원·데이터·자격증명을 영구 삭제한다.
    tenant 0/1 은 보호.

    Query/Body confirm: 슬러그 (slug) 값과 일치해야 실행 (사고 방지).
    """
    err = _require_super()
    if err is not None:
        return err
    db = SessionLocal()
    try:
        t = db.query(Tenant).get(tid)
        if not t:
            return _err("tenant not found", "NOT_FOUND", 404)
        if t.id in (0, 1):
            return _err(f"tenant {tid} 은 보호 — 완전 삭제 불가", "FORBIDDEN", 403)
        # confirm 검증 — body 의 confirmSlug 가 실제 slug 와 일치해야 진행.
        body = request.get_json(silent=True) or {}
        confirm = (body.get("confirmSlug") or body.get("confirm_slug") or "").strip()
        if confirm != (t.slug or ""):
            return _err(
                f"확인을 위해 slug ('{t.slug}') 를 정확히 입력해야 합니다.",
                "CONFIRM_REQUIRED", 400,
            )
        slug_for_log = t.slug
        try:
            from backend.services.tenant_purge import purge_tenant as _purge
            result = _purge(t.id)
        except Exception as e:
            logger.error("tenant %s purge 실패: %s", t.id, e)
            db.rollback()
            return _err(f"purge 실패: {e}", "SERVER_ERROR", 500)
        # tenant row 까지 purge 안에서 삭제됨. session 의 t 객체 참조 안 함.
        db.commit()
        log_audit("system", "tenant.purge", "tenant", str(tid),
                  detail={"slug": slug_for_log, "result": result})
        return _ok({"id": tid, "slug": slug_for_log,
                    "purged": True, "detail": result})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ─────────────────────── Impersonate ───────────────────────

@sys_bp.route("/impersonate/start", methods=["POST"])
def impersonate_start():
    """super_admin 이 특정 tenant 컨텍스트로 진입.

    body: { tenant_id, reason? }
    """
    err = _require_super()
    if err is not None:
        return err
    body = request.get_json(force=True) or {}
    body = normalize_camel_to_snake(body)
    target_tid = body.get("tenant_id")
    reason = body.get("reason", "")
    if not target_tid:
        return _err("tenant_id 필수", "VALIDATION")

    db = SessionLocal()
    try:
        t = db.query(Tenant).get(int(target_tid))
        if not t:
            return _err("tenant not found", "NOT_FOUND", 404)

        # 이전 컨텍스트 저장 (stop 시 복귀)
        session["impersonate"] = {
            "real_user_id": session["user_id"],
            "real_tenant_id": session.get("tenant_id", 0),
            "started_at": datetime.utcnow().isoformat(),
            "reason": reason,
        }
        session["tenant_id"] = t.id
        session["tenant_role"] = "tenant_admin"  # impersonate 시 기본 역할
        session.modified = True  # nested dict 변경 명시
        # is_super 는 그대로 (rbac-target-v1.md § 5.2)

        log_audit("system", "impersonate.start", "tenant", str(t.id),
                  detail={"reason": reason, "real_user": session["username"]})
        logger.info("impersonate start: %s -> tenant %s (reason: %s)",
                    session["username"], t.id, reason)
        return _ok({
            "tenant_id": t.id, "tenant_slug": t.slug, "tenant_name": t.name,
        })
    finally:
        db.close()


@sys_bp.route("/impersonate/stop", methods=["POST"])
def impersonate_stop():
    """impersonate 복귀."""
    if not session.get("impersonate"):
        return _err("impersonate 중이 아님", "VALIDATION")
    meta = session["impersonate"]
    revert_tid = meta.get("real_tenant_id", 0)
    session["tenant_id"] = revert_tid
    session["tenant_role"] = "super_admin"  # 원 컨텍스트는 super
    session.pop("impersonate", None)
    session.modified = True

    log_audit("system", "impersonate.stop", "tenant", str(revert_tid),
              detail={"duration_from": meta.get("started_at")})
    return _ok({"reverted_to": revert_tid})


# ─────────────────────── 사용량 미터링 ───────────────────────

@sys_bp.route("/usage/<int:tid>", methods=["GET"])
def tenant_usage(tid):
    """tenant 사용량 — D15 빌링 미적용, 미터링만 수집·노출.

    반환:
        pipelines, imports, catalogs, mqtt_connectors, minio_objects, audit_events
    """
    err = _require_super()
    if err is not None:
        return err
    db = SessionLocal()
    try:
        from sqlalchemy import text as sql_text
        counts = {}
        for table, key in [
            ("pipeline", "pipelines"),
            ("import_collector", "imports"),
            ("data_catalog", "catalogs"),
            ("mqtt_connector", "mqtt_connectors"),
            ("db_connector", "db_connectors"),
            ("opcua_connector", "opcua_connectors"),
            ("modbus_connector", "modbus_connectors"),
            ("api_connector", "api_connectors"),
            ("file_collector", "file_collectors"),
            ("minio_object", "minio_objects"),
            ("audit_log", "audit_events"),
            ("file_index", "file_index_rows"),
        ]:
            try:
                counts[key] = db.execute(sql_text(
                    f'SELECT COUNT(*) FROM "{table}" WHERE tenant_id = :tid'
                ), {"tid": tid}).scalar()
            except Exception:
                counts[key] = 0
        # MinIO bytes (minio_object)
        try:
            counts["minio_bytes"] = int(db.execute(sql_text(
                "SELECT COALESCE(SUM(size), 0) FROM minio_object WHERE tenant_id = :tid"
            ), {"tid": tid}).scalar() or 0)
        except Exception:
            counts["minio_bytes"] = 0
        return _ok({"tenant_id": tid, "counts": counts})
    finally:
        db.close()
