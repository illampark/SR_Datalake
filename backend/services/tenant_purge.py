"""Phase 8 — tenant 완전 제거 (Hard delete).

`archive_tenant` 가 soft delete (IAM disable + PG NOLOGIN + status='archived')
인 반면, 본 모듈은 App row + MinIO bucket/IAM + PG schema/user 를 **영구 삭제**.
사용자 명시 호출 (DELETE /api/sys/tenants/<id>/purge) 시에만 수행되며,
sys.py 의 라우트가 super_admin 가드 + slug 확인 후 진입한다.

설계:
- App 단: tenant_id 컬럼이 있는 모든 테이블 row 일괄 삭제 → user 삭제 → tenant 삭제.
- MinIO: 5 종 bucket + 안 객체 + IAM user/policy 삭제.
- PG: schema CASCADE DROP + REASSIGN OWNED + DROP USER.

각 단계 별 실패는 logger.warning 으로 기록하고 다음 단계로 진행 (best-effort).
완전 실패해도 일부 자원은 정리되므로 다시 호출 시 멱등 cleanup 가능.
"""
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlparse

from backend.database import SessionLocal
from backend.config import (
    DATABASE_URL,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
)

logger = logging.getLogger(__name__)


# tenant 보호 — 운영 tenant (1) / 시스템 (0) 은 절대 purge 금지.
_PROTECTED_TIDS = (0, 1)


def _purge_app(tenant_id: int) -> dict:
    """App DB 의 tenant_id row 일괄 삭제 + User + Tenant row.

    SQLAlchemy 인스펙터로 tenant_id 컬럼 있는 테이블을 자동 발견.
    audit_log → 다른 종속 → tenant 순서.
    """
    from sqlalchemy import inspect, text
    from backend.models.tenant import Tenant
    from backend.models.user import User

    out = {}
    db = SessionLocal()
    try:
        insp = inspect(db.get_bind())
        tables_with_tenant: list[str] = []
        for tname in insp.get_table_names():
            cols = [c["name"] for c in insp.get_columns(tname)]
            if "tenant_id" in cols and tname != "tenant":
                tables_with_tenant.append(tname)

        # FK 의존 순서: 로그 류 먼저 → 연결 객체 → 설정 → membership 마지막.
        priority = [
            "audit_log", "syslog_event",
            "alarm_event", "alarm_rule", "alarm_channel",
            "login_history",
            "file_index", "minio_object",
            "file_cleanup_policy", "retention_policy", "retention_execution_log",
            "tsdb_config", "rdbms_config", "downsampling_policy",
            "data_catalog", "search_tag", "tag_metadata",
            "pipeline_step", "pipeline_run", "pipeline",
            "import_collector",
            "mqtt_connector", "opcua_connector", "modbus_connector",
            "api_connector", "file_connector", "db_connector",
            "external_connection", "external_connection_password",
            "tenant_membership",
        ]
        ordered = ([t for t in priority if t in tables_with_tenant]
                   + [t for t in tables_with_tenant if t not in priority])

        for tname in ordered:
            try:
                n = db.execute(
                    text(f'DELETE FROM "{tname}" WHERE tenant_id = :tid'),
                    {"tid": tenant_id},
                ).rowcount
                if n:
                    out[tname] = n
            except Exception as e:
                logger.warning("purge %s tenant=%s 실패: %s", tname, tenant_id, e)
                db.rollback()

        # tenant 의 user 중 다른 tenant membership 이 없는 user 만 삭제
        # (membership 은 위에서 삭제 — 이제 orphan user 찾기)
        orphan_users = db.execute(text("""
            SELECT u.id, u.username FROM app_user u
            WHERE u.is_super = false
              AND NOT EXISTS (
                  SELECT 1 FROM tenant_membership m WHERE m.user_id = u.id
              )
        """)).fetchall()
        n_users = 0
        for uid, uname in orphan_users:
            db.execute(text("DELETE FROM app_user WHERE id = :i"), {"i": uid})
            n_users += 1
            logger.info("purge orphan user %s (id=%s)", uname, uid)
        if n_users:
            out["app_user_orphan"] = n_users

        # tenant 자체 삭제
        n_tenant = db.execute(
            text("DELETE FROM tenant WHERE id = :tid"),
            {"tid": tenant_id},
        ).rowcount
        out["tenant"] = n_tenant
        db.commit()
    finally:
        db.close()
    return out


def _purge_minio(tenant_id: int) -> dict:
    """MinIO 5 종 bucket + 객체 + IAM user/policy 삭제."""
    from minio import Minio
    from minio.deleteobjects import DeleteObject
    from backend.services.minio_buckets import all_buckets_for
    from backend.services.minio_iam import _admin, username_for, policy_name_for

    out = {"buckets": {}, "objects": 0}
    c = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
              secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    for b in all_buckets_for(tenant_id):
        try:
            if not c.bucket_exists(b):
                continue
            keys = [o.object_name for o in c.list_objects(b, recursive=True)]
            if keys:
                list(c.remove_objects(b, [DeleteObject(k) for k in keys]))
                out["objects"] += len(keys)
            c.remove_bucket(b)
            out["buckets"][b] = "removed"
        except Exception as e:
            logger.warning("purge bucket %s 실패: %s", b, e)
            out["buckets"][b] = f"failed: {e}"

    # IAM user + policy 제거
    try:
        admin = _admin()
        try:
            admin.user_remove(username_for(tenant_id))
            out["iam_user"] = "removed"
        except Exception as e:
            out["iam_user"] = f"failed: {str(e)[:80]}"
        try:
            admin.policy_remove(policy_name_for(tenant_id))
            out["iam_policy"] = "removed"
        except Exception as e:
            out["iam_policy"] = f"failed: {str(e)[:80]}"
    except Exception as e:
        logger.warning("IAM admin client 실패: %s", e)
        out["iam_user"] = f"admin client failed: {e}"
    return out


def _purge_pg(tenant_id: int) -> dict:
    """PG schema CASCADE drop + user (REASSIGN + DROP OWNED + DROP USER)."""
    import psycopg2
    from backend.services.tenant_pg import schema_for, pg_user_for

    out = {}
    p = urlparse(DATABASE_URL)
    conn = psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        dbname=p.path.lstrip("/"),
        user=p.username, password=p.password,
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        schema = schema_for(tenant_id)
        user = pg_user_for(tenant_id)
        try:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            out["schema"] = "dropped"
        except Exception as e:
            out["schema"] = f"failed: {str(e)[:80]}"
        try:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
            if cur.fetchone():
                # REASSIGN → DROP OWNED → DROP USER
                cur.execute(f'REASSIGN OWNED BY "{user}" TO sdl_user')
                cur.execute(f'DROP OWNED BY "{user}"')
                cur.execute(f'DROP USER IF EXISTS "{user}"')
                out["user"] = "dropped"
            else:
                out["user"] = "not_exists"
        except Exception as e:
            out["user"] = f"failed: {str(e)[:80]}"
    finally:
        cur.close()
        conn.close()
    return out


def purge_tenant(tenant_id: int) -> dict:
    """tenant 완전 제거. 보호 tenant 는 예외.

    Returns: { app: {...}, minio: {...}, pg: {...} }
    """
    if tenant_id in _PROTECTED_TIDS:
        raise ValueError(f"tenant {tenant_id} 은 보호 — purge 금지")
    logger.info("purge tenant %s START", tenant_id)
    result = {
        "app": _purge_app(tenant_id),
        "minio": _purge_minio(tenant_id),
        "pg": _purge_pg(tenant_id),
    }
    logger.info("purge tenant %s DONE: %s", tenant_id, result)
    return result
