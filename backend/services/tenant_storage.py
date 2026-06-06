"""Phase 8 Phase 1 — tenant 별 default TsdbConfig + RdbmsConfig 자동 발급.

설계:
- 신규 tenant 생성 시 호출되어 즉시 스토리지 화면·import target 으로 사용 가능.
- TsdbConfig 는 표시용 (실제 INSERT 는 application session 통과) — 공유 sdl_user.
  Phase 2 에서 per-tenant schema/user 로 분리 예정.
- RdbmsConfig 는 tenant_pg 가 발급한 t_N_user / tenant_N schema 사용.
  import_parser 의 RDBMS 분기가 이 user 로 connect → PG 단 격리 자동 성립.
- 멱등.
"""
from __future__ import annotations

import logging
from typing import Optional

from backend.database import SessionLocal
from backend.models.storage import TsdbConfig, RdbmsConfig
from backend.services.tenant_pg import schema_for, pg_user_for

logger = logging.getLogger(__name__)


def ensure_tenant_default_storage(
    tenant_id: int, pg_password: Optional[str] = None,
) -> dict:
    """tenant 의 default TsdbConfig + RdbmsConfig 멱등 발급.

    Args:
        tenant_id: 대상 tenant
        pg_password: tenant_pg 가 신규 발급한 PG user password. None 이면
            RdbmsConfig 의 password 컬럼 미수정 (기존 값 유지 또는 빈 문자열).

    Returns:
        {"tsdb": {...}, "rdbms": {...}} — 생성·확인된 row 의 to_dict.
    """
    db = SessionLocal()
    try:
        # ── TsdbConfig (표시용 + retention 설정 보관) ──
        tsdb_name = "SDL TSDB" if tenant_id == 1 else f"Tenant {tenant_id} TSDB"
        tsdb = (
            db.query(TsdbConfig)
            .filter_by(tenant_id=tenant_id, name=tsdb_name)
            .first()
        )
        if not tsdb:
            tsdb = TsdbConfig(
                tenant_id=tenant_id,
                name=tsdb_name,
                db_type="PostgreSQL",
                host="postgres",
                port=5432,
                database_name="sdl_tsdb",
                username="sdl_user",
                password="",  # 공유 application user — 비밀번호는 환경변수에서.
                status="connected",
                retention_days=30,
                description=(
                    f"Tenant {tenant_id} default TSDB (shared sdl_tsdb) — "
                    "Phase 2 에서 schema 분리 예정"
                ),
            )
            db.add(tsdb)
            logger.info("tenant %s TsdbConfig created (id will be set after flush)", tenant_id)

        # ── RdbmsConfig (tenant 별 user/schema 격리) ──
        rdbms_name = "SDL PostgreSQL" if tenant_id == 1 else f"Tenant {tenant_id} PostgreSQL"
        rdbms = (
            db.query(RdbmsConfig)
            .filter_by(tenant_id=tenant_id, name=rdbms_name)
            .first()
        )
        if not rdbms:
            if tenant_id == 1:
                # T1 legacy — 기존 SDL PostgreSQL 패턴 유지 (sdl_user / public).
                rdbms = RdbmsConfig(
                    tenant_id=tenant_id,
                    name=rdbms_name,
                    db_type="PostgreSQL",
                    host="postgres",
                    port=5432,
                    database_name="sdl",
                    schema_name="public",
                    username="sdl_user",
                    password="",
                    status="connected",
                    description="SDL legacy PostgreSQL (shared sdl_user / public)",
                )
            else:
                rdbms = RdbmsConfig(
                    tenant_id=tenant_id,
                    name=rdbms_name,
                    db_type="PostgreSQL",
                    host="postgres",
                    port=5432,
                    database_name="sdl",
                    schema_name=schema_for(tenant_id),
                    username=pg_user_for(tenant_id),
                    password=pg_password or "",
                    status="connected",
                    description=(
                        f"Tenant {tenant_id} isolated PostgreSQL "
                        f"(schema={schema_for(tenant_id)}, "
                        f"user={pg_user_for(tenant_id)})"
                    ),
                )
            db.add(rdbms)
            logger.info("tenant %s RdbmsConfig created", tenant_id)
        elif pg_password and tenant_id != 1:
            # 기존 row 가 있으면서 새 password 가 들어왔으면 (rotate) 갱신.
            rdbms.password = pg_password
            logger.info("tenant %s RdbmsConfig password updated (rotate)", tenant_id)

        db.commit()
        db.refresh(tsdb)
        db.refresh(rdbms)
        return {
            "tsdb": {"id": tsdb.id, "name": tsdb.name},
            "rdbms": {
                "id": rdbms.id, "name": rdbms.name,
                "schema": rdbms.schema_name, "username": rdbms.username,
            },
        }
    except Exception as e:
        db.rollback()
        logger.error("ensure_tenant_default_storage(%s) failed: %s", tenant_id, e)
        raise
    finally:
        db.close()
