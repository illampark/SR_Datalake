"""Phase 8 Phase 1 — tenant 별 PG schema + user 자동 발급.

설계 (claudedocs/multitenant-storage-v1.md § 2):
- 단일 PG instance + 단일 DB (sdl) 공유, schema 분리 (`tenant_N`).
- tenant 별 PG user (`t_N_user`) 가 자기 schema 안에서만 SELECT/INSERT/CREATE.
- 비밀번호는 rotate-only (DB 에 plaintext 저장하지만 rotate API 로 재발급 가능).
- 멱등 — 이미 존재하면 skip.

사용:
    from backend.services.tenant_pg import ensure_tenant_pg
    pw = ensure_tenant_pg(tenant_id)  # 신규 생성 시 password 반환
"""
from __future__ import annotations

import logging
import secrets
from typing import Optional

import psycopg2
from urllib.parse import urlparse

from backend.config import DATABASE_URL

logger = logging.getLogger(__name__)


# tenant 1 은 legacy SDL PostgreSQL 을 그대로 사용 (sdl_user / public schema).
# tenant 2+ 부터 schema/user 분리. Phase 2 에서 T1 마이그 검토.
def schema_for(tenant_id: int) -> str:
    return f"tenant_{tenant_id}"


def pg_user_for(tenant_id: int) -> str:
    return f"t_{tenant_id}_user"


def _get_admin_raw_connection():
    """sdl_user 권한 (superuser) 의 raw psycopg2 connection (autocommit).

    SQLAlchemy session 의 connection 을 빌려쓰면 transaction 컨텍스트 충돌로
    DDL 이 rollback 될 수 있다 (CREATE USER 등). 별도 psycopg2 connect 로
    독립 connection 을 받아 autocommit 모드로 사용.
    """
    parsed = urlparse(DATABASE_URL)
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
    )
    conn.autocommit = True
    return conn


def ensure_tenant_pg(tenant_id: int, rotate_password: bool = False) -> Optional[str]:
    """tenant_N schema + t_N_user 멱등 발급.

    Returns:
        새로 생성하거나 rotate=True 일 때 새 password. 이미 존재하고
        rotate=False 면 None.

    Raises:
        Exception — DDL 실패 시. 호출자가 catch.
    """
    if tenant_id == 1:
        # T1 은 legacy SDL PostgreSQL 그대로 사용 — schema/user 발급 안 함.
        return None

    schema = schema_for(tenant_id)
    user = pg_user_for(tenant_id)
    conn = _get_admin_raw_connection()
    cur = conn.cursor()
    try:
        # 1. user 존재 여부
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
        user_exists = cur.fetchone() is not None

        new_password: Optional[str] = None
        if not user_exists:
            new_password = secrets.token_urlsafe(24)
            cur.execute(
                'CREATE USER "{u}" WITH PASSWORD %s'.format(u=user),
                (new_password,),
            )
            logger.info("PG user %s created", user)
        elif rotate_password:
            new_password = secrets.token_urlsafe(24)
            cur.execute(
                'ALTER USER "{u}" WITH PASSWORD %s'.format(u=user),
                (new_password,),
            )
            logger.info("PG user %s password rotated", user)
        # NOLOGIN 으로 비활성화된 user 도 다시 LOGIN 처리 (idempotent).
        cur.execute('ALTER USER "{u}" LOGIN'.format(u=user))

        # 2. schema (idempotent — IF NOT EXISTS)
        cur.execute(
            'CREATE SCHEMA IF NOT EXISTS "{s}" AUTHORIZATION "{u}"'.format(
                s=schema, u=user)
        )

        # 3. grants — 자기 schema 안에서 모든 권한.
        cur.execute(
            'GRANT USAGE, CREATE ON SCHEMA "{s}" TO "{u}"'.format(s=schema, u=user)
        )
        cur.execute(
            'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{s}" TO "{u}"'.format(
                s=schema, u=user)
        )
        cur.execute(
            'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{s}" TO "{u}"'.format(
                s=schema, u=user)
        )
        # 이후 새로 생성되는 객체에도 자동 grant.
        cur.execute(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA "{s}" '
            'GRANT ALL ON TABLES TO "{u}"'.format(s=schema, u=user)
        )
        cur.execute(
            'ALTER DEFAULT PRIVILEGES IN SCHEMA "{s}" '
            'GRANT ALL ON SEQUENCES TO "{u}"'.format(s=schema, u=user)
        )

        # 4. default search_path → 자기 schema.
        # 명시 schema 없는 CREATE TABLE / SELECT 가 자기 schema 로 자동 라우팅.
        cur.execute(
            'ALTER ROLE "{u}" SET search_path = "{s}"'.format(u=user, s=schema)
        )

        # 5. public schema 의 CREATE 권한 회수 — t_N_user 가 public 에
        #    실수로 테이블 생성 못 하게. USAGE 는 시스템 카탈로그 호환을 위해
        #    유지 (information_schema 일부 조회 등).
        cur.execute(
            'REVOKE CREATE ON SCHEMA public FROM "{u}"'.format(u=user)
        )

        # 6. Phase 2 — tenant 별 time_series_data 테이블 자동 생성 (LIKE public).
        #    sdl_user 가 만든 후 owner 를 t_N_user 로 이전.
        cur.execute(
            'CREATE TABLE IF NOT EXISTS "{s}".time_series_data '
            '(LIKE public.time_series_data INCLUDING ALL)'.format(s=schema)
        )
        cur.execute(
            'ALTER TABLE "{s}".time_series_data OWNER TO "{u}"'.format(s=schema, u=user)
        )

        return new_password
    finally:
        cur.close()
        conn.close()


def disable_tenant_pg(tenant_id: int) -> None:
    """tenant archive 시 호출 — user 비활성화. 데이터·schema 는 보존."""
    if tenant_id == 1:
        return
    user = pg_user_for(tenant_id)
    conn = _get_admin_raw_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
        if cur.fetchone():
            cur.execute('ALTER USER "{u}" NOLOGIN'.format(u=user))
            logger.info("PG user %s disabled (NOLOGIN)", user)
    finally:
        cur.close()
        conn.close()


def tenant_schema(tenant_id: int = None) -> str:
    """현재 또는 지정 tenant 의 PG schema 이름.
    T1 = 'public' (legacy), T2+ = 'tenant_N'.
    """
    if tenant_id is None:
        from backend.services.tenant_filter import _current_tenant_id
        tenant_id = _current_tenant_id()
    if tenant_id == 1:
        return "public"
    return schema_for(tenant_id)


def tenant_table(table_name: str, tenant_id: int = None) -> str:
    """schema-qualified 테이블 이름 — `public.time_series_data` or `tenant_N.time_series_data`.

    catalog/storage_tsdb 등 application session 통한 SELECT 에서 사용. T1 은
    기존 public 그대로, T2+ 는 자기 schema 의 테이블을 가리킴.
    """
    return f"{tenant_schema(tenant_id)}.{table_name}"


def get_pg_info(tenant_id: int) -> dict:
    """현재 tenant 의 PG 자격증명 메타정보 (password 제외).

    UI 의 시스템 정보 카드에서 사용.
    """
    if tenant_id == 1:
        return {
            "schema": "public",
            "username": "sdl_user",
            "isLegacy": True,
        }
    return {
        "schema": schema_for(tenant_id),
        "username": pg_user_for(tenant_id),
        "host": "postgres",
        "port": 5432,
        "databaseRdbms": "sdl",
        "databaseTsdb": "sdl",  # 단일 DB(sdl) + schema 격리. 폐기된 별도 DB(sdl_tsdb) 아님
        "isLegacy": False,
    }
