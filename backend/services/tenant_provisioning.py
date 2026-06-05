"""테넌트 프로비저닝 서비스 — Phase 1 (L2 schema 인프라 준비)

claudedocs/migration-guide.md § 9 / multitenant-design-v1.md § 4 에 정의된
schema-per-tenant 모델의 핵심 인프라.

이 모듈은 신규 tenant가 추가될 때 PG schema `t_<id>`를 생성하고
도메인 테이블을 복제하는 절차를 캡슐화한다.

Phase 1에서는 정의만 두고 라우트에서 호출하지 않는다 (super_admin
콘솔이 Phase 7에서 생기면 그때 활용). 자동 테스트로만 호출해 동작을 검증.

설계 결정 (multitenant-design-v1.md D2/D3):
- tenant 1 은 `public` schema 유지 (legacy 그대로)
- tenant 2+ 는 `t_<id>` schema 사용
- 도메인 테이블만 복제 (글로벌 테이블은 public 에 유지)
"""

from __future__ import annotations

from typing import Iterable
from sqlalchemy import text
from sqlalchemy.orm import Session


# 글로벌 테이블 — schema 분리 대상이 아님.
# multitenant-design-v1.md § 5.2 에 정의.
GLOBAL_TABLES = frozenset({
    "tenant",
    "tenant_membership",
    "app_user",          # 사용자 풀은 글로벌, 멤버십으로 tenant 소속 표현
    "login_history",
    "admin_setting",
    "api_key",           # tenant_id 컬럼은 갖되 테이블 자체는 public 위치
    "api_access_log",
    "alembic_version",   # Alembic 메타
})


def schema_for(tenant_id: int) -> str:
    """tenant id → schema name. tenant 1 은 public 으로 매핑."""
    if tenant_id == 1:
        return "public"
    if tenant_id <= 0:
        raise ValueError(f"invalid tenant_id for schema: {tenant_id}")
    return f"t_{tenant_id}"


def list_domain_tables(session: Session) -> list[str]:
    """public schema 의 현재 테이블 중 글로벌 제외 = 도메인 테이블 후보."""
    rows = session.execute(text(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )).fetchall()
    return [r[0] for r in rows if r[0] not in GLOBAL_TABLES]


def create_tenant_schema(session: Session, tenant_id: int,
                          domain_tables: Iterable[str] | None = None) -> str:
    """신규 tenant 의 schema 와 도메인 테이블을 생성한다.

    - schema `t_<id>` 생성 (이미 있으면 에러 — 멱등성은 호출 측에서 보장)
    - public 의 도메인 테이블을 `LIKE ... INCLUDING ALL` 로 복제
    - 글로벌 테이블은 복제 안 함

    Phase 1 에서는 정의만, 실제 사용은 Phase 7 의 super_admin 콘솔에서.
    """
    schema = schema_for(tenant_id)
    if schema == "public":
        raise ValueError("tenant 1 uses public schema — provisioning skipped")

    if domain_tables is None:
        domain_tables = list_domain_tables(session)
    else:
        domain_tables = list(domain_tables)

    # 1) schema
    session.execute(text(f'CREATE SCHEMA "{schema}"'))

    # 2) 도메인 테이블 복제
    for tbl in domain_tables:
        session.execute(text(
            f'CREATE TABLE "{schema}"."{tbl}" '
            f'(LIKE public."{tbl}" INCLUDING ALL)'
        ))

    return schema


def drop_tenant_schema(session: Session, tenant_id: int) -> None:
    """tenant 의 schema 와 그 안의 모든 테이블을 제거한다 (CASCADE).

    매우 파괴적인 작업이므로 super_admin 콘솔에서만 호출하며
    사전 확인 + 백업 단계를 거친다.
    """
    schema = schema_for(tenant_id)
    if schema == "public":
        raise ValueError("refuse to drop public schema")
    session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def set_search_path(session: Session, tenant_id: int) -> None:
    """세션의 search_path 를 해당 tenant schema 우선으로 설정.

    글로벌 테이블(public) 도 보이도록 public 을 fallback 으로 둔다.
    """
    schema = schema_for(tenant_id)
    if schema == "public":
        session.execute(text("SET search_path TO public"))
    else:
        session.execute(text(f'SET search_path TO "{schema}", public'))


def list_tenant_schemas(session: Session) -> list[str]:
    """현재 존재하는 tenant 관련 schema (t_*) 목록."""
    rows = session.execute(text(
        "SELECT nspname FROM pg_namespace WHERE nspname LIKE 't\\_%' ESCAPE '\\' ORDER BY nspname"
    )).fetchall()
    return [r[0] for r in rows]
