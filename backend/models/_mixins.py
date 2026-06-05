"""모델 mixin — Phase 3 (Multi-tenant tenant_id 컬럼)

claudedocs/multitenant-design-v1.md § 4 — 모든 도메인 모델에 적용.

belt-and-suspenders 격리의 ORM 단:
- PG schema 분리 (Phase 3 인프라, 신규 tenant 부터 적용)
- tenant_id 컬럼 (이 mixin)
- 서비스 계층 명시 필터 (Phase 4)
- before_flush 이벤트 가드 (tenant_guard.py)
"""

from sqlalchemy import Column, BigInteger, ForeignKey
from sqlalchemy.orm import declared_attr


class TenantScopedMixin:
    """도메인 테이블에 tenant_id 컬럼을 부여하는 mixin.

    - NOT NULL + default=1 — 기존 라우트는 변경 없이 tenant 1 으로 동작
    - FK → tenant(id) ON DELETE RESTRICT — tenant 삭제 시 데이터 보호
    - 인덱스는 Alembic migration 에서 별도 생성 (mixin 으로 자동 생성 X)

    적용 예:
        class Pipeline(Base, TenantScopedMixin):
            __tablename__ = "pipeline"
            ...
    """

    @declared_attr
    def tenant_id(cls):
        return Column(
            BigInteger,
            ForeignKey("tenant.id", ondelete="RESTRICT"),
            nullable=False,
            default=1,
            server_default="1",
        )


class NullableTenantScopedMixin:
    """system_log 처럼 tenant_id 가 NULL 일 수 있는 케이스용.

    시스템 레벨 행(예: 부팅 로그, 시스템 작업)은 tenant_id=NULL.
    """

    @declared_attr
    def tenant_id(cls):
        return Column(
            BigInteger,
            ForeignKey("tenant.id", ondelete="RESTRICT"),
            nullable=True,
            default=None,
        )
