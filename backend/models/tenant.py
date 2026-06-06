"""Tenant 도메인 모델 — Phase 1 (Multi-tenant 전환)

claudedocs/multitenant-design-v1.md § 4 / rbac-target-v1.md § 3 에 정의된 스키마.

이 시점에는 모델만 정의하고, 라우트는 변경하지 않는다. MULTITENANT_MODE=off (기본값)
인 경우 어떤 동작 변경도 없다.

핵심 개념:
- Tenant: 한 회사. id=0=system(super_admin 소속용), id=1=default(현 운영 고객).
- TenantMembership: User ↔ Tenant 관계 + 그 tenant에서의 role.
- 1단계는 1 user = 1 tenant (Phase 7에서 N:M 확장 검토).
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, DateTime, ForeignKey,
    UniqueConstraint, CheckConstraint, Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from backend.database import Base


# 역할 문자열 — rbac-target-v1.md § 2.1 ROLE_RANK 와 일치
TENANT_ROLES = ("tenant_admin", "tenant_editor", "tenant_viewer")


class Tenant(Base):
    """테넌트 (회사) 엔티티"""
    __tablename__ = "tenant"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    slug = Column(String(50), unique=True, nullable=False)        # URL/표시용 식별자
    name = Column(String(200), nullable=False)
    status = Column(String(20), nullable=False, default="active") # active/suspended/archived
    plan = Column(String(20), nullable=False, default="default")
    settings = Column(JSONB, nullable=False, default=dict)        # 브랜딩, locale, quota 등
    minio_username = Column(String(100), unique=True, nullable=True)  # Phase 8 B-1: SFTP IAM 사용자명 (비번은 MinIO hash 보관)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        CheckConstraint(
            "status IN ('active','suspended','archived')",
            name="ck_tenant_status",
        ),
        Index("ix_tenant_status", "status"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "slug": self.slug,
            "name": self.name,
            "status": self.status,
            "plan": self.plan,
            "settings": self.settings or {},
            "minioUsername": self.minio_username or "",
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else "",
            "updatedAt": self.updated_at.strftime("%Y-%m-%d %H:%M:%S") if self.updated_at else "",
        }


class TenantMembership(Base):
    """사용자가 특정 테넌트에서 어떤 역할인지 표현 — Phase 1은 1:1 관계 (1 user = 1 tenant)."""
    __tablename__ = "tenant_membership"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(
        Integer,
        ForeignKey("app_user.id", ondelete="CASCADE"),
        nullable=False,
    )
    tenant_id = Column(
        BigInteger,
        ForeignKey("tenant.id", ondelete="CASCADE"),
        nullable=False,
    )
    role = Column(String(30), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "tenant_id", name="uq_membership_user_tenant"),
        CheckConstraint(
            "role IN ('tenant_admin','tenant_editor','tenant_viewer')",
            name="ck_membership_role",
        ),
        Index("ix_membership_user", "user_id"),
        Index("ix_membership_tenant", "tenant_id"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "userId": self.user_id,
            "tenantId": self.tenant_id,
            "role": self.role,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else "",
        }
