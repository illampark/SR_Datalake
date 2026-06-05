"""tenant_foundation: tenant + tenant_membership + user.is_super + 시드

Revision ID: 0001_tenant_foundation
Revises:
Create Date: 2026-06-05

Phase 1 첫 마이그레이션. 다음을 수행:

1) tenant 테이블 생성 (BIGINT id, slug UNIQUE, status/plan/settings)
2) tenant_membership 테이블 생성 (user ↔ tenant + role)
3) app_user.is_super 컬럼 추가 (super_admin 표식)
4) 시드:
   - tenant(id=0, slug='system')  ← super_admin 소속용
   - tenant(id=1, slug='default') ← 현 운영 고객
   - 기존 app_user 전부 → tenant 1 멤버 (legacy role → tenant_role 매핑)
   - tenant_id_seq 보정 (다음 신규 tenant 가 id=2 부터 시작)

설계 근거: claudedocs/multitenant-design-v1.md § 4, rbac-target-v1.md § 3.3.

downgrade 는 신규 추가분만 되돌린다 (시드 데이터·is_super 컬럼·새 테이블).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0001_tenant_foundation"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) tenant 테이블
    op.create_table(
        "tenant",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("slug", sa.String(50), nullable=False),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("plan", sa.String(20), nullable=False, server_default="default"),
        sa.Column("settings", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        sa.UniqueConstraint("slug", name="uq_tenant_slug"),
        sa.CheckConstraint(
            "status IN ('active','suspended','archived')",
            name="ck_tenant_status",
        ),
    )
    op.create_index("ix_tenant_status", "tenant", ["status"])

    # 2) tenant_membership 테이블
    op.create_table(
        "tenant_membership",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("tenant_id", sa.BigInteger(), nullable=False),
        sa.Column("role", sa.String(30), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(["user_id"], ["app_user.id"], ondelete="CASCADE",
                                name="fk_membership_user"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant.id"], ondelete="CASCADE",
                                name="fk_membership_tenant"),
        sa.UniqueConstraint("user_id", "tenant_id", name="uq_membership_user_tenant"),
        sa.CheckConstraint(
            "role IN ('tenant_admin','tenant_editor','tenant_viewer')",
            name="ck_membership_role",
        ),
    )
    op.create_index("ix_membership_user", "tenant_membership", ["user_id"])
    op.create_index("ix_membership_tenant", "tenant_membership", ["tenant_id"])

    # 3) app_user.is_super 컬럼
    op.add_column(
        "app_user",
        sa.Column("is_super", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")),
    )

    # 4) 시드
    #    - tenant(id=0,1) 명시 ID 로 직접 INSERT
    #    - 기존 user → tenant 1 멤버 매핑
    op.execute(sa.text("""
        INSERT INTO tenant(id, slug, name, status, plan)
        VALUES
            (0, 'system',  'System',          'active', 'default'),
            (1, 'default', '기본 테넌트',      'active', 'default')
        ON CONFLICT (id) DO NOTHING
    """))

    # 시퀀스 보정 — 다음 신규 tenant 가 id=2 부터 (현재 max=1 이므로 setval(seq, 1))
    op.execute(sa.text(
        "SELECT setval(pg_get_serial_sequence('tenant','id'), "
        "GREATEST(1, (SELECT COALESCE(MAX(id),1) FROM tenant)), TRUE)"
    ))

    # 기존 app_user 전부를 tenant 1 의 멤버로 등록.
    # legacy role → tenant_role 매핑 (rbac.map_legacy_role_to_tenant_role 와 동일 규칙):
    #   admin                → tenant_admin
    #   engineer / operator  → tenant_editor   (원래 의도 살림)
    #   viewer / 그 외 / NULL → tenant_viewer
    op.execute(sa.text("""
        INSERT INTO tenant_membership(user_id, tenant_id, role)
        SELECT u.id, 1,
               CASE LOWER(COALESCE(u.role,''))
                 WHEN 'admin'    THEN 'tenant_admin'
                 WHEN 'engineer' THEN 'tenant_editor'
                 WHEN 'operator' THEN 'tenant_editor'
                 WHEN 'viewer'   THEN 'tenant_viewer'
                 ELSE 'tenant_viewer'
               END
        FROM app_user u
        ON CONFLICT (user_id, tenant_id) DO NOTHING
    """))

    # 참고: super_admin 지정(is_super=TRUE)은 운영자가 별도 명령으로 수행.
    # 마이그레이션이 임의 사용자를 super 로 만드는 것을 명시적으로 금지.


def downgrade() -> None:
    op.drop_index("ix_membership_tenant", table_name="tenant_membership")
    op.drop_index("ix_membership_user", table_name="tenant_membership")
    op.drop_table("tenant_membership")

    op.drop_column("app_user", "is_super")

    op.drop_index("ix_tenant_status", table_name="tenant")
    op.drop_table("tenant")
