"""api_key v2: tenant_id + role + scopes + key_prefix + revoked_at + created_by

Revision ID: 0003_api_key_v2
Revises: 0002_tenant_id_columns
Create Date: 2026-06-05

Phase 6 - API 키를 tenant 스코프로 전환.

추가 컬럼:
- tenant_id     BIGINT NOT NULL FK -> tenant(id), DEFAULT 1
- role          VARCHAR(30) NOT NULL DEFAULT 'tenant_viewer'
                CHECK IN ('tenant_admin','tenant_editor','tenant_viewer')
- scopes        JSONB NOT NULL DEFAULT '[]' (예: ["read:pipeline","write:catalog"])
- key_prefix    VARCHAR(16) - UI 표시용 (sdl_pk_xxxx)
- revoked_at    TIMESTAMP nullable
- created_by    INTEGER FK -> app_user(id)

기존 행은 모두 tenant_id=1, role='tenant_viewer' 로 백필 (현재 0행이라 영향 없음).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0003_api_key_v2"
down_revision: Union[str, None] = "0002_tenant_id_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("api_key", sa.Column(
        "tenant_id", sa.BigInteger(), nullable=True, server_default=sa.text("1"),
    ))
    op.execute(sa.text("UPDATE api_key SET tenant_id = 1 WHERE tenant_id IS NULL"))
    op.alter_column("api_key", "tenant_id", nullable=False)

    op.add_column("api_key", sa.Column(
        "role", sa.String(30), nullable=False, server_default="tenant_viewer",
    ))
    op.add_column("api_key", sa.Column(
        "scopes", postgresql.JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb"),
    ))
    op.add_column("api_key", sa.Column("key_prefix", sa.String(16), nullable=True))
    op.add_column("api_key", sa.Column("revoked_at", sa.DateTime(), nullable=True))
    op.add_column("api_key", sa.Column("created_by", sa.Integer(), nullable=True))

    # FK + 제약
    op.create_foreign_key(
        "fk_api_key_tenant", "api_key", "tenant",
        ["tenant_id"], ["id"], ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_api_key_created_by", "api_key", "app_user",
        ["created_by"], ["id"],
    )
    op.create_check_constraint(
        "ck_api_key_role", "api_key",
        "role IN ('tenant_admin','tenant_editor','tenant_viewer')",
    )

    # 인덱스
    op.create_index("ix_api_key_tenant", "api_key", ["tenant_id"])
    op.create_index("ix_api_key_prefix", "api_key", ["key_prefix"])


def downgrade() -> None:
    op.drop_index("ix_api_key_prefix", table_name="api_key")
    op.drop_index("ix_api_key_tenant", table_name="api_key")
    op.drop_constraint("ck_api_key_role", "api_key", type_="check")
    op.drop_constraint("fk_api_key_created_by", "api_key", type_="foreignkey")
    op.drop_constraint("fk_api_key_tenant", "api_key", type_="foreignkey")
    op.drop_column("api_key", "created_by")
    op.drop_column("api_key", "revoked_at")
    op.drop_column("api_key", "key_prefix")
    op.drop_column("api_key", "scopes")
    op.drop_column("api_key", "role")
    op.drop_column("api_key", "tenant_id")
