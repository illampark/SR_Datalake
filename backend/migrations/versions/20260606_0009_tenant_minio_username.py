"""tenant.minio_username 컬럼 추가 — MinIO IAM 자격증명 메타

Revision ID: 0009_tenant_minio_uname
Revises: 0008_storage_policy_uniq
Create Date: 2026-06-06

Phase 8 B-1 — 각 tenant 의 SFTP/S3 자격증명을 MinIO IAM 으로 분리.
비밀번호는 rotate-only (MinIO 자체가 hash 보관). DB 에는 username 만.

기존 tenant 1·2 는 router-level backfill 로 보강 (별도 1회 호출).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0009_tenant_minio_uname"
down_revision: Union[str, None] = "0008_storage_policy_uniq"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tenant",
        sa.Column("minio_username", sa.String(length=100), nullable=True),
    )
    op.create_unique_constraint(
        "tenant_minio_username_uniq", "tenant", ["minio_username"],
    )


def downgrade() -> None:
    op.drop_constraint("tenant_minio_username_uniq", "tenant", type_="unique")
    op.drop_column("tenant", "minio_username")
