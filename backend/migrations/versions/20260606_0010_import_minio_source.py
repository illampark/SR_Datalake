"""import_collector: source_mode=minio_bucket 컬럼 추가

Revision ID: 0010_import_minio_src
Revises: 0009_tenant_minio_uname
Create Date: 2026-06-06

Phase 8 B-2 — import collector 가 호스트 fs (local_path) 외에 MinIO bucket
도 source 로 받을 수 있도록 컬럼 추가.

기존 source_mode (string) 컬럼 값에 'minio_bucket' 이 추가됨. 컬럼 자체는
이미 존재 (Phase 3) 이므로 마이그 불요.

신규 컬럼:
- source_bucket  (varchar 100, nullable) — 예: 't-2-imports', 'sdl-imports'
- source_prefix  (varchar 500, nullable) — 예: '2026/q1/', '' (bucket root)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0010_import_minio_src"
down_revision: Union[str, None] = "0009_tenant_minio_uname"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "import_collector",
        sa.Column("source_bucket", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "import_collector",
        sa.Column("source_prefix", sa.String(length=500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("import_collector", "source_prefix")
    op.drop_column("import_collector", "source_bucket")
