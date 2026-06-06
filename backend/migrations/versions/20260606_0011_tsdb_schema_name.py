"""tsdb_config: schema_name 컬럼 추가 (Phase 8 Phase 2 — TSDB schema 격리)

Revision ID: 0011_tsdb_schema_name
Revises: 0010_import_minio_src
Create Date: 2026-06-06

tenant 별 PG schema (`tenant_N`) 에 자체 `time_series_data` 테이블을 두기 위해
TsdbConfig 에 schema_name 컬럼 추가. RdbmsConfig 와 동일 패턴.

기본값 'public' — T1 legacy 호환 (기존 SDL TSDB 가 public.time_series_data 사용).
T2+ 는 ensure_tenant_default_storage 가 'tenant_N' 으로 셋.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0011_tsdb_schema_name"
down_revision: Union[str, None] = "0010_import_minio_src"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tsdb_config",
        sa.Column("schema_name", sa.String(length=100),
                  nullable=False, server_default="public"),
    )


def downgrade() -> None:
    op.drop_column("tsdb_config", "schema_name")
