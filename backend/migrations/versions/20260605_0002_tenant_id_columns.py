"""tenant_id columns on domain tables

Revision ID: 0002_tenant_id_columns
Revises: 0001_tenant_foundation
Create Date: 2026-06-05

Phase 3 핵심 마이그레이션 — 모든 도메인 테이블에 tenant_id 컬럼 추가.

대상: 47 개 도메인 테이블 (NOT NULL DEFAULT 1, FK → tenant.id, INDEX)
     + 1 개 nullable 테이블 (system_log — 시스템 레벨 로그는 NULL)

설계 근거:
- claudedocs/multitenant-design-v1.md § 4, § 5
- claudedocs/migration-guide.md § 3 표준 패턴

단계 (각 테이블별):
1) ADD COLUMN tenant_id BIGINT DEFAULT 1 (NULL 허용으로 시작)
2) UPDATE 백필 (NULL 행을 1 로 — 기존 모든 데이터는 tenant 1)
3) ALTER COLUMN SET NOT NULL (system_log 제외)
4) ADD CONSTRAINT FK → tenant(id) ON DELETE RESTRICT
5) CREATE INDEX ix_<table>_tenant

제외 (글로벌 테이블):
- app_user, login_history, admin_setting, tenant, tenant_membership,
  api_key (Phase 6 별도), api_access_log, alembic_version

특수:
- tb_q1_plc_data 는 PLC sink 출력용 customer-specific 테이블 (model 없음).
  Phase 4 에서 sink 측 처리 (현재 마이그 대상 아님).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0002_tenant_id_columns"
down_revision: Union[str, None] = "0001_tenant_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ────────────────────────────────────────────────────────────────────────
# 대상 테이블 목록
# ────────────────────────────────────────────────────────────────────────

DOMAIN_TABLES = [
    # 파이프라인 (10)
    "pipeline", "pipeline_step", "pipeline_binding",
    "normalize_rule", "unit_conversion", "filter_rule",
    "anomaly_config", "aggregate_config", "enrich_config", "script_config",
    # 컬렉터 (12)
    "mqtt_connector", "mqtt_tag",
    "db_connector", "db_tag",
    "file_collector",
    "opcua_connector", "opcua_tag",
    "modbus_connector", "modbus_tag",
    "api_connector", "api_endpoint",
    "import_collector",
    # 스토리지 (8)
    "tsdb_config", "downsampling_policy", "rdbms_config",
    "file_cleanup_policy", "warm_aggregated_data",
    "retention_policy", "retention_execution_log",
    "time_series_data",
    # 카탈로그 (4)
    "data_catalog", "catalog_search_tag", "data_recipe", "aggregated_data",
    # 메타데이터 (2)
    "tag_metadata", "data_lineage",
    # 통합 (1)
    "external_connection",
    # 알림 (3)
    "alarm_rule", "alarm_event", "alarm_channel",
    # 감사 (1)
    "audit_log",
    # 데이터셋 요청 (1)
    "dataset_request",
    # 백업 (1)
    "backup_history",
    # 공지 (1)
    "notice",
    # 파일 인덱스 (2)
    "file_index", "file_index_state",
    # MinIO 오브젝트 (1)
    "minio_object",
]
# Total: 47 도메인 테이블

NULLABLE_TABLES = [
    # 시스템 로그 — 시스템 레벨 행 (부팅 / 마이그 / 시스템 작업) 은 tenant_id=NULL
    "system_log",
]


# ────────────────────────────────────────────────────────────────────────
# 헬퍼
# ────────────────────────────────────────────────────────────────────────

def _add_tenant_id(table: str, *, nullable: bool) -> None:
    """한 테이블에 tenant_id 컬럼 + FK + 인덱스를 추가한다."""
    # 1) 컬럼 추가 (DEFAULT 1 — 기존 행도 즉시 채워짐, PG 11+ 메타데이터만 변경)
    op.add_column(
        table,
        sa.Column(
            "tenant_id",
            sa.BigInteger(),
            nullable=True,
            server_default=sa.text("1") if not nullable else None,
        ),
    )

    # 2) 백필 (NULL 행만; nullable 테이블은 그대로 NULL 유지 가능)
    if not nullable:
        op.execute(sa.text(f'UPDATE "{table}" SET tenant_id = 1 WHERE tenant_id IS NULL'))

    # 3) NOT NULL (nullable 테이블 제외)
    if not nullable:
        op.alter_column(table, "tenant_id", nullable=False)

    # 4) FK
    op.create_foreign_key(
        f"fk_{table}_tenant",
        source_table=table,
        referent_table="tenant",
        local_cols=["tenant_id"],
        remote_cols=["id"],
        ondelete="RESTRICT",
    )

    # 5) 인덱스
    op.create_index(f"ix_{table}_tenant", table, ["tenant_id"])


def _drop_tenant_id(table: str) -> None:
    op.drop_index(f"ix_{table}_tenant", table_name=table)
    op.drop_constraint(f"fk_{table}_tenant", table, type_="foreignkey")
    op.drop_column(table, "tenant_id")


# ────────────────────────────────────────────────────────────────────────
# upgrade / downgrade
# ────────────────────────────────────────────────────────────────────────

def upgrade() -> None:
    for tbl in DOMAIN_TABLES:
        _add_tenant_id(tbl, nullable=False)

    for tbl in NULLABLE_TABLES:
        _add_tenant_id(tbl, nullable=True)


def downgrade() -> None:
    for tbl in NULLABLE_TABLES:
        _drop_tenant_id(tbl)

    for tbl in reversed(DOMAIN_TABLES):
        _drop_tenant_id(tbl)
