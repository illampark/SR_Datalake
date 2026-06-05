"""처리 모듈 7 종: composite UNIQUE (tenant_id, name) 추가

Revision ID: 0006_module_name_uniq
Revises: 0005_ext_conn_name_uniq
Create Date: 2026-06-05

Phase 8 - 7 개 처리 모듈 테이블 (normalize_rule / unit_conversion / filter_rule /
anomaly_config / aggregate_config / enrich_config / script_config) 에 self-tenant
내 name 중복을 DB 레벨에서 차단. cross-tenant 는 허용.

이전엔 UNIQUE 제약이 없어 self-tenant 도 동일 이름 rule 중복 생성 가능했음.
이전 7 connector (0004) 및 external_connection (0005) 와 동일한 컨벤션 적용.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "0006_module_name_uniq"
down_revision: Union[str, None] = "0005_ext_conn_name_uniq"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (table, constraint_name)
TABLES = [
    ("normalize_rule",   "normalize_rule_tenant_name_uniq"),
    ("unit_conversion",  "unit_conversion_tenant_name_uniq"),
    ("filter_rule",      "filter_rule_tenant_name_uniq"),
    ("anomaly_config",   "anomaly_config_tenant_name_uniq"),
    ("aggregate_config", "aggregate_config_tenant_name_uniq"),
    ("enrich_config",    "enrich_config_tenant_name_uniq"),
    ("script_config",    "script_config_tenant_name_uniq"),
]


def upgrade() -> None:
    for table, con in TABLES:
        op.create_unique_constraint(con, table, ["tenant_id", "name"])


def downgrade() -> None:
    for table, con in TABLES:
        op.drop_constraint(con, table, type_="unique")
