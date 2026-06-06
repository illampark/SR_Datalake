"""storage 정책 UNIQUE: RetentionPolicy / FileCleanupPolicy 의 (tenant_id) +
DownsamplingPolicy 의 (tenant_id, tsdb_id, policy_name)

Revision ID: 0008_storage_policy_uniq
Revises: 0007_pipeline_name_uniq
Create Date: 2026-06-06

Phase 8 - 스토리지 정책 모델에 UNIQUE 제약을 추가해 운영 정합성 강화:

- retention_policy: tenant 당 1 정책 강제. 이전엔 코드가 .first() 로 자기
  tenant 의 정책 선택했는데 row 가 여러 개여도 막을 수 없었음.
- file_cleanup_policy: 동일.
- downsampling_policy: 같은 tenant + 같은 tsdb 인스턴스 안에서 같은
  policy_name 중복 차단.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "0008_storage_policy_uniq"
down_revision: Union[str, None] = "0007_pipeline_name_uniq"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (table, constraint_name, columns)
CONSTRAINTS = [
    ("retention_policy",     "retention_policy_tenant_uniq",     ["tenant_id"]),
    ("file_cleanup_policy",  "file_cleanup_policy_tenant_uniq",  ["tenant_id"]),
    ("downsampling_policy",  "downsampling_policy_tenant_tsdb_name_uniq",
     ["tenant_id", "tsdb_id", "policy_name"]),
]


def upgrade() -> None:
    for table, con, cols in CONSTRAINTS:
        op.create_unique_constraint(con, table, cols)


def downgrade() -> None:
    for table, con, _cols in CONSTRAINTS:
        op.drop_constraint(con, table, type_="unique")
