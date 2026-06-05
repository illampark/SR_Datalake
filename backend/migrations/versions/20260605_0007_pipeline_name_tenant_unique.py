"""Pipeline.name: global UNIQUE → composite (tenant_id, name)

Revision ID: 0007_pipeline_name_uniq
Revises: 0006_module_name_uniq
Create Date: 2026-06-05

Phase 8 - pipeline 테이블의 name 컬럼이 GLOBAL UNIQUE (pipeline_name_key) 였던
것을 composite (tenant_id, name) UNIQUE 로 변경. 같은 이름의 pipeline 을 서로
다른 tenant 가 자유롭게 만들 수 있도록.

이전 7 connector (0004) / external_connection (0005) / 7 처리 모듈 (0006) 과
동일한 멀티테넌트 UNIQUE 컨벤션 적용.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "0007_pipeline_name_uniq"
down_revision: Union[str, None] = "0006_module_name_uniq"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_OLD = "pipeline_name_key"
_NEW = "pipeline_tenant_name_uniq"


def upgrade() -> None:
    op.drop_constraint(_OLD, "pipeline", type_="unique")
    op.create_unique_constraint(_NEW, "pipeline", ["tenant_id", "name"])


def downgrade() -> None:
    op.drop_constraint(_NEW, "pipeline", type_="unique")
    op.create_unique_constraint(_OLD, "pipeline", ["name"])
