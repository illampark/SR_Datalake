"""external_connection: composite UNIQUE (tenant_id, name) 추가

Revision ID: 0005_ext_conn_name_uniq
Revises: 0004_conn_name_uniq
Create Date: 2026-06-05

Phase 8 - 외부 연결 (ExternalConnection) 도 7 종 connector 와 동일하게
self-tenant 내 name 중복을 DB 레벨에서 차단. cross-tenant 는 허용.

이전엔 unique 제약 자체가 없어 self-tenant 도 중복 허용됐음 (예: T1 에
'T1 rdbms warm' 2개 들어가는 케이스 확인).
"""
from typing import Sequence, Union

from alembic import op


revision: str = "0005_ext_conn_name_uniq"
down_revision: Union[str, None] = "0004_conn_name_uniq"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_CON = "external_connection_tenant_name_uniq"


def upgrade() -> None:
    op.create_unique_constraint(_CON, "external_connection", ["tenant_id", "name"])


def downgrade() -> None:
    op.drop_constraint(_CON, "external_connection", type_="unique")
