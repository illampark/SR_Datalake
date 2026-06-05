"""connector name uniqueness: global → (tenant_id, name)

Revision ID: 0004_conn_name_uniq
Revises: 0003_api_key_v2
Create Date: 2026-06-05

Phase 8 - 7 개 connector 테이블의 name 컬럼이 GLOBAL UNIQUE 였던 것을
(tenant_id, name) composite UNIQUE 로 변경. 동일 이름을 서로 다른
tenant 가 자유롭게 사용 가능.

대상:
- mqtt_connector, opcua_connector, modbus_connector,
- api_connector, file_collector, db_connector, import_collector
"""
from typing import Sequence, Union

from alembic import op


revision: str = "0004_conn_name_uniq"
down_revision: Union[str, None] = "0003_api_key_v2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (table, old_constraint_name, new_constraint_name)
TABLES = [
    ("mqtt_connector",   "mqtt_connector_name_key",   "mqtt_connector_tenant_name_uniq"),
    ("opcua_connector",  "opcua_connector_name_key",  "opcua_connector_tenant_name_uniq"),
    ("modbus_connector", "modbus_connector_name_key", "modbus_connector_tenant_name_uniq"),
    ("api_connector",    "api_connector_name_key",    "api_connector_tenant_name_uniq"),
    ("file_collector",   "file_collector_name_key",   "file_collector_tenant_name_uniq"),
    ("db_connector",     "db_connector_name_key",     "db_connector_tenant_name_uniq"),
    ("import_collector", "import_collector_name_key", "import_collector_tenant_name_uniq"),
]


def upgrade() -> None:
    for table, old_uq, new_uq in TABLES:
        op.drop_constraint(old_uq, table, type_="unique")
        op.create_unique_constraint(new_uq, table, ["tenant_id", "name"])


def downgrade() -> None:
    for table, old_uq, new_uq in TABLES:
        op.drop_constraint(new_uq, table, type_="unique")
        op.create_unique_constraint(old_uq, table, ["name"])
