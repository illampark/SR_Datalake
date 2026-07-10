"""pipeline / opcua_connector / modbus_connector / mqtt_connector 에 config_version 컬럼

Revision ID: 0016_config_version
Revises: 0015_mqtt_tag_aas_meta
Create Date: 2026-07-10

Multi-worker 환경에서 config 변경을 각 gunicorn worker 프로세스에 전파하기 위한
version 트래킹. update 라우트가 매 PUT (또는 태그 CRUD) 마다 version bump.
Reconciler 는 로컬 runtime version 과 DB version 이 다르면 자동 재기동.
"""
from alembic import op
import sqlalchemy as sa

revision = "0016_config_version"
down_revision = "0015_mqtt_tag_aas_meta"
branch_labels = None
depends_on = None


def upgrade():
    for tbl in ("pipeline", "opcua_connector", "modbus_connector", "mqtt_connector"):
        op.add_column(
            tbl,
            sa.Column("config_version", sa.Integer(), nullable=False, server_default="1"),
        )


def downgrade():
    for tbl in ("pipeline", "opcua_connector", "modbus_connector", "mqtt_connector"):
        op.drop_column(tbl, "config_version")
