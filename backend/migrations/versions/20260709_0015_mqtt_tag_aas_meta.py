"""mqtt_tag.aas 컬럼 (submodel_id_short/submodel_role/semantic_id)

Revision ID: 0015_mqtt_tag_aas_meta
Revises: 0014_mqtt_tag_json_path
Create Date: 2026-07-09

여러 submodel 이 포함된 AASX 를 다중 등록 지원하기 위한 태그 메타.
submodel_role: 'stream' (매 메시지 저장) | 'change' (값 변경 시만) | 'static' (부팅 후 첫 값만)
"""
from alembic import op
import sqlalchemy as sa

revision = "0015_mqtt_tag_aas_meta"
down_revision = "0014_mqtt_tag_json_path"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "mqtt_tag",
        sa.Column("submodel_id_short", sa.String(length=200), nullable=False, server_default=""),
    )
    op.add_column(
        "mqtt_tag",
        sa.Column("submodel_role", sa.String(length=20), nullable=False, server_default=""),
    )
    op.add_column(
        "mqtt_tag",
        sa.Column("semantic_id", sa.String(length=500), nullable=False, server_default=""),
    )


def downgrade():
    op.drop_column("mqtt_tag", "semantic_id")
    op.drop_column("mqtt_tag", "submodel_role")
    op.drop_column("mqtt_tag", "submodel_id_short")
