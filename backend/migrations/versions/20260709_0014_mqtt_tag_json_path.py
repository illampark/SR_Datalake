"""mqtt_tag.json_path — payload 필드 파싱 경로

Revision ID: 0014_mqtt_tag_json_path
Revises: 0013_uniq_membership_user
Create Date: 2026-07-09

MQTT 커넥터에서 하나의 토픽 payload 가 다중 필드 JSON 인 경우, 각 태그가
어느 필드를 취할지 결정하기 위한 경로. Benthos mapping 은 값이 비어있으면
payload 전체를 value 로 취급 (기존 동작).
"""
from alembic import op
import sqlalchemy as sa

revision = "0014_mqtt_tag_json_path"
down_revision = "0013_uniq_membership_user"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "mqtt_tag",
        sa.Column("json_path", sa.String(length=500), nullable=False, server_default=""),
    )


def downgrade():
    op.drop_column("mqtt_tag", "json_path")
