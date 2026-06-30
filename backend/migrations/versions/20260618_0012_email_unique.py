"""app_user.email UNIQUE+NOT NULL + username UNIQUE drop + length 100

Revision ID: 0012_email_unique
Revises: 0011_tsdb_schema_name
Create Date: 2026-06-18

Phase 8+ — 로그인 ID 를 username 에서 email 로 전환. tenant 별 같은 username
(예: admin@acme.com / admin@beta.com → username 모두 "admin") 을 허용하기
위해 username 의 UNIQUE 제약을 제거한다.

신규 사용자는 email 을 입력하고 username 은 local part 가 자동 추출됨.
같은 tenant 안 username 충돌 시 자동 suffix (admin2, admin3).

Phase 8+ B-7 deprecation: 1-2 주 운영 후 username 인증 fallback 제거 예정.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0012_email_unique"
down_revision: Union[str, None] = "0011_tsdb_schema_name"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) email 빈 행 백필 — '{username}@local' 형식 (운영자 후속 수정 가능)
    op.execute("""
        UPDATE app_user
           SET email = username || '@local'
         WHERE COALESCE(email, '') = ''
    """)

    # 2) email NOT NULL
    op.alter_column("app_user", "email",
                    existing_type=sa.String(length=200),
                    nullable=False)

    # 3) email UNIQUE 제약
    op.create_unique_constraint(
        "app_user_email_unique", "app_user", ["email"]
    )

    # 4) username UNIQUE drop (tenant 별 중복 허용)
    op.execute("ALTER TABLE app_user DROP CONSTRAINT IF EXISTS app_user_username_key")

    # 5) username 컬럼 길이 50 → 100 (email local part 가 50 이상 가능)
    op.alter_column("app_user", "username",
                    existing_type=sa.String(length=50),
                    type_=sa.String(length=100))


def downgrade() -> None:
    op.alter_column("app_user", "username",
                    existing_type=sa.String(length=100),
                    type_=sa.String(length=50))
    # username UNIQUE 복원 — 중복 row 가 있으면 실패할 수 있음 (downgrade 시 운영자 사전 정리 필요)
    op.create_unique_constraint(
        "app_user_username_key", "app_user", ["username"]
    )
    op.drop_constraint("app_user_email_unique", "app_user", type_="unique")
    op.alter_column("app_user", "email",
                    existing_type=sa.String(length=200),
                    nullable=True)
