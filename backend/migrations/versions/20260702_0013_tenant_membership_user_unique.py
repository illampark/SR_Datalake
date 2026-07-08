"""tenant_membership.user_id UNIQUE — 1 user = 1 tenant 강제

Revision ID: 0013_uniq_membership_user
Revises: 0012_email_unique
Create Date: 2026-07-02

원 설계 (multitenant-design-v1.md § 6 — Phase 1 D6) 는 `1 user = 1 tenant`
이지만 tenant_membership 스키마가 다대다 를 허용해 방치돼 있었다.
auth_login 은 첫 membership 하나만 취하는데 정렬이 없어 여러 개면
반환 순서 (예측 불가) 로 세션의 tenant_id 가 결정되는 결함.

프로덕션에서 super_admin 이 다른 테넌트 사용자의 displayName 을 편집할
때 update_user 가 자기 default 컨텍스트에 새 membership 을 UPSERT 하던
버그로 dd1/dd2/dd3 가 daeduck + default 이중 소속이 됐던 사례가 있었다.
버그는 이미 fix (commit 5165eba) 되어 있고 데이터도 정정됨.

본 마이그레이션은 DB 레벨에서 이 원칙을 강제한다. 3 서버 스캔 결과
이중 membership 사용자 0 명 (super 제외) — 데이터 정리 부담 없음.

super_admin 이 다른 tenant 에 접근해야 하는 경우는 impersonate 흐름을
사용 (운영자 콘솔 → 테넌트 목록 → [Impersonate]) — 기존 그대로 지원.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0013_uniq_membership_user"
down_revision: Union[str, None] = "0012_email_unique"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 안전 가드: 혹시 남아 있는 이중 membership 은 tenant_id 가장 작은 것만 유지.
    # (실운영 스캔 결과 0 명이지만, 다른 환경에서 마이그 시 안전을 위해)
    op.execute("""
        DELETE FROM tenant_membership tm
         WHERE EXISTS (
             SELECT 1 FROM tenant_membership tm2
              WHERE tm2.user_id = tm.user_id
                AND tm2.tenant_id < tm.tenant_id
         )
    """)

    # UNIQUE 제약 추가 — user_id 는 반드시 하나의 tenant 에만 소속
    op.create_unique_constraint(
        "tenant_membership_user_id_unique",
        "tenant_membership",
        ["user_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "tenant_membership_user_id_unique",
        "tenant_membership",
        type_="unique",
    )
