"""Alembic environment — SDL Multi-tenant 마이그레이션

claudedocs/migration-guide.md § 2 참조.

이 모듈은 Alembic CLI 호출 시 매번 실행된다.
- DATABASE_URL: backend.config 또는 환경변수에서 읽음 (alembic.ini 의 sqlalchemy.url 무시)
- target_metadata: backend.models 의 Base.metadata
- compare_type: True (컬럼 타입 변경 감지)

L2 schema-per-tenant 다중 스키마 적용은 Phase 3 에서 활성화 (현재는 단일 schema=public).
"""

from __future__ import annotations
import os
import sys
from pathlib import Path
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from alembic import context

# repo 루트를 path 에 추가 — alembic 이 어디서 실행되든 backend 패키지가 보이도록.
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

# Alembic Config 객체
config = context.config

# logging.fileConfig — alembic.ini 의 [loggers] 등을 적용
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# DATABASE_URL 결정 우선순위:
#   1) 환경변수 DATABASE_URL  (운영·CI 표준)
#   2) backend.config.DATABASE_URL  (코드의 기본값)
db_url = os.getenv("DATABASE_URL")
if not db_url:
    from backend.config import DATABASE_URL as _CFG_URL
    db_url = _CFG_URL
config.set_main_option("sqlalchemy.url", db_url)

# target_metadata: backend.models 의 모든 모델이 매핑된 Base.metadata
# (import 부작용으로 모든 모델 클래스가 등록되어야 함)
from backend.database import Base                                            # noqa: E402
import backend.models                                                        # noqa: E402,F401

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """오프라인 모드 — SQL 파일만 출력, DB 연결 없음."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """온라인 모드 — 실제 DB 에 연결해 마이그레이션 적용."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
