from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from backend.config import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _migrate_add_columns():
    """기존 테이블에 누락된 컬럼을 추가한다 (ALTER TABLE)."""
    insp = inspect(engine)
    _additions = [
        # (테이블명, 컬럼명, SQL 타입, 기본값)
        ("data_catalog", "connector_description", "TEXT", "''"),
        ("data_catalog", "pipeline_id", "INTEGER", "NULL"),
        ("data_catalog", "sink_type", "VARCHAR(30)", "''"),
    ]
    with engine.begin() as conn:
        for table, col, col_type, default in _additions:
            if insp.has_table(table):
                existing = [c["name"] for c in insp.get_columns(table)]
                if col not in existing:
                    conn.execute(text(
                        f'ALTER TABLE {table} ADD COLUMN {col} {col_type} DEFAULT {default}'
                    ))
        # data_level 값 마이그레이션: aggregated/archived → user_created
        if insp.has_table("data_catalog"):
            conn.execute(text(
                "UPDATE data_catalog SET data_level = 'user_created' "
                "WHERE data_level IN ('aggregated', 'archived')"
            ))


def _migrate_sdm_to_sdl():
    """sdm → sdl 네이밍 마이그레이션 (데이터 값 변환).

    기존 DB에 저장된 'sdm' 접두사 데이터를 'sdl'로 일괄 변환한다.
    이미 sdl로 되어 있는 데이터에는 영향 없음 (WHERE 조건으로 필터링).
    """
    insp = inspect(engine)

    # (테이블, 컬럼, 변환 방식) 정의
    # replace: 문자열 내 sdm → sdl 치환
    # json_replace: JSON 텍스트 내 sdm → sdl 치환
    _updates = [
        # ── 버킷명 변경 ──
        ("file_collector", "target_bucket", "replace", "sdm-", "sdl-"),
        ("dataset_request", "storage_bucket", "replace", "sdm-", "sdl-"),

        # ── MQTT 토픽 변경 ──
        ("pipeline", "input_topic", "replace", "sdm/", "sdl/"),
        ("pipeline", "output_topic", "replace", "sdm/", "sdl/"),
        ("tag_metadata", "mqtt_topic", "replace", "sdm/", "sdl/"),

        # ── 데이터 계보 ──
        ("data_lineage", "destination_target", "replace", "sdm-", "sdl-"),
        ("data_lineage", "destination_target", "replace", "sdm/", "sdl/"),

        # ── 카탈로그 접근 URL ──
        ("data_catalog", "access_url", "replace", "sdm-", "sdl-"),
        ("data_catalog", "access_url", "replace", "sdm/", "sdl/"),

        # ── 백업 이력 ──
        ("backup_history", "storage_key", "replace", "sdm-", "sdl-"),
        ("backup_history", "storage_key", "replace", "pg_sdm_if", "pg_sdl"),

        # ── TSDB/RDBMS 설정 ──
        ("tsdb_config", "name", "replace", "SDM", "SDL"),
        ("tsdb_config", "organization", "replace", "sdm-", "sdl-"),
        ("tsdb_config", "database_name", "replace", "sdm_", "sdl_"),
        ("tsdb_config", "description", "replace", "SDM", "SDL"),
        ("rdbms_config", "database_name", "replace", "sdm_", "sdl_"),
        ("rdbms_config", "username", "replace", "sdm_", "sdl_"),

        # ── DB 커넥터 ──
        ("db_connector", "database", "replace", "sdm_", "sdl_"),
        ("db_connector", "username", "replace", "sdm_", "sdl_"),

        # ── 외부 연결 ──
        ("external_connection", "database_name", "replace", "sdm_", "sdl_"),

        # ── 카탈로그 검색 태그 ──
        ("catalog_search_tag", "tag", "replace", "SDM", "SDL"),

        # ── 카탈로그 접근 URL (대문자) ──
        ("data_catalog", "access_url", "replace", "SDM", "SDL"),
    ]

    # JSON 컬럼 업데이트 (텍스트 캐스팅 후 치환)
    _json_updates = [
        # file_cleanup_policy.target_buckets: ["sdm-files","sdm-archive"] → ["sdl-*"]
        ("file_cleanup_policy", "target_buckets", "sdm-", "sdl-"),
    ]

    # MQTT 커넥터 config 내 clientId
    _mqtt_config_updates = [
        ("mqtt_connector", "config", "sdm-mqtt-", "sdl-mqtt-"),
    ]

    with engine.begin() as conn:
        # 일반 문자열 컬럼 치환
        for table, col, mode, old, new in _updates:
            if not insp.has_table(table):
                continue
            conn.execute(text(
                f"UPDATE {table} SET {col} = REPLACE({col}, :old, :new) "
                f"WHERE {col} LIKE :pattern"
            ), {"old": old, "new": new, "pattern": f"%{old}%"})

        # JSON 컬럼 치환 (PostgreSQL cast)
        for table, col, old, new in _json_updates:
            if not insp.has_table(table):
                continue
            conn.execute(text(
                f"UPDATE {table} SET {col} = "
                f"CAST(REPLACE(CAST({col} AS TEXT), :old, :new) AS JSON) "
                f"WHERE CAST({col} AS TEXT) LIKE :pattern"
            ), {"old": old, "new": new, "pattern": f"%{old}%"})

        # MQTT 커넥터 config JSON 내 clientId 치환
        for table, col, old, new in _mqtt_config_updates:
            if not insp.has_table(table):
                continue
            conn.execute(text(
                f"UPDATE {table} SET {col} = "
                f"CAST(REPLACE(CAST({col} AS TEXT), :old, :new) AS JSON) "
                f"WHERE CAST({col} AS TEXT) LIKE :pattern"
            ), {"old": old, "new": new, "pattern": f"%{old}%"})


def init_db():
    import backend.models.storage  # noqa: F401
    import backend.models.collector  # noqa: F401
    import backend.models.pipeline  # noqa: F401
    import backend.models.metadata  # noqa: F401
    import backend.models.catalog  # noqa: F401
    import backend.models.alarm  # noqa: F401
    import backend.models.system_log  # noqa: F401
    import backend.models.user  # noqa: F401
    import backend.models.backup  # noqa: F401
    import backend.models.gateway  # noqa: F401
    import backend.models.audit  # noqa: F401
    Base.metadata.create_all(bind=engine)
    _migrate_add_columns()
    _migrate_sdm_to_sdl()

    # 기존 커넥터에 대한 커넥터 레벨 카탈로그 일괄 생성
    try:
        from backend.services.metadata_tracker import backfill_connector_catalogs
        backfill_connector_catalogs()
    except Exception:
        pass

    # 기존 파이프라인 싱크에 대한 카탈로그 일괄 생성
    try:
        from backend.services.catalog_sync import backfill_pipeline_catalogs
        backfill_pipeline_catalogs()
    except Exception:
        pass
