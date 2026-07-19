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
        ("import_collector", "source_mode", "VARCHAR(20)", "'upload'"),
        ("import_collector", "local_path", "VARCHAR(1000)", "''"),
        ("import_collector", "file_patterns", "JSON", "'[\"*\"]'"),
        ("import_collector", "recursive", "BOOLEAN", "true"),
        # local_path import 후 소스 정리 정책 (MinIO 정본화) — 기존 행은 keep 유지
        ("import_collector", "post_import_action", "VARCHAR(20)", "'keep'"),
        ("import_collector", "archive_subdir", "VARCHAR(200)", "'.imported'"),
        # TagMetadata 거버넌스 필드
        ("tag_metadata", "description", "TEXT", "''"),
        ("tag_metadata", "owner", "VARCHAR(100)", "''"),
        ("tag_metadata", "category", "VARCHAR(100)", "''"),
        ("tag_metadata", "data_level", "VARCHAR(20)", "'raw'"),
        ("tag_metadata", "sensitivity", "VARCHAR(20)", "'internal'"),
        ("tag_metadata", "retention_policy", "VARCHAR(100)", "''"),
        ("tag_metadata", "is_published", "BOOLEAN", "true"),
        ("tag_metadata", "is_deprecated", "BOOLEAN", "false"),
        # PipelineStep per-step run counters (run_file_source 가 throttle commit)
        ("pipeline_step", "processed_count", "INTEGER", "0"),
        ("pipeline_step", "error_count", "INTEGER", "0"),
        ("pipeline_step", "dropped_count", "INTEGER", "0"),
        ("pipeline_step", "last_processed_at", "TIMESTAMP", "NULL"),
        # DatasetRequest — Tier 2 단일 카탈로그 비동기 export 연결
        ("dataset_request", "catalog_id", "INTEGER", "NULL"),
        ("dataset_request", "where_clause", "TEXT", "''"),
        ("dataset_request", "column_filters", "JSON", "'[]'"),
        # config_version — multi-worker config 전파 (e6ad44e). 모델에만 추가되고
        # 마이그레이션이 누락되어, 기존 DB 에 신규 이미지를 올리면
        # "column pipeline.config_version does not exist" 로 파이프라인/커넥터
        # 조회가 전부 깨진다 (create_all 은 테이블만 만들고 컬럼은 안 채운다).
        ("pipeline", "config_version", "INTEGER", "1"),
        ("opcua_connector", "config_version", "INTEGER", "1"),
        ("modbus_connector", "config_version", "INTEGER", "1"),
        ("mqtt_connector", "config_version", "INTEGER", "1"),
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


def _migrate_fill_internal_storage_credentials():
    """tenant 1 legacy 내부 스토리지(TsdbConfig/RdbmsConfig) 설정 정합화.

    tenant 1 의 default 인스턴스는 과거 잘못 시드되어 두 종류의 드리프트가 있다.

    (A) database_name 오설정: 초기 Phase 8 설계가 TSDB 를 별도 DB `sdl_tsdb` 로
        분리하려다 폐기되고 "sdl DB 내 schema 격리"로 바뀌었으나, 기존 tenant 1
        TsdbConfig 행이 database_name='sdl_tsdb'(빈 DB) 로 남았다. psycopg2 로
        이 값에 직접 붙는 조회 경로(storage_tsdb query·retention)가
        `relation "time_series_data" does not exist` 로 실패한다.
        → 실데이터가 있는 config.DB_NAME(=sdl) 으로 교정. schema 격리는 유지.

    (B) 빈 자격증명: password="" 로 시드된 행. INSERT 경로는 (구)하드코딩 폴백으로
        동작했지만 직접 조회 경로는 fe_sendauth 로 실패한다. 폴백 제거 후를 위해
        DATABASE_URL 자격증명으로 채운다.

    tenant 1 로 한정 — tenant N(N>1) 은 tenant_pg 가 발급한 t_N_user / tenant_N
    schema 로 PG 단 격리를 성립시키므로, 여기서 공유 sdl_user 로 덮어쓰면 격리가
    깨진다. tenant N 의 빈 자격증명은 백필 대상이 아니라 tenant_pg 재발급 대상이다.
    host 가 내부 DB 와 같은 행만 — 외부 RDBMS 설정은 안 건드린다.
    (A) 를 먼저 해야 (B) 의 database_name 매칭이 성립한다.
    """
    from backend import config

    if not config.DB_PASSWORD:
        return

    insp = inspect(engine)
    with engine.begin() as conn:
        for table in ("tsdb_config", "rdbms_config"):
            if not insp.has_table(table):
                continue
            cols = [c["name"] for c in insp.get_columns(table)]
            if "tenant_id" not in cols:
                continue
            # (A) database_name 교정: 내부 DB 를 가리키는 tenant 1 행의
            #     database_name 이 비었거나 폐기된 'sdl_tsdb' 면 config.DB_NAME 으로.
            conn.execute(text(
                f"UPDATE {table} SET database_name = :dbname "
                f"WHERE tenant_id = 1 AND host = :host "
                f"AND (database_name IS NULL OR database_name = '' "
                f"     OR database_name = 'sdl_tsdb')"
            ), {"dbname": config.DB_NAME, "host": config.DB_HOST})
            # (B) 빈 자격증명 백필 (database_name 교정 후 매칭).
            conn.execute(text(
                f"UPDATE {table} SET username = :user, password = :pw "
                f"WHERE tenant_id = 1 "
                f"AND (password IS NULL OR password = '') "
                f"AND host = :host AND database_name = :dbname"
            ), {
                "user": config.DB_USER,
                "pw": config.DB_PASSWORD,
                "host": config.DB_HOST,
                "dbname": config.DB_NAME,
            })


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
    import backend.models.dataset  # noqa: F401
    import backend.models.file_index  # noqa: F401
    import backend.models.minio_object  # noqa: F401

    # 멀티 워커 동시 부팅 시 CREATE TABLE / ALTER TABLE 가 race 하지 않도록
    # PostgreSQL advisory lock 으로 직렬화. 비-PG 백엔드면 lock 없이 진행.
    # key 0x53444C5F494E4954 = 'SDL_INIT'
    is_pg = engine.dialect.name == "postgresql"
    if is_pg:
        with engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_lock(0x53444C5F494E4954)"))
            try:
                Base.metadata.create_all(bind=engine)
                _migrate_add_columns()
                _migrate_sdm_to_sdl()
                _migrate_fill_internal_storage_credentials()
            finally:
                conn.execute(text("SELECT pg_advisory_unlock(0x53444C5F494E4954)"))
    else:
        Base.metadata.create_all(bind=engine)
        _migrate_add_columns()
        _migrate_sdm_to_sdl()
        _migrate_fill_internal_storage_credentials()

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
