import logging
from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, text as _sql_text
from backend.database import SessionLocal
from backend.models.storage import RdbmsConfig
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size

logger = logging.getLogger(__name__)

rdbms_bp = Blueprint("storage_rdbms", __name__, url_prefix="/api/storage/rdbms")


# ──────────────────────────────────────────────
# 시스템 운영에 필수인 SDL 코어 테이블 — DROP/TRUNCATE 차단 화이트리스트
# ──────────────────────────────────────────────
SYSTEM_PROTECTED_TABLES = frozenset({
    # 사용자/인증/감사
    "app_user", "login_history", "audit_log", "admin_setting", "notice",
    # 커넥터·태그
    "opcua_connector", "opcua_tag",
    "modbus_connector", "modbus_tag", "mqtt_connector", "mqtt_tag",
    "api_connector", "api_endpoint", "file_collector",
    "db_connector", "db_tag", "import_collector",
    # 파이프라인·모듈
    "pipeline", "pipeline_step", "pipeline_binding",
    "normalize_rule", "filter_rule", "anomaly_config", "aggregate_config",
    "enrich_config", "script_config", "unit_conversion",
    # 데이터·메타
    "data_catalog", "catalog_search_tag", "data_lineage", "data_recipe",
    "tag_metadata", "aggregated_data", "warm_aggregated_data",
    "downsampling_policy", "time_series_data",
    # 스토리지·정책
    "tsdb_config", "rdbms_config", "file_cleanup_policy",
    "retention_policy", "retention_execution_log",
    # 알람·게이트웨이·시스템
    "alarm_rule", "alarm_channel", "alarm_event",
    "api_key", "api_access_log",
    "backup_history", "system_log", "external_connection",
})


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


def _pg_connect(config):
    """RdbmsConfig로부터 psycopg2 연결 생성"""
    import psycopg2
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database_name or "postgres",
        user=config.username or "sdl_user",
        password=config.password or "sdl_password_2025",
        connect_timeout=config.connection_timeout or 5,
    )


def _fmt_bytes(b):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


# ──────────────────────────────────────────────
# STR-015: GET /api/storage/rdbms  — 인스턴스 목록
# ──────────────────────────────────────────────
@rdbms_bp.route("", methods=["GET"])
def list_instances():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        total = db.query(func.count(RdbmsConfig.id)).scalar()
        rows = (
            db.query(RdbmsConfig)
            .order_by(RdbmsConfig.id)
            .offset((page - 1) * size)
            .limit(size)
            .all()
        )
        return _ok(
            [r.to_dict() for r in rows],
            {"page": page, "size": size, "total": total},
        )
    finally:
        db.close()


# ──────────────────────────────────────────────
# GET /api/storage/rdbms/<id>  — 상세 조회
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>", methods=["GET"])
def get_instance(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(row.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-016: POST /api/storage/rdbms  — 인스턴스 등록
# ──────────────────────────────────────────────
@rdbms_bp.route("", methods=["POST"])
@audit_route("storage", "storage.rdbms.config.create", target_type="rdbms_config",
             detail_keys=["name", "dbType", "host", "port", "database", "schemaName"])
def create_instance():
    db = _db()
    try:
        body = request.get_json(force=True)
        if not body.get("name") or not body.get("host"):
            return _err("name과 host는 필수 항목입니다.", "VALIDATION")

        row = RdbmsConfig(
            name=body["name"],
            db_type=body.get("db_type", "PostgreSQL"),
            host=body["host"],
            port=body.get("port", 5432),
            database_name=body.get("database_name", ""),
            schema_name=body.get("schema_name", "public"),
            username=body.get("username", ""),
            password=body.get("password", ""),
            charset=body.get("charset", "UTF-8"),
            max_connections=body.get("max_connections", 100),
            connection_timeout=body.get("connection_timeout", 30),
            pool_size=body.get("pool_size", 10),
            status="disconnected",
            description=body.get("description", ""),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return _ok(row.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# PUT /api/storage/rdbms/<id>  — 인스턴스 수정
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>", methods=["PUT"])
@audit_route("storage", "storage.rdbms.config.update", target_type="rdbms_config",
             target_name_kwarg="rdbms_id",
             detail_keys=["name", "dbType", "host", "port", "database", "schemaName"])
def update_instance(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        for field in [
            "name", "db_type", "host", "port", "database_name", "schema_name",
            "username", "password", "charset", "max_connections",
            "connection_timeout", "pool_size", "description",
        ]:
            if field in body:
                val = body[field]
                if field == "password" and val == "":
                    continue  # 빈 패스워드는 기존 값 유지
                setattr(row, field, val)

        row.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(row)
        return _ok(row.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DELETE /api/storage/rdbms/<id>  — 인스턴스 삭제
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>", methods=["DELETE"])
@audit_route("storage", "storage.rdbms.config.delete", target_type="rdbms_config",
             target_name_kwarg="rdbms_id")
def delete_instance(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(row)
        db.commit()
        return _ok({"deleted": rdbms_id})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-017: POST /api/storage/rdbms/<id>/test  — 연결 테스트
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/test", methods=["POST"])
@audit_route("storage", "storage.rdbms.config.test", target_type="rdbms_config",
             target_name_kwarg="rdbms_id")
def test_connection(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connected = False
        message = ""
        latency_ms = 0
        version = ""

        try:
            import time
            t0 = time.time()
            conn = _pg_connect(row)
            latency_ms = round((time.time() - t0) * 1000, 1)
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            cur.close()
            conn.close()
            connected = True
            message = "연결 성공"
        except Exception as conn_err:
            message = f"연결 실패: {conn_err}"

        if connected:
            row.status = "connected"
            row.last_connected_at = datetime.utcnow()
        else:
            row.status = "error"

        db.commit()
        db.refresh(row)
        return _ok({
            "connected": connected,
            "message": message,
            "latency_ms": latency_ms,
            "version": version,
            "status": row.status,
        })
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# GET /api/storage/rdbms/<id>/performance  — 성능 통계
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/performance", methods=["GET"])
def get_performance(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        perf = {
            "active_connections": 0,
            "idle_connections": 0,
            "max_connections": row.max_connections,
            "slow_queries_24h": 0,
            "avg_response_ms": 0,
            "cache_hit_rate": 0,
            "disk_io_mbps": 0,
            "tps": 0,
            "lock_waits": 0,
            "db_size_bytes": 0,
            "db_size_display": "0 B",
            "uptime": "",
        }

        try:
            conn = _pg_connect(row)
            cur = conn.cursor()

            # Active / idle connections
            cur.execute("""
                SELECT state, count(*)
                FROM pg_stat_activity
                WHERE datname = current_database()
                GROUP BY state;
            """)
            for state, cnt in cur.fetchall():
                if state == "active":
                    perf["active_connections"] = cnt
                elif state == "idle":
                    perf["idle_connections"] = cnt

            # Max connections setting
            cur.execute("SHOW max_connections;")
            perf["max_connections"] = int(cur.fetchone()[0])

            # DB size
            cur.execute("SELECT pg_database_size(current_database());")
            sz = cur.fetchone()[0]
            perf["db_size_bytes"] = sz
            perf["db_size_display"] = _fmt_bytes(sz)

            # Cache hit rate
            cur.execute("""
                SELECT
                    CASE WHEN blks_hit + blks_read = 0 THEN 0
                    ELSE round(100.0 * blks_hit / (blks_hit + blks_read), 1) END
                FROM pg_stat_database
                WHERE datname = current_database();
            """)
            r = cur.fetchone()
            if r:
                perf["cache_hit_rate"] = float(r[0])

            # TPS (commits + rollbacks)
            cur.execute("""
                SELECT xact_commit + xact_rollback
                FROM pg_stat_database
                WHERE datname = current_database();
            """)
            r = cur.fetchone()
            if r:
                perf["tps"] = r[0]

            # Lock waits
            cur.execute("""
                SELECT count(*)
                FROM pg_stat_activity
                WHERE wait_event_type = 'Lock' AND datname = current_database();
            """)
            perf["lock_waits"] = cur.fetchone()[0]

            # Uptime
            cur.execute("SELECT current_timestamp - pg_postmaster_start_time();")
            interval = cur.fetchone()[0]
            days = interval.days
            hours = interval.seconds // 3600
            perf["uptime"] = f"{days}일 {hours}시간"

            # Avg query time from pg_stat_statements (if available)
            try:
                cur.execute("""
                    SELECT round(avg(mean_exec_time)::numeric, 2)
                    FROM pg_stat_statements
                    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database());
                """)
                r = cur.fetchone()
                if r and r[0]:
                    perf["avg_response_ms"] = float(r[0])
            except Exception:
                conn.rollback()

            # Slow queries count (>1s) from pg_stat_statements
            try:
                cur.execute("""
                    SELECT count(*)
                    FROM pg_stat_statements
                    WHERE mean_exec_time > 1000
                    AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database());
                """)
                r = cur.fetchone()
                if r:
                    perf["slow_queries_24h"] = r[0]
            except Exception:
                conn.rollback()

            cur.close()
            conn.close()
        except Exception:
            pass

        perf["snapshot_at"] = datetime.utcnow().isoformat()
        return _ok(perf)
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-018: GET /api/storage/rdbms/<id>/tables  — 테이블 목록
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/tables", methods=["GET"])
def list_tables(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        tables = []
        try:
            conn = _pg_connect(row)
            cur = conn.cursor()
            schema = row.schema_name or "public"
            cur.execute("""
                SELECT
                    c.relname AS table_name,
                    obj_description(c.oid, 'pg_class') AS description,
                    c.reltuples::bigint AS row_estimate,
                    pg_total_relation_size(c.oid) AS total_bytes,
                    (SELECT count(*) FROM pg_indexes WHERE tablename = c.relname AND schemaname = %s) AS index_count,
                    COALESCE(
                        (SELECT max(last_analyze) FROM pg_stat_user_tables WHERE relname = c.relname AND schemaname = %s),
                        (SELECT max(last_autoanalyze) FROM pg_stat_user_tables WHERE relname = c.relname AND schemaname = %s)
                    ) AS last_updated
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind = 'r'
                ORDER BY total_bytes DESC;
            """, (schema, schema, schema, schema))

            for tbl_name, desc, rows_est, total_bytes, idx_cnt, last_upd in cur.fetchall():
                tables.append({
                    "table_name": tbl_name,
                    "description": desc or "",
                    "row_count": max(rows_est, 0),
                    "size_bytes": total_bytes,
                    "size_display": _fmt_bytes(total_bytes),
                    "index_count": idx_cnt,
                    "last_updated": last_upd.isoformat() if last_upd else None,
                })

            cur.close()
            conn.close()
        except Exception:
            pass

        return _ok(tables)
    finally:
        db.close()


# ──────────────────────────────────────────────
# GET /api/storage/rdbms/<id>/tables/<name>/schema  — 테이블 스키마
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/tables/<table_name>/schema", methods=["GET"])
def get_table_schema(rdbms_id, table_name):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        columns = []
        try:
            conn = _pg_connect(row)
            cur = conn.cursor()
            schema = row.schema_name or "public"
            cur.execute("""
                SELECT
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (schema, table_name))

            for col_name, data_type, max_len, nullable, default in cur.fetchall():
                col_type = data_type.upper()
                if max_len:
                    col_type += f"({max_len})"
                columns.append({
                    "column_name": col_name,
                    "data_type": col_type,
                    "nullable": nullable == "YES",
                    "default": default,
                })

            cur.close()
            conn.close()
        except Exception:
            pass

        if not columns:
            return _err(f"테이블 '{table_name}'을 찾을 수 없습니다.", "NOT_FOUND", 404)

        return _ok({"table_name": table_name, "columns": columns})
    finally:
        db.close()


# ──────────────────────────────────────────────
# GET /api/storage/rdbms/<id>/tables/<name>/usage  — 삭제 영향 분석
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/tables/<table_name>/usage", methods=["GET"])
def get_table_usage(rdbms_id, table_name):
    """테이블 삭제 전 영향 범위 조회.

    응답 필드:
      is_system_table : SDL 코어 테이블 여부 (True 면 삭제 차단 대상)
      row_count, size_bytes, size_display
      in_catalog       : data_catalog 등록 항목 (id, name)
      used_in_pipelines: pipeline_step.config 의 tableName 매칭 (id, name, step_order)
      fk_referenced_by : 다른 테이블이 이 테이블을 FK 로 참조 중 (참조 테이블 목록)
      pipeline_running : sink 로 사용 중인 파이프라인 중 current_run_id != '' 인 것
    """
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        is_system = table_name.lower() in SYSTEM_PROTECTED_TABLES

        # 1) 테이블 통계 (RDBMS 인스턴스의 PG 에서)
        row_count = 0
        size_bytes = 0
        fk_referenced_by = []
        try:
            conn = _pg_connect(row)
            cur = conn.cursor()
            schema = row.schema_name or "public"
            cur.execute("""
                SELECT
                    c.reltuples::bigint AS row_estimate,
                    pg_total_relation_size(c.oid) AS total_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'r';
            """, (schema, table_name))
            r = cur.fetchone()
            if r:
                row_count = max(r[0] or 0, 0)
                size_bytes = r[1] or 0

            # FK 참조 — 이 테이블을 참조하는 다른 테이블
            cur.execute("""
                SELECT DISTINCT n2.nspname || '.' || c2.relname AS referencing
                FROM pg_constraint con
                JOIN pg_class c1 ON c1.oid = con.confrelid
                JOIN pg_namespace n1 ON n1.oid = c1.relnamespace
                JOIN pg_class c2 ON c2.oid = con.conrelid
                JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                WHERE con.contype = 'f'
                  AND n1.nspname = %s AND c1.relname = %s;
            """, (schema, table_name))
            fk_referenced_by = [r[0] for r in cur.fetchall()]

            cur.close()
            conn.close()
        except Exception as e:
            logger.warning("usage stat fetch error: %s", e)

        # 2) 카탈로그 등록 여부 — access_url 의 끝이 /tableName 인 행
        in_catalog = []
        try:
            res = db.execute(_sql_text("""
                SELECT id, name FROM data_catalog
                WHERE access_url LIKE :pat
                  AND sink_type IN ('internal_rdbms_sink', 'external_rdbms_sink')
            """), {"pat": f"%/{table_name}"})
            in_catalog = [{"id": r[0], "name": r[1]} for r in res.fetchall()]
        except Exception as e:
            logger.warning("catalog lookup error: %s", e)

        # 3) 파이프라인 sink 등록 여부 — pipeline_step.config->>'tableName'
        used_in_pipelines = []
        pipeline_running = []
        try:
            res = db.execute(_sql_text("""
                SELECT ps.pipeline_id, p.name, ps.step_order, p.current_run_id
                FROM pipeline_step ps
                JOIN pipeline p ON p.id = ps.pipeline_id
                WHERE ps.module_type IN ('internal_rdbms_sink', 'external_rdbms_sink')
                  AND (ps.config->>'tableName') = :tn
            """), {"tn": table_name})
            for pid, pname, sord, run_id in res.fetchall():
                entry = {"id": pid, "name": pname, "step_order": sord}
                used_in_pipelines.append(entry)
                if run_id and str(run_id).strip():
                    pipeline_running.append(entry)
        except Exception as e:
            logger.warning("pipeline lookup error: %s", e)

        return _ok({
            "table_name": table_name,
            "is_system_table": is_system,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "size_display": _fmt_bytes(size_bytes),
            "in_catalog": in_catalog,
            "used_in_pipelines": used_in_pipelines,
            "pipeline_running": pipeline_running,
            "fk_referenced_by": fk_referenced_by,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# DELETE /api/storage/rdbms/<id>/tables/<name>  — 테이블 삭제/비우기
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/tables/<table_name>", methods=["DELETE"])
def delete_table(rdbms_id, table_name):
    """테이블 DROP 또는 TRUNCATE.

    Body:
      mode            : "drop" (default) | "truncate"
      cascade         : bool (default False)
      cleanup_catalog : bool (default True) — data_catalog 행 함께 삭제
      confirm         : 사용자가 입력한 테이블명 (정확히 일치해야 진행)
    """
    body = request.get_json(silent=True) or {}
    mode = (body.get("mode") or "drop").lower()
    cascade = bool(body.get("cascade", False))
    cleanup_catalog = bool(body.get("cleanup_catalog", True))
    confirm = (body.get("confirm") or "").strip()

    if mode not in ("drop", "truncate"):
        return _err("mode 는 'drop' 또는 'truncate' 여야 합니다.", "VALIDATION")
    if confirm != table_name:
        return _err("확인용 테이블명이 일치하지 않습니다.", "CONFIRM_MISMATCH")

    # 시스템 테이블 보호
    if table_name.lower() in SYSTEM_PROTECTED_TABLES:
        return _err(
            f"'{table_name}' 은 SDL 시스템 테이블이라 삭제할 수 없습니다.",
            "SYSTEM_PROTECTED", 403,
        )

    # information_schema / pg_catalog 등 시스템 schema 차단 (안전망)
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)
        schema = row.schema_name or "public"
        if schema.lower() in ("information_schema", "pg_catalog", "pg_toast"):
            return _err("시스템 schema 의 테이블은 삭제할 수 없습니다.", "SYSTEM_PROTECTED", 403)

        # 진행 중 파이프라인 sink 로 등록되어 있으면 거부
        running_pipes = []
        try:
            res = db.execute(_sql_text("""
                SELECT p.id, p.name FROM pipeline p
                JOIN pipeline_step ps ON ps.pipeline_id = p.id
                WHERE ps.module_type IN ('internal_rdbms_sink', 'external_rdbms_sink')
                  AND (ps.config->>'tableName') = :tn
                  AND COALESCE(p.current_run_id,'') <> ''
            """), {"tn": table_name})
            running_pipes = [{"id": r[0], "name": r[1]} for r in res.fetchall()]
        except Exception:
            pass
        if running_pipes:
            return _err(
                "이 테이블을 sink 로 사용 중인 파이프라인이 실행 중입니다. 정지 후 다시 시도하세요.",
                "PIPELINE_RUNNING", 409,
            )

        # 실제 DROP/TRUNCATE — psycopg2 sql.Identifier 로 안전 quoting
        import psycopg2
        from psycopg2 import sql as _sql
        conn = _pg_connect(row)
        try:
            conn.autocommit = False
            cur = conn.cursor()
            full = _sql.Identifier(schema, table_name)
            if mode == "drop":
                stmt = _sql.SQL("DROP TABLE IF EXISTS {}{}").format(
                    full,
                    _sql.SQL(" CASCADE") if cascade else _sql.SQL(""),
                )
            else:  # truncate
                stmt = _sql.SQL("TRUNCATE TABLE {}{}").format(
                    full,
                    _sql.SQL(" CASCADE") if cascade else _sql.SQL(""),
                )
            cur.execute(stmt)
            conn.commit()
            cur.close()
        except psycopg2.Error as e:
            conn.rollback()
            return _err(f"테이블 {mode} 실패: {e}", "SQL_ERROR", 500)
        finally:
            conn.close()

        # 카탈로그 정리
        catalog_cleaned = 0
        if cleanup_catalog and mode == "drop":
            try:
                res = db.execute(_sql_text("""
                    DELETE FROM data_catalog
                    WHERE access_url LIKE :pat
                      AND sink_type IN ('internal_rdbms_sink', 'external_rdbms_sink')
                """), {"pat": f"%/{table_name}"})
                catalog_cleaned = res.rowcount or 0
                db.commit()
            except Exception as e:
                db.rollback()
                logger.warning("catalog cleanup error: %s", e)

        # 감사 로그
        try:
            from backend.services.audit_logger import log_audit
            from flask import g
            uname = None
            if getattr(g, "api_key_authenticated", False):
                uname = "api-key"
            log_audit(
                action_type="storage",
                action=f"storage.rdbms.table.{mode}",
                target_type="rdbms_table",
                target_name=f"{schema}.{table_name}",
                detail={
                    "rdbms_id": rdbms_id,
                    "mode": mode,
                    "cascade": cascade,
                    "cleanup_catalog": cleanup_catalog,
                    "catalog_cleaned": catalog_cleaned,
                },
                username=uname,
            )
        except Exception:
            logger.warning("audit log write error", exc_info=True)

        return _ok({
            "table_name": table_name,
            "mode": mode,
            "cascade": cascade,
            "cleanup_catalog": cleanup_catalog,
            "catalog_cleaned": catalog_cleaned,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-019: GET /api/storage/rdbms/<id>/slow-queries  — 슬로우 쿼리
# ──────────────────────────────────────────────
@rdbms_bp.route("/<int:rdbms_id>/slow-queries", methods=["GET"])
def get_slow_queries(rdbms_id):
    db = _db()
    try:
        row = db.query(RdbmsConfig).get(rdbms_id)
        if not row:
            return _err("RDBMS 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        queries = []

        try:
            conn = _pg_connect(row)
            cur = conn.cursor()

            # pg_stat_statements에서 슬로우 쿼리 조회 시도
            try:
                cur.execute("""
                    SELECT
                        query,
                        round(mean_exec_time::numeric, 2) AS avg_time_ms,
                        calls,
                        rows,
                        round(total_exec_time::numeric, 2) AS total_time_ms
                    FROM pg_stat_statements
                    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                    ORDER BY mean_exec_time DESC
                    LIMIT %s OFFSET %s;
                """, (size, (page - 1) * size))

                for query_text, avg_ms, calls, rows_affected, total_ms in cur.fetchall():
                    queries.append({
                        "query": query_text[:200],
                        "avg_time_ms": float(avg_ms),
                        "calls": calls,
                        "rows": rows_affected,
                        "total_time_ms": float(total_ms),
                    })
            except Exception:
                conn.rollback()
                # pg_stat_statements 없으면 pg_stat_activity에서 현재 실행중인 쿼리 조회
                cur.execute("""
                    SELECT
                        query,
                        EXTRACT(EPOCH FROM (now() - query_start)) * 1000 AS duration_ms,
                        usename,
                        state
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                        AND state != 'idle'
                        AND query NOT LIKE '%%pg_stat%%'
                    ORDER BY query_start
                    LIMIT %s OFFSET %s;
                """, (size, (page - 1) * size))

                for query_text, dur_ms, user, state in cur.fetchall():
                    queries.append({
                        "query": query_text[:200],
                        "avg_time_ms": round(float(dur_ms), 2) if dur_ms else 0,
                        "calls": 1,
                        "rows": 0,
                        "total_time_ms": round(float(dur_ms), 2) if dur_ms else 0,
                        "user": user,
                        "state": state,
                    })

            cur.close()
            conn.close()
        except Exception:
            pass

        return _ok(queries, {"page": page, "size": size})
    finally:
        db.close()
