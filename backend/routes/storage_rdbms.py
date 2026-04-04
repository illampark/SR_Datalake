from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.storage import RdbmsConfig

rdbms_bp = Blueprint("storage_rdbms", __name__, url_prefix="/api/storage/rdbms")


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
        size = request.args.get("size", 20, type=int)
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
        size = request.args.get("size", 20, type=int)
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
