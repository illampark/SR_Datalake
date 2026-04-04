import random
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.storage import TsdbConfig, DownsamplingPolicy

tsdb_bp = Blueprint("storage_tsdb", __name__, url_prefix="/api/storage/tsdb")


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


# ──────────────────────────────────────────────
# STR-001: GET /api/storage/tsdb  — 인스턴스 목록
# ──────────────────────────────────────────────
@tsdb_bp.route("", methods=["GET"])
def list_instances():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 20, type=int)
        total = db.query(func.count(TsdbConfig.id)).scalar()
        rows = (
            db.query(TsdbConfig)
            .order_by(TsdbConfig.id)
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
# STR-002: GET /api/storage/tsdb/<id>  — 상세 조회
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>", methods=["GET"])
def get_instance(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(row.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-003: POST /api/storage/tsdb  — 인스턴스 등록
# ──────────────────────────────────────────────
@tsdb_bp.route("", methods=["POST"])
def create_instance():
    db = _db()
    try:
        body = request.get_json(force=True)
        if not body.get("name") or not body.get("host"):
            return _err("name과 host는 필수 항목입니다.", "VALIDATION")

        row = TsdbConfig(
            name=body["name"],
            db_type=body.get("db_type", "PostgreSQL"),
            host=body["host"],
            port=body.get("port", 5432),
            organization=body.get("organization", ""),
            bucket=body.get("bucket", ""),
            database_name=body.get("database_name", ""),
            api_token=body.get("api_token", ""),
            tls_enabled=body.get("tls_enabled", False),
            retention_days=body.get("retention_days", 30),
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
# STR-004: PUT /api/storage/tsdb/<id>  — 설정 수정
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>", methods=["PUT"])
def update_instance(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        for field in [
            "name", "db_type", "host", "port", "organization", "bucket",
            "database_name", "api_token", "tls_enabled", "retention_days", "description",
        ]:
            if field in body:
                setattr(row, field, body[field])

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
# STR-005: DELETE /api/storage/tsdb/<id>  — 삭제
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>", methods=["DELETE"])
def delete_instance(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(row)
        db.commit()
        return _ok({"deleted": tsdb_id})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-006: POST /api/storage/tsdb/<id>/test  — 연결 테스트
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/test", methods=["POST"])
def test_connection(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 실제 연결 테스트: PostgreSQL인 경우 psycopg2로 연결 시도
        connected = False
        message = ""
        latency_ms = 0

        try:
            import psycopg2
            import time
            t0 = time.time()
            test_conn = psycopg2.connect(
                host=row.host,
                port=row.port,
                dbname=row.database_name or "postgres",
                user="sdl_user",
                password="sdl_password_2025",
                connect_timeout=5,
            )
            latency_ms = round((time.time() - t0) * 1000, 1)
            test_conn.close()
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
            "status": row.status,
        })
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-007: GET /api/storage/tsdb/<id>/usage  — 사용량 통계
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/usage", methods=["GET"])
def get_usage(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 실제 DB에서 테이블별 크기 조회 시도
        usage_data = _query_pg_usage(row)

        return _ok(usage_data)
    finally:
        db.close()


def _query_pg_usage(config):
    """PostgreSQL의 실제 DB 사이즈 및 테이블별 사용량 조회"""
    measurements = []
    total_size_bytes = 0
    total_rows = 0

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database_name or "postgres",
            user="sdl_user",
            password="sdl_password_2025",
            connect_timeout=5,
        )
        cur = conn.cursor()

        # DB 전체 크기
        cur.execute("SELECT pg_database_size(current_database());")
        total_size_bytes = cur.fetchone()[0]

        # 테이블별 크기
        cur.execute("""
            SELECT
                relname AS table_name,
                pg_total_relation_size(c.oid) AS total_bytes,
                reltuples::bigint AS row_estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind = 'r'
            ORDER BY total_bytes DESC
            LIMIT 10;
        """)
        for tbl_name, tbl_bytes, tbl_rows in cur.fetchall():
            measurements.append({
                "name": tbl_name,
                "size_bytes": tbl_bytes,
                "size_display": _fmt_bytes(tbl_bytes),
                "row_count": max(tbl_rows, 0),
            })

        # 쓰기 속도 (tps 기반 추정)
        cur.execute("SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = current_database();")
        txn_row = cur.fetchone()
        write_rate = txn_row[0] if txn_row else 0

        cur.close()
        conn.close()
    except Exception:
        # 연결 실패 시 빈 데이터 반환
        pass

    return {
        "total_size_bytes": total_size_bytes,
        "total_size_display": _fmt_bytes(total_size_bytes),
        "total_data_points": total_rows,
        "write_rate": write_rate if 'write_rate' in dir() else 0,
        "measurements": measurements,
        "snapshot_at": datetime.utcnow().isoformat(),
    }


def _fmt_bytes(b):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


# ──────────────────────────────────────────────
# STR-008: GET /api/storage/tsdb/<id>/downsampling  — 정책 목록
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/downsampling", methods=["GET"])
def list_downsampling(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        policies = (
            db.query(DownsamplingPolicy)
            .filter(DownsamplingPolicy.tsdb_id == tsdb_id)
            .order_by(DownsamplingPolicy.id)
            .all()
        )
        return _ok([p.to_dict() for p in policies])
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-009: POST /api/storage/tsdb/<id>/downsampling  — 정책 추가
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/downsampling", methods=["POST"])
def create_downsampling(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        if not body.get("policy_name"):
            return _err("policy_name은 필수 항목입니다.", "VALIDATION")

        policy = DownsamplingPolicy(
            tsdb_id=tsdb_id,
            policy_name=body["policy_name"],
            source_retention=body.get("source_retention", "Raw (1초)"),
            target_retention=body.get("target_retention", "1년"),
            aggregate_functions=body.get("aggregate_functions", ["MEAN"]),
            aggregation_period=body.get("aggregation_period", "1분"),
            target_data=body.get("target_data", ""),
            enabled=body.get("enabled", True),
        )
        db.add(policy)
        db.commit()
        db.refresh(policy)
        return _ok(policy.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-010: DELETE /api/storage/tsdb/<id>/downsampling/<pid>  — 정책 삭제
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/downsampling/<int:policy_id>", methods=["DELETE"])
def delete_downsampling(tsdb_id, policy_id):
    db = _db()
    try:
        policy = (
            db.query(DownsamplingPolicy)
            .filter(DownsamplingPolicy.id == policy_id, DownsamplingPolicy.tsdb_id == tsdb_id)
            .first()
        )
        if not policy:
            return _err("다운샘플링 정책을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(policy)
        db.commit()
        return _ok({"deleted": policy_id})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 추가: PUT /api/storage/tsdb/<id>/downsampling/<pid>  — 정책 수정
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/downsampling/<int:policy_id>", methods=["PUT"])
def update_downsampling(tsdb_id, policy_id):
    db = _db()
    try:
        policy = (
            db.query(DownsamplingPolicy)
            .filter(DownsamplingPolicy.id == policy_id, DownsamplingPolicy.tsdb_id == tsdb_id)
            .first()
        )
        if not policy:
            return _err("다운샘플링 정책을 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        for field in [
            "policy_name", "source_retention", "target_retention",
            "aggregate_functions", "aggregation_period", "target_data", "enabled",
        ]:
            if field in body:
                setattr(policy, field, body[field])

        policy.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(policy)
        return _ok(policy.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 추가: PATCH /api/storage/tsdb/<id>/downsampling/<pid>/toggle  — 정책 상태 토글
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/downsampling/<int:policy_id>/toggle", methods=["PATCH"])
def toggle_downsampling(tsdb_id, policy_id):
    db = _db()
    try:
        policy = (
            db.query(DownsamplingPolicy)
            .filter(DownsamplingPolicy.id == policy_id, DownsamplingPolicy.tsdb_id == tsdb_id)
            .first()
        )
        if not policy:
            return _err("다운샘플링 정책을 찾을 수 없습니다.", "NOT_FOUND", 404)

        policy.enabled = not policy.enabled
        policy.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(policy)
        return _ok(policy.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 추가: POST /api/storage/tsdb/<id>/query  — 쿼리 실행
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/query", methods=["POST"])
def execute_query(tsdb_id):
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        query_text = body.get("query", "").strip()
        if not query_text:
            return _err("쿼리를 입력해주세요.", "VALIDATION")

        # 실제 PostgreSQL 쿼리 실행 (SELECT만 허용)
        upper = query_text.upper().strip()
        if not upper.startswith("SELECT"):
            return _err("SELECT 쿼리만 실행 가능합니다.", "VALIDATION")

        try:
            import psycopg2
            import time
            t0 = time.time()
            conn = psycopg2.connect(
                host=row.host,
                port=row.port,
                dbname=row.database_name or "postgres",
                user="sdl_user",
                password="sdl_password_2025",
                connect_timeout=5,
            )
            conn.set_session(readonly=True, autocommit=True)
            cur = conn.cursor()
            cur.execute(query_text)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchmany(1000)
            row_count = cur.rowcount
            elapsed = round((time.time() - t0) * 1000, 1)
            cur.close()
            conn.close()

            result_data = []
            for r in rows:
                result_data.append(dict(zip(columns, [str(v) if v is not None else None for v in r])))

            return _ok({
                "columns": columns,
                "rows": result_data,
                "row_count": row_count,
                "elapsed_ms": elapsed,
            })
        except Exception as qe:
            return _err(f"쿼리 실행 오류: {qe}", "QUERY_ERROR")
    finally:
        db.close()
