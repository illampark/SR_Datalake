import logging
import random
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
from sqlalchemy import func, text as _sql_text
from backend.database import SessionLocal
from backend.models.storage import TsdbConfig, DownsamplingPolicy
from backend.services.system_settings import get_default_page_size

logger = logging.getLogger(__name__)

tsdb_bp = Blueprint("storage_tsdb", __name__, url_prefix="/api/storage/tsdb")


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _pg_connect(config):
    """TsdbConfig로부터 psycopg2 연결 생성"""
    import psycopg2
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database_name or "postgres",
        user=config.username or "sdl_user",
        password=config.password or "",
        connect_timeout=5,
    )


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
        size = request.args.get("size", get_default_page_size(), type=int)
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
            username=body.get("username", ""),
            password=body.get("password", ""),
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
            "database_name", "username", "password", "api_token",
            "tls_enabled", "retention_days", "description",
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
            import time
            t0 = time.time()
            test_conn = _pg_connect(row)
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
        conn = _pg_connect(config)
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
            import time
            t0 = time.time()
            conn = _pg_connect(row)
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


# ──────────────────────────────────────────────
# GET /api/storage/tsdb/<id>/data-summary  — 적재 데이터 요약
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/data-summary", methods=["GET"])
def get_data_summary(tsdb_id):
    """time_series_data 의 (pipeline_id × measurement) 별 행수/시간 범위.

    pipeline 이 카탈로그/sink 에서 제거된 후 남은 잔존 데이터(orphan)도
    그대로 보여준다. UI 에서 행 별 [삭제] 버튼으로 정리 가능.
    """
    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 현재 sink 로 활성 사용 중인 (pipeline_id, measurement) 조합 — 잔존 식별용
        active_sinks = set()
        try:
            res = db.execute(_sql_text("""
                SELECT DISTINCT ps.pipeline_id, ps.config->>'measurement' AS meas
                FROM pipeline_step ps
                WHERE ps.module_type = 'internal_tsdb_sink' AND ps.enabled = TRUE
            """))
            for pid, meas in res.fetchall():
                active_sinks.add((pid, meas or ""))
        except Exception as e:
            logger.warning("active sinks lookup failed: %s", e)

        # 요약 집계
        summary = []
        try:
            res = db.execute(_sql_text("""
                SELECT pipeline_id, measurement,
                       COUNT(*) AS rows,
                       MIN(timestamp) AS first_ts,
                       MAX(timestamp) AS last_ts
                FROM time_series_data
                WHERE tsdb_id = :tid
                GROUP BY pipeline_id, measurement
                ORDER BY rows DESC
            """), {"tid": tsdb_id})
            for pid, meas, n, mn, mx in res.fetchall():
                # pipeline 이름 조회
                pname = ""
                try:
                    pname = db.execute(_sql_text(
                        "SELECT name FROM pipeline WHERE id=:p"
                    ), {"p": pid}).scalar() or ""
                except Exception:
                    pass
                is_orphan = (pid, meas or "") not in active_sinks
                summary.append({
                    "pipeline_id": pid,
                    "pipeline_name": pname,
                    "measurement": meas or "",
                    "rows": n or 0,
                    "first_ts": mn.isoformat() if mn else None,
                    "last_ts": mx.isoformat() if mx else None,
                    "is_orphan": is_orphan,
                })
        except Exception as e:
            logger.warning("data summary failed: %s", e)

        return _ok({"tsdb_id": tsdb_id, "items": summary})
    finally:
        db.close()


# ──────────────────────────────────────────────
# DELETE /api/storage/tsdb/<id>/data  — 행 단위 삭제
# ──────────────────────────────────────────────
@tsdb_bp.route("/<int:tsdb_id>/data", methods=["DELETE"])
def delete_data(tsdb_id):
    """기준 (pipeline_id, measurement, before_ts) 으로 time_series_data 행 삭제.

    Body:
      pipeline_id  : int  (선택)
      measurement  : str  (선택)
      before_ts    : ISO  (선택, 해당 시각 이전 행만)
      cleanup_catalog : bool (default True) — 카탈로그 항목도 함께 정리
      confirm      : str  ("DELETE-{rows}" 형식, 미리 보여준 row 수와 일치해야)
    적어도 pipeline_id 또는 measurement 또는 before_ts 중 하나가 있어야 한다
    (전체 TRUNCATE 방지).
    """
    body = request.get_json(silent=True) or {}
    pid = body.get("pipeline_id")
    meas = (body.get("measurement") or "").strip()
    before_ts = (body.get("before_ts") or "").strip()
    cleanup_catalog = bool(body.get("cleanup_catalog", True))
    confirm = (body.get("confirm") or "").strip()

    if pid is None and not meas and not before_ts:
        return _err("pipeline_id, measurement, before_ts 중 하나는 지정해야 합니다.", "VALIDATION")

    db = _db()
    try:
        row = db.query(TsdbConfig).get(tsdb_id)
        if not row:
            return _err("TSDB 인스턴스를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 활성 sink 사용 중이면 거부 (pipeline_id 가 명시되었을 때만 검사)
        if pid is not None:
            running = db.execute(_sql_text("""
                SELECT p.id FROM pipeline p
                JOIN pipeline_step ps ON ps.pipeline_id = p.id
                WHERE p.id = :pid
                  AND ps.module_type = 'internal_tsdb_sink'
                  AND COALESCE(p.current_run_id,'') <> ''
            """), {"pid": pid}).fetchone()
            if running:
                return _err(
                    "지정한 파이프라인이 TSDB sink 로 실행 중입니다. 정지 후 다시 시도하세요.",
                    "PIPELINE_RUNNING", 409,
                )

        # WHERE 절 동적 조립
        clauses = ["tsdb_id = :tid"]
        params = {"tid": tsdb_id}
        if pid is not None:
            clauses.append("pipeline_id = :pid")
            params["pid"] = int(pid)
        if meas:
            clauses.append("measurement = :meas")
            params["meas"] = meas
        if before_ts:
            clauses.append("timestamp < cast(:bts as timestamp)")
            params["bts"] = before_ts
        where_sql = " AND ".join(clauses)

        # 영향받을 행 수 미리 카운트 (confirm 검증용)
        try:
            row_count = db.execute(_sql_text(
                f"SELECT COUNT(*) FROM time_series_data WHERE {where_sql}"
            ), params).scalar() or 0
        except Exception as e:
            return _err(f"행 수 계산 오류: {e}", "SQL_ERROR", 500)

        # confirm 검증
        expected = f"DELETE-{row_count}"
        if confirm != expected:
            return _err(
                f"확인 문자열이 일치하지 않습니다. 예상: {expected}",
                "CONFIRM_MISMATCH",
            )

        # 실제 삭제
        try:
            res = db.execute(_sql_text(
                f"DELETE FROM time_series_data WHERE {where_sql}"
            ), params)
            deleted = res.rowcount or 0
            db.commit()
        except Exception as e:
            db.rollback()
            return _err(f"삭제 실패: {e}", "SQL_ERROR", 500)

        # 카탈로그 정리 (pipeline_id 가 있고 cleanup_catalog=true 일 때)
        catalog_cleaned = 0
        if cleanup_catalog and pid is not None:
            try:
                cat_res = db.execute(_sql_text("""
                    DELETE FROM data_catalog
                    WHERE connector_type = 'pipeline'
                      AND connector_id = :pid
                      AND sink_type = 'internal_tsdb_sink'
                """), {"pid": int(pid)})
                catalog_cleaned = cat_res.rowcount or 0
                db.commit()
            except Exception as e:
                db.rollback()
                logger.warning("catalog cleanup failed: %s", e)

        # 감사 로그
        try:
            from flask import session, g
            uname = session.get("username") or ("api-key" if getattr(g, "api_key_authenticated", False) else "unknown")
            import json as _json
            detail_str = _json.dumps({
                "tsdb_id": tsdb_id,
                "pipeline_id": pid,
                "measurement": meas or None,
                "before_ts": before_ts or None,
                "deleted": deleted,
                "cleanup_catalog": cleanup_catalog,
                "catalog_cleaned": catalog_cleaned,
            }, ensure_ascii=False)
            db.execute(_sql_text("""
                INSERT INTO audit_log (timestamp, username, action_type, action, target_type, target_name, ip_address, result, detail)
                VALUES (NOW(), :u, 'storage.tsdb.data_delete', :a, 'tsdb_data', :tn, :ip, 'success', cast(:detail as json))
            """), {
                "u": uname,
                "a": "TSDB data DELETE",
                "tn": f"tsdb#{tsdb_id} pid={pid} meas={meas}",
                "ip": (request.remote_addr or ""),
                "detail": detail_str,
            })
            db.commit()
        except Exception as e:
            db.rollback()
            logger.warning("audit log failed: %s", e)

        return _ok({
            "deleted": deleted,
            "catalog_cleaned": catalog_cleaned,
        })
    finally:
        db.close()
