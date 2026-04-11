from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.integration import ExternalConnection

integration_bp = Blueprint("integration", __name__, url_prefix="/api/integration")


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
# GET /api/integration — 연결 목록
# ──────────────────────────────────────────────
@integration_bp.route("", methods=["GET"])
def list_connections():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 20, type=int)
        conn_type = request.args.get("type", "").strip()
        search = request.args.get("search", "").strip()

        q = db.query(ExternalConnection)
        if conn_type:
            q = q.filter(ExternalConnection.connection_type == conn_type)
        if search:
            like = f"%{search}%"
            q = q.filter(
                ExternalConnection.name.ilike(like) | ExternalConnection.host.ilike(like)
            )

        total = q.count()
        rows = q.order_by(ExternalConnection.id.desc()).offset((page - 1) * size).limit(size).all()

        # 통계
        active_count = db.query(func.count(ExternalConnection.id)).filter(
            ExternalConnection.enabled == True,
        )
        if conn_type:
            active_count = active_count.filter(ExternalConnection.connection_type == conn_type)
        active_count = active_count.scalar() or 0

        error_count = db.query(func.count(ExternalConnection.id)).filter(
            ExternalConnection.status == "error",
        )
        if conn_type:
            error_count = error_count.filter(ExternalConnection.connection_type == conn_type)
        error_count = error_count.scalar() or 0

        connected_count = db.query(func.count(ExternalConnection.id)).filter(
            ExternalConnection.status == "connected",
        )
        if conn_type:
            connected_count = connected_count.filter(ExternalConnection.connection_type == conn_type)
        connected_count = connected_count.scalar() or 0

        return _ok({
            "items": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
            "activeCount": active_count,
            "errorCount": error_count,
            "connectedCount": connected_count,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# GET /api/integration/<id> — 상세 조회
# ──────────────────────────────────────────────
@integration_bp.route("/<int:conn_id>", methods=["GET"])
def get_connection(conn_id):
    db = _db()
    try:
        row = db.query(ExternalConnection).get(conn_id)
        if not row:
            return _err("연결을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(row.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/integration — 연결 생성
# ──────────────────────────────────────────────
@integration_bp.route("", methods=["POST"])
def create_connection():
    db = _db()
    try:
        body = request.get_json(force=True)
        if not body.get("name"):
            return _err("name은 필수 항목입니다.", "VALIDATION")
        if not body.get("connection_type"):
            return _err("connection_type은 필수 항목입니다.", "VALIDATION")

        valid_types = ["tsdb", "rdbms", "kafka", "file_storage", "mqtt_broker"]
        if body["connection_type"] not in valid_types:
            return _err(f"connection_type은 {valid_types} 중 하나여야 합니다.", "VALIDATION")

        row = ExternalConnection(
            name=body["name"],
            connection_type=body["connection_type"],
            host=body.get("host", ""),
            port=body.get("port", 0),
            database_name=body.get("database_name", ""),
            username=body.get("username", ""),
            password=body.get("password", ""),
            config=body.get("config", {}),
            enabled=body.get("enabled", True),
            description=body.get("description", ""),
            status="unknown",
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
# PUT /api/integration/<id> — 연결 수정
# ──────────────────────────────────────────────
@integration_bp.route("/<int:conn_id>", methods=["PUT"])
def update_connection(conn_id):
    db = _db()
    try:
        row = db.query(ExternalConnection).get(conn_id)
        if not row:
            return _err("연결을 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        for field in [
            "name", "host", "port", "database_name", "username", "password",
            "config", "enabled", "description",
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
# DELETE /api/integration/<id> — 연결 삭제
# ──────────────────────────────────────────────
@integration_bp.route("/<int:conn_id>", methods=["DELETE"])
def delete_connection(conn_id):
    db = _db()
    try:
        row = db.query(ExternalConnection).get(conn_id)
        if not row:
            return _err("연결을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(row)
        db.commit()
        return _ok({"deleted": conn_id})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/integration/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@integration_bp.route("/<int:conn_id>/test", methods=["POST"])
def test_connection(conn_id):
    db = _db()
    try:
        row = db.query(ExternalConnection).get(conn_id)
        if not row:
            return _err("연결을 찾을 수 없습니다.", "NOT_FOUND", 404)

        connected = False
        message = ""
        latency_ms = 0

        import time
        t0 = time.time()

        try:
            if row.connection_type == "tsdb":
                connected, message = _test_tsdb(row)
            elif row.connection_type == "rdbms":
                connected, message = _test_rdbms(row)
            elif row.connection_type == "kafka":
                connected, message = _test_kafka(row)
            elif row.connection_type == "mqtt_broker":
                connected, message = _test_mqtt_broker(row)
            elif row.connection_type == "file_storage":
                connected, message = _test_file_storage(row)
            else:
                message = f"알 수 없는 연결 타입: {row.connection_type}"
        except Exception as conn_err:
            message = f"연결 실패: {conn_err}"

        latency_ms = round((time.time() - t0) * 1000, 1)

        if connected:
            row.status = "connected"
        else:
            row.status = "error"
        row.last_tested_at = datetime.utcnow()

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
# 연결 테스트 헬퍼
# ──────────────────────────────────────────────

def _test_tsdb(row):
    """시계열DB 연결 테스트 (InfluxDB / TimescaleDB)"""
    cfg = row.config or {}
    db_type = cfg.get("db_type", "influxdb")

    if db_type == "timescaledb":
        # TimescaleDB는 PostgreSQL 기반
        return _test_pg(row.host, row.port or 5432, row.database_name, row.username, row.password)

    # InfluxDB: HTTP health check
    import urllib.request
    url = f"http://{row.host}:{row.port or 8086}/health"
    try:
        req = urllib.request.Request(url, method="GET")
        token = cfg.get("token", "")
        if token:
            req.add_header("Authorization", f"Token {token}")
        resp = urllib.request.urlopen(req, timeout=5)
        if resp.status == 200:
            return True, "InfluxDB 연결 성공"
        return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, f"InfluxDB 연결 실패: {e}"


def _test_rdbms(row):
    """관계형DB 연결 테스트 — db_type에 따라 분기"""
    cfg = row.config or {}
    db_type = cfg.get("db_type", "").lower()

    if "mysql" in db_type or "maria" in db_type:
        try:
            import pymysql
            conn = pymysql.connect(
                host=row.host, port=row.port or 3306,
                user=row.username, password=row.password,
                database=row.database_name or "",
                connect_timeout=5,
            )
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            ver = cur.fetchone()[0]
            conn.close()
            return True, f"MariaDB/MySQL 연결 성공 (v{ver})"
        except Exception as e:
            return False, f"MariaDB/MySQL 연결 실패: {e}"

    elif "oracle" in db_type:
        try:
            import oracledb
            dsn = f"{row.host}:{row.port or 1521}/{row.database_name}"
            conn = oracledb.connect(user=row.username, password=row.password, dsn=dsn)
            conn.close()
            return True, f"Oracle 연결 성공 ({dsn})"
        except ImportError:
            return False, "oracledb 패키지가 설치되지 않았습니다"
        except Exception as e:
            return False, f"Oracle 연결 실패: {e}"

    elif "mssql" in db_type or "sqlserver" in db_type:
        try:
            import pymssql
            conn = pymssql.connect(
                server=row.host, port=row.port or 1433,
                user=row.username, password=row.password,
                database=row.database_name or "master",
            )
            conn.close()
            return True, f"MSSQL 연결 성공 ({row.host}:{row.port})"
        except ImportError:
            return False, "pymssql 패키지가 설치되지 않았습니다"
        except Exception as e:
            return False, f"MSSQL 연결 실패: {e}"

    else:
        return _test_pg(row.host, row.port or 5432, row.database_name, row.username, row.password)


def _test_mqtt_broker(row):
    """외부 MQTT 브로커 연결 테스트"""
    import socket
    host = row.host or "localhost"
    port = row.port or 1883
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.close()
        return True, f"MQTT 브로커 연결 성공 ({host}:{port})"
    except Exception as e:
        return False, f"MQTT 연결 실패: {e}"


def _test_kafka(row):
    """Kafka 연결 테스트 (socket 레벨)"""
    import socket
    cfg = row.config or {}
    bootstrap = cfg.get("bootstrap_servers", "") or f"{row.host}:{row.port or 9092}"
    host_port = bootstrap.split(",")[0].strip()
    parts = host_port.rsplit(":", 1)
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 9092
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.close()
        return True, f"Kafka 브로커 연결 성공 ({host}:{port})"
    except Exception as e:
        return False, f"Kafka 연결 실패: {e}"


def _test_file_storage(row):
    """파일 스토리지 연결 테스트 (S3/FTP/NFS)"""
    cfg = row.config or {}
    storage_type = cfg.get("storage_type", "s3")

    if storage_type == "s3":
        try:
            from minio import Minio
            client = Minio(
                f"{row.host}:{row.port or 9000}",
                access_key=cfg.get("access_key", ""),
                secret_key=cfg.get("secret_key", ""),
                secure=cfg.get("secure", False),
            )
            client.list_buckets()
            return True, "S3 스토리지 연결 성공"
        except Exception as e:
            return False, f"S3 연결 실패: {e}"
    elif storage_type == "ftp":
        import socket
        try:
            s = socket.create_connection((row.host, row.port or 21), timeout=5)
            s.close()
            return True, f"FTP 연결 성공 ({row.host}:{row.port or 21})"
        except Exception as e:
            return False, f"FTP 연결 실패: {e}"
    else:
        # NFS: 경로 존재 확인
        import os
        base = cfg.get("base_path", "")
        if base and os.path.isdir(base):
            return True, f"NFS 경로 존재 확인: {base}"
        return False, f"NFS 경로를 찾을 수 없습니다: {base}"


def _test_pg(host, port, dbname, user, password):
    """PostgreSQL 연결 테스트"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host, port=port,
            dbname=dbname or "postgres",
            user=user, password=password,
            connect_timeout=5,
        )
        conn.close()
        return True, "DB 연결 성공"
    except Exception as e:
        return False, f"DB 연결 실패: {e}"
