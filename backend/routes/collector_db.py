from datetime import datetime
from copy import deepcopy
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from sqlalchemy.orm.attributes import flag_modified
from backend.database import SessionLocal
from backend.models.collector import DbConnector, DbTag
from backend.services import benthos_manager as bm
from backend.services import mqtt_manager
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size

db_bp = Blueprint("collector_db", __name__, url_prefix="/api/connectors/db")


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
# DB-001: GET /api/connectors/db — 목록 조회
# ──────────────────────────────────────────────
@db_bp.route("", methods=["GET"])
def list_connectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        status_filter = request.args.get("status", "")
        dbtype_filter = request.args.get("dbType", "")
        search = (request.args.get("q") or "").strip()

        q = db.query(DbConnector)
        if status_filter:
            q = q.filter(DbConnector.status == status_filter)
        if dbtype_filter:
            q = q.filter(DbConnector.db_type == dbtype_filter)
        if search:
            like = f"%{search}%"
            q = q.filter(or_(DbConnector.name.ilike(like),
                             DbConnector.description.ilike(like),
                             DbConnector.host.ilike(like)))

        total = q.count()
        rows = q.order_by(DbConnector.id).offset((page - 1) * size).limit(size).all()

        # Sync with Benthos runtime status
        streams = bm.list_streams()
        items = []
        for r in rows:
            d = r.to_dict()
            sid = r.benthos_stream_id()
            if sid in streams:
                d["benthos_active"] = streams[sid].get("active", False)
                d["benthos_uptime"] = streams[sid].get("uptime_str", "")
            else:
                d["benthos_active"] = False
                d["benthos_uptime"] = ""
            items.append(d)

        return _ok(items, {"page": page, "size": size, "total": total})
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-002: GET /api/connectors/db/<id> — 상세 조회
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>", methods=["GET"])
def get_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        stream = bm.get_db_stream_status(c)
        d["benthos_stream"] = stream
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-003: POST /api/connectors/db — 등록
# ──────────────────────────────────────────────
@db_bp.route("", methods=["POST"])
@audit_route("connector", "connector.db.create", target_type="db_connector",
             detail_keys=["name", "dbType", "host", "port", "database", "schemaName", "collectMode"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        if db.query(DbConnector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        db_type = body.get("dbType", "mysql")
        host = body.get("host", "localhost")
        port_defaults = {"oracle": 1521, "mssql": 1433, "postgresql": 5432, "mysql": 3306}
        port = int(body.get("port", port_defaults.get(db_type, 3306)))

        config = {
            "tables": _normalize_tables_input(body.get("tables", [])),
            "trackingColumn": body.get("trackingColumn", "id"),
            "pollingInterval": int(body.get("pollingInterval", 60)),
            "batchSize": int(body.get("batchSize", 1000)),
            "poolSize": int(body.get("poolSize", 5)),
        }

        c = DbConnector(
            name=name,
            description=body.get("description", ""),
            db_type=db_type,
            host=host,
            port=port,
            database=body.get("database", ""),
            schema_name=body.get("schemaName", ""),
            username=body.get("username", ""),
            password=body.get("password", ""),
            collect_mode=body.get("collectMode", "polling"),
            config=config,
        )
        db.add(c)
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-004: PUT /api/connectors/db/<id> — 수정
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>", methods=["PUT"])
@audit_route("connector", "connector.db.update", target_type="db_connector",
             target_name_kwarg="cid",
             detail_keys=["name", "dbType", "host", "port", "database", "schemaName", "collectMode", "enabled"])
def update_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)

        for field in ["description", "host", "database", "schema_name", "username", "password", "collect_mode"]:
            camel = field.replace("_", " ").title().replace(" ", "")
            camel = camel[0].lower() + camel[1:]  # camelCase
            # Map: schemaName, collectMode
            key_map = {"schemaName": "schema_name", "collectMode": "collect_mode"}
            json_key = next((k for k, v in key_map.items() if v == field), camel)
            if json_key in body:
                val = body[json_key]
                if field == "password" and val == "":
                    continue  # 빈 패스워드는 기존 값 유지
                setattr(c, field, val)
        if "dbType" in body:
            c.db_type = body["dbType"]
        if "port" in body:
            c.port = int(body["port"])

        # Merge config fields (deepcopy to ensure SQLAlchemy detects mutation)
        cfg = deepcopy(c.config or {})
        for key in ["trackingColumn", "pollingInterval", "batchSize", "poolSize"]:
            if key in body:
                cfg[key] = body[key]
        if "tables" in body:
            cfg["tables"] = _normalize_tables_input(body["tables"])
        c.config = cfg
        flag_modified(c, "config")
        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "db", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "db", cid, body["name"])

        db.commit()
        db.refresh(c)

        # If running, update Benthos stream too
        if c.status == "running":
            callback_url = _callback_url()
            stream_config = bm.build_db_stream_config(c, callback_url)
            bm.update_stream(c.benthos_stream_id(), stream_config)

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-005: DELETE /api/connectors/db/<id> — 삭제
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>", methods=["DELETE"])
@audit_route("connector", "connector.db.delete", target_type="db_connector",
             target_name_kwarg="cid")
def delete_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            bm.stop_db_stream(c)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "db", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-006: POST /api/connectors/db/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/start", methods=["POST"])
@audit_route("connector", "connector.db.start", target_type="db_connector",
             target_name_kwarg="cid")
def start_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_db_stream(c, callback_url)
        if not ok:
            c.status = "error"
            c.last_error = err or "스트림 생성 실패"
            db.commit()
            return _err(f"스트림 시작 실패: {err}", "STREAM_ERROR", 500)

        c.status = "running"
        c.last_error = ""
        c.row_count = 0
        c.error_count = 0
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-007: POST /api/connectors/db/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/stop", methods=["POST"])
@audit_route("connector", "connector.db.stop", target_type="db_connector",
             target_name_kwarg="cid")
def stop_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_db_stream(c)
        c.status = "stopped"
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-008: POST /api/connectors/db/<id>/restart — 재시작
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/restart", methods=["POST"])
@audit_route("connector", "connector.db.restart", target_type="db_connector",
             target_name_kwarg="cid")
def restart_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_db_stream(c)

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_db_stream(c, callback_url)
        if not ok:
            c.status = "error"
            c.last_error = err or "재시작 실패"
            db.commit()
            return _err(f"재시작 실패: {err}", "STREAM_ERROR", 500)

        c.status = "running"
        c.last_error = ""
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-009: POST /api/connectors/db/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/test", methods=["POST"])
@audit_route("connector", "connector.db.test", target_type="db_connector",
             target_name_kwarg="cid")
def test_connector(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        ok, msg, info = bm.test_db_connection(
            c.db_type, c.host, c.port, c.database, c.username, c.password,
        )
        return _ok({"success": ok, "message": msg, "info": info})
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-010: POST /api/connectors/db/test-connection — 연결 테스트 (등록 전)
# ──────────────────────────────────────────────
@db_bp.route("/test-connection", methods=["POST"])
@audit_route("connector", "connector.db.test_connection", target_type="db_connector",
             detail_keys=["dbType", "host", "port", "database"])
def test_connection_direct():
    body = request.get_json(force=True)
    db_type = body.get("dbType", "mysql")
    host = body.get("host", "localhost")
    port = int(body.get("port", 3306))
    database = body.get("database", "")
    username = body.get("username", "")
    password = body.get("password", "")

    ok, msg, info = bm.test_db_connection(db_type, host, port, database, username, password)
    return _ok({"success": ok, "message": msg, "info": info})


# ──────────────────────────────────────────────
# DB-011: GET /api/connectors/db/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/status", methods=["GET"])
def connector_status(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        stream = bm.get_db_stream_status(c)
        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "rowCount": c.row_count,
            "errorCount": c.error_count,
            "lastCollectedAt": c.last_collected_at.isoformat() if c.last_collected_at else None,
            "lastError": c.last_error,
            "benthos": stream,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-012: GET /api/connectors/db/<id>/tags — 태그(테이블) 목록
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/tags", methods=["GET"])
def list_tags(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        tags = db.query(DbTag).filter_by(connector_id=cid).order_by(DbTag.id).all()
        return _ok([t.to_dict() for t in tags])
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-013: POST /api/connectors/db/<id>/tags — 태그(테이블) 등록
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/tags", methods=["POST"])
@audit_route("connector", "connector.db.tag.create", target_type="db_tag",
             detail_keys=["tableName", "tagName", "primaryKey", "dataType"])
def create_tag(cid):
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        table_name = body.get("tableName", "").strip()
        tag_name = body.get("tagName", "").strip()
        if not table_name or not tag_name:
            return _err("tableName과 tagName은 필수입니다.", "VALIDATION")

        tag = DbTag(
            connector_id=cid,
            table_name=table_name,
            tag_name=tag_name,
            description=body.get("description", ""),
        )
        db.add(tag)
        db.commit()
        db.refresh(tag)
        return _ok(tag.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-014: DELETE /api/connectors/db/<id>/tags/<tagId> — 태그 삭제
# ──────────────────────────────────────────────
@db_bp.route("/<int:cid>/tags/<int:tid>", methods=["DELETE"])
@audit_route("connector", "connector.db.tag.delete", target_type="db_tag",
             target_name_kwarg="tid")
def delete_tag(cid, tid):
    db = _db()
    try:
        tag = db.query(DbTag).filter_by(id=tid, connector_id=cid).first()
        if not tag:
            return _err("태그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        db.delete(tag)
        db.commit()
        return _ok({"deleted": tid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# DB-015: GET /api/connectors/db/summary — 대시보드 통계
# ──────────────────────────────────────────────
@db_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(DbConnector.id)).scalar()
        running = db.query(func.count(DbConnector.id)).filter(DbConnector.status == "running").scalar()
        total_rows = db.query(func.coalesce(func.sum(DbConnector.row_count), 0)).scalar()

        # Count total monitored tables (supports both string and object array)
        all_connectors = db.query(DbConnector).all()
        table_count = 0
        for c in all_connectors:
            cfg = c.config or {}
            table_count += len(cfg.get("tables", []))

        return _ok({
            "totalConnectors": total,
            "runningConnectors": running,
            "totalRowCount": int(total_rows),
            "totalTables": table_count,
            "benthos_running": bm.is_running(),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/connectors/db/callback — Benthos 메시지 콜백
# ──────────────────────────────────────────────
@db_bp.route("/callback", methods=["POST"])
def message_callback():
    """Receives polling trigger from Benthos.
    Queries each configured table for new rows, updates stats, publishes to MQTT.
    """
    db = _db()
    try:
        body = request.get_json(force=True)
        meta = body.get("_meta", {})
        connector_id = meta.get("connector_id")

        if not connector_id:
            return "", 200

        c = db.query(DbConnector).get(connector_id)
        if not c:
            return "", 200

        from backend.services.metadata_tracker import ensure_connector_catalog
        ensure_connector_catalog("db", connector_id, c.name)

        cfg = deepcopy(c.config or {})
        tables = _normalize_tables_cfg(cfg)
        if not tables:
            return "", 200

        # 커서 상태: config 내 _cursors에 테이블별 last_id 저장
        cursors = cfg.get("_cursors", {})
        tags = db.query(DbTag).filter_by(connector_id=connector_id).all()
        total_new_rows = 0

        for tbl in tables:
            tbl_name = tbl["name"]
            tc = tbl["trackingColumn"]
            custom_sql = tbl.get("customSql", "")
            last_val = cursors.get(tbl_name, 0)

            rows = _query_table(c, tbl_name, tc, last_val, custom_sql=custom_sql)
            if not rows:
                continue

            # 태그 매칭
            matched_tags = [t for t in tags if t.table_name == tbl_name]
            for row in rows:
                # 커서 업데이트
                row_tc_val = row.get(tc, last_val)
                if isinstance(row_tc_val, (int, float)) and row_tc_val > last_val:
                    last_val = row_tc_val

                # MQTT 발행
                if matched_tags:
                    for tag in matched_tags:
                        mqtt_manager.publish_raw(
                            "db", connector_id, tag.tag_name, row,
                            data_type="json",
                        )
                else:
                    mqtt_manager.publish_raw(
                        "db", connector_id, tbl_name, row,
                        data_type="json",
                    )
                total_new_rows += 1

            cursors[tbl_name] = last_val

        # 통계 + 커서 저장
        if total_new_rows > 0:
            c.row_count = (c.row_count or 0) + total_new_rows
            c.last_collected_at = datetime.utcnow()
            cfg["_cursors"] = cursors
            c.config = cfg
            flag_modified(c, "config")
            db.commit()

        return "", 200
    except Exception as e:
        import traceback
        traceback.print_exc()
        return "", 200  # Benthos 재시도 방지를 위해 항상 200 반환
    finally:
        db.close()


def _normalize_tables_cfg(cfg):
    """Normalize tables from config (supports string array and object array)."""
    raw = cfg.get("tables", [])
    global_tc = cfg.get("trackingColumn", "id")
    global_sql = cfg.get("customSql", "")
    result = []
    for item in raw:
        if isinstance(item, str):
            result.append({"name": item, "trackingColumn": global_tc, "customSql": global_sql})
        elif isinstance(item, dict):
            result.append({
                "name": item.get("name", ""),
                "trackingColumn": item.get("trackingColumn", global_tc),
                "customSql": item.get("customSql", ""),
            })
    return [t for t in result if t["name"]]


def _query_table(connector, table_name, tracking_column, last_value, custom_sql=""):
    """Query a table for rows with tracking_column > last_value. Returns list of dicts.

    If custom_sql is provided, it is used instead of the default SELECT *.
    The custom SQL must contain exactly one %s placeholder for the last_value parameter.
    Example: "SELECT id, sensor_id, value FROM sensor_data WHERE id > %s ORDER BY id ASC LIMIT 500"
    """
    try:
        if connector.db_type in ("mysql", "mariadb"):
            import pymysql
            conn = pymysql.connect(
                host=connector.host, port=connector.port,
                user=connector.username, password=connector.password,
                database=connector.database, connect_timeout=10,
                cursorclass=pymysql.cursors.DictCursor,
            )
        elif connector.db_type == "postgresql":
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(
                host=connector.host, port=connector.port,
                user=connector.username, password=connector.password,
                dbname=connector.database, connect_timeout=10,
            )
        else:
            return []

        cfg = connector.config or {}
        batch_size = cfg.get("batchSize", 1000)

        if custom_sql:
            # 커스텀 SQL 사용: %s 플레이스홀더에 last_value 바인딩
            if connector.db_type in ("mysql", "mariadb"):
                cursor = conn.cursor()
            else:
                import psycopg2.extras
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(custom_sql, (last_value,))
            rows = cursor.fetchall()
            if connector.db_type == "postgresql":
                rows = [dict(r) for r in rows]
        elif connector.db_type in ("mysql", "mariadb"):
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM `{table_name}` WHERE `{tracking_column}` > %s "
                f"ORDER BY `{tracking_column}` ASC LIMIT %s",
                (last_value, batch_size),
            )
            rows = cursor.fetchall()
        elif connector.db_type == "postgresql":
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(
                f'SELECT * FROM "{table_name}" WHERE "{tracking_column}" > %s '
                f'ORDER BY "{tracking_column}" ASC LIMIT %s',
                (last_value, batch_size),
            )
            rows = [dict(r) for r in cursor.fetchall()]
        else:
            rows = []

        conn.close()

        # Convert datetime values to strings for JSON serialization
        result = []
        for row in rows:
            clean = {}
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    clean[k] = v.isoformat()
                else:
                    clean[k] = v
            result.append(clean)
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    return "http://localhost:5001/api/connectors/db/callback"


def _normalize_tables_input(tables_raw):
    """
    Normalize tables input from frontend.
    Accepts:
      - string array: ["sensor_data", "production_log"]
      - object array: [{"name":"sensor_data","trackingColumn":"id","customSql":""}, ...]
    Always returns object array format.
    """
    result = []
    for item in (tables_raw or []):
        if isinstance(item, str):
            name = item.strip()
            if name:
                result.append({"name": name, "trackingColumn": "id", "customSql": ""})
        elif isinstance(item, dict):
            name = (item.get("name") or "").strip()
            if name:
                result.append({
                    "name": name,
                    "trackingColumn": (item.get("trackingColumn") or "id").strip(),
                    "customSql": (item.get("customSql") or "").strip(),
                })
    return result
