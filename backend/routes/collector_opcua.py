from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.collector import OpcuaConnector, OpcuaTag
from backend.services import benthos_manager as bm

opcua_bp = Blueprint("collector_opcua", __name__, url_prefix="/api/connectors/opcua")


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
# OPCUA-001: GET /api/connectors/opcua — 목록 조회
# ──────────────────────────────────────────────
@opcua_bp.route("", methods=["GET"])
def list_connectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        status_filter = request.args.get("status", "")

        q = db.query(OpcuaConnector)
        if status_filter:
            q = q.filter(OpcuaConnector.status == status_filter)

        total = q.count()
        rows = q.order_by(OpcuaConnector.id).offset((page - 1) * size).limit(size).all()

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
# OPCUA-002: GET /api/connectors/opcua/<id> — 상세 조회
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>", methods=["GET"])
def get_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        stream = bm.get_opcua_stream_status(c)
        d["benthos_stream"] = stream
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-003: POST /api/connectors/opcua — 등록
# ──────────────────────────────────────────────
@opcua_bp.route("", methods=["POST"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        if db.query(OpcuaConnector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        config = {
            "nodeIds": body.get("nodeIds", []),
        }

        c = OpcuaConnector(
            name=name,
            description=body.get("description", ""),
            server_url=body.get("serverUrl", "opc.tcp://localhost:4840"),
            namespace_index=int(body.get("namespaceIndex", 2)),
            security_policy=body.get("securityPolicy", "None"),
            security_mode=body.get("securityMode", "None"),
            auth_type=body.get("authType", "anonymous"),
            username=body.get("username", ""),
            password=body.get("password", ""),
            polling_interval=int(body.get("pollingInterval", 1000)),
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
# OPCUA-004: PUT /api/connectors/opcua/<id> — 수정
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>", methods=["PUT"])
def update_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)

        if "description" in body:
            c.description = body["description"]
        if "serverUrl" in body:
            c.server_url = body["serverUrl"]
        if "namespaceIndex" in body:
            c.namespace_index = int(body["namespaceIndex"])
        if "securityPolicy" in body:
            c.security_policy = body["securityPolicy"]
        if "securityMode" in body:
            c.security_mode = body["securityMode"]
        if "authType" in body:
            c.auth_type = body["authType"]
        if "username" in body:
            c.username = body["username"]
        if "password" in body and body["password"] != "":
            c.password = body["password"]
        if "pollingInterval" in body:
            c.polling_interval = int(body["pollingInterval"])

        # Merge config fields
        cfg = c.config or {}
        if "nodeIds" in body:
            cfg["nodeIds"] = body["nodeIds"]
        c.config = cfg
        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "opcua", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "opcua", cid, body["name"])

        db.commit()
        db.refresh(c)

        # If running, update Benthos stream too
        if c.status == "running":
            callback_url = _callback_url()
            stream_config = bm.build_opcua_stream_config(c, callback_url)
            bm.update_stream(c.benthos_stream_id(), stream_config)

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-005: DELETE /api/connectors/opcua/<id> — 삭제
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>", methods=["DELETE"])
def delete_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            bm.stop_opcua_stream(c)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "opcua", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-006: POST /api/connectors/opcua/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/start", methods=["POST"])
def start_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_opcua_stream(c, callback_url)
        if not ok:
            c.status = "error"
            c.last_error = err or "스트림 생성 실패"
            db.commit()
            return _err(f"스트림 시작 실패: {err}", "STREAM_ERROR", 500)

        c.status = "running"
        c.last_error = ""
        c.point_count = 0
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
# OPCUA-007: POST /api/connectors/opcua/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/stop", methods=["POST"])
def stop_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_opcua_stream(c)
        c.status = "stopped"
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-008: POST /api/connectors/opcua/<id>/restart — 재시작
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/restart", methods=["POST"])
def restart_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_opcua_stream(c)

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_opcua_stream(c, callback_url)
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
# OPCUA-009: POST /api/connectors/opcua/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/test", methods=["POST"])
def test_connector(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        ok, msg, info = bm.test_opcua_connection(
            c.server_url, c.security_policy, c.security_mode,
            c.auth_type, c.username, c.password,
        )
        return _ok({"success": ok, "message": msg, "info": info})
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-010: POST /api/connectors/opcua/test-connection — 연결 테스트 (등록 전)
# ──────────────────────────────────────────────
@opcua_bp.route("/test-connection", methods=["POST"])
def test_connection_direct():
    body = request.get_json(force=True)
    server_url = body.get("serverUrl", "opc.tcp://localhost:4840")
    security_policy = body.get("securityPolicy", "None")
    security_mode = body.get("securityMode", "None")
    auth_type = body.get("authType", "anonymous")
    username = body.get("username", "")
    password = body.get("password", "")

    ok, msg, info = bm.test_opcua_connection(
        server_url, security_policy, security_mode,
        auth_type, username, password,
    )
    return _ok({"success": ok, "message": msg, "info": info})


# ──────────────────────────────────────────────
# OPCUA-011: GET /api/connectors/opcua/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/status", methods=["GET"])
def connector_status(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        stream = bm.get_opcua_stream_status(c)
        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "pointCount": c.point_count,
            "errorCount": c.error_count,
            "lastCollectedAt": c.last_collected_at.isoformat() if c.last_collected_at else None,
            "lastError": c.last_error,
            "benthos": stream,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-012: GET /api/connectors/opcua/<id>/tags — 태그(노드) 목록
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/tags", methods=["GET"])
def list_tags(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        tags = db.query(OpcuaTag).filter_by(connector_id=cid).order_by(OpcuaTag.id).all()
        return _ok([t.to_dict() for t in tags])
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-013: POST /api/connectors/opcua/<id>/tags — 태그(노드) 등록
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/tags", methods=["POST"])
def create_tag(cid):
    db = _db()
    try:
        c = db.query(OpcuaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        node_id = body.get("nodeId", "").strip()
        tag_name = body.get("tagName", "").strip()
        if not node_id or not tag_name:
            return _err("nodeId와 tagName은 필수입니다.", "VALIDATION")

        tag = OpcuaTag(
            connector_id=cid,
            node_id=node_id,
            tag_name=tag_name,
            data_type=body.get("dataType", "float"),
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
# OPCUA-014: DELETE /api/connectors/opcua/<id>/tags/<tagId> — 태그 삭제
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/tags/<int:tid>", methods=["DELETE"])
def delete_tag(cid, tid):
    db = _db()
    try:
        tag = db.query(OpcuaTag).filter_by(id=tid, connector_id=cid).first()
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
# OPCUA-015: GET /api/connectors/opcua/summary — 대시보드 통계
# ──────────────────────────────────────────────
@opcua_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(OpcuaConnector.id)).scalar()
        running = db.query(func.count(OpcuaConnector.id)).filter(OpcuaConnector.status == "running").scalar()
        total_points = db.query(func.coalesce(func.sum(OpcuaConnector.point_count), 0)).scalar()

        # Count total monitored node IDs
        all_connectors = db.query(OpcuaConnector).all()
        node_count = 0
        for c in all_connectors:
            cfg = c.config or {}
            node_ids = cfg.get("nodeIds", [])
            node_count += len(node_ids)

        return _ok({
            "totalConnectors": total,
            "runningConnectors": running,
            "totalPointCount": int(total_points),
            "totalNodeIds": node_count,
            "benthos_running": bm.is_running(),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/connectors/opcua/callback — Benthos 메시지 콜백
# ──────────────────────────────────────────────
@opcua_bp.route("/callback", methods=["POST"])
def message_callback():
    """Receives data points from Benthos HTTP output. Updates connector stats."""
    db = _db()
    try:
        body = request.get_json(force=True)
        meta = body.get("_meta", {})
        connector_id = meta.get("connector_id")

        if connector_id:
            c = db.query(OpcuaConnector).get(connector_id)
            if c:
                c.point_count = (c.point_count or 0) + 1
                c.last_collected_at = datetime.utcnow()
                db.commit()

        return "", 200
    except Exception:
        return "", 200
    finally:
        db.close()


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    return "http://localhost:5001/api/connectors/opcua/callback"
