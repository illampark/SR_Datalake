from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from backend.database import SessionLocal
from backend.models.collector import OpcdaConnector, OpcdaTag
from backend.services import benthos_manager as bm

opcda_bp = Blueprint("collector_opcda", __name__, url_prefix="/api/connectors/opcda")


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
# OPCDA-001: GET /api/connectors/opcda — 목록 조회
# ──────────────────────────────────────────────
@opcda_bp.route("", methods=["GET"])
def list_connectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        status_filter = request.args.get("status", "")
        search = (request.args.get("q") or "").strip()

        q = db.query(OpcdaConnector)
        if status_filter:
            q = q.filter(OpcdaConnector.status == status_filter)
        if search:
            like = f"%{search}%"
            q = q.filter(or_(OpcdaConnector.name.ilike(like),
                             OpcdaConnector.description.ilike(like)))

        total = q.count()
        rows = q.order_by(OpcdaConnector.id).offset((page - 1) * size).limit(size).all()

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
# OPCDA-002: GET /api/connectors/opcda/<id> — 상세 조회
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>", methods=["GET"])
def get_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        stream = bm.get_opcda_stream_status(c)
        d["benthos_stream"] = stream
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-003: POST /api/connectors/opcda — 등록
# ──────────────────────────────────────────────
@opcda_bp.route("", methods=["POST"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        if db.query(OpcdaConnector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        config = {
            "groups": body.get("groups", {}),
        }

        c = OpcdaConnector(
            name=name,
            description=body.get("description", ""),
            server_name=body.get("serverName", ""),
            host=body.get("host", "localhost"),
            polling_interval=int(body.get("pollingInterval", 1000)),
            dcom_auth=body.get("dcomAuth", "default"),
            username=body.get("username", ""),
            password=body.get("password", ""),
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
# OPCDA-004: PUT /api/connectors/opcda/<id> — 수정
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>", methods=["PUT"])
def update_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)

        if "description" in body:
            c.description = body["description"]
        if "serverName" in body:
            c.server_name = body["serverName"]
        if "host" in body:
            c.host = body["host"]
        if "pollingInterval" in body:
            c.polling_interval = int(body["pollingInterval"])
        if "dcomAuth" in body:
            c.dcom_auth = body["dcomAuth"]
        if "username" in body:
            c.username = body["username"]
        if "password" in body and body["password"] != "":
            c.password = body["password"]

        # Merge config fields
        cfg = c.config or {}
        if "groups" in body:
            cfg["groups"] = body["groups"]
        c.config = cfg
        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "opcda", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "opcda", cid, body["name"])

        db.commit()
        db.refresh(c)

        # If running, update Benthos stream too
        if c.status == "running":
            callback_url = _callback_url()
            stream_config = bm.build_opcda_stream_config(c, callback_url)
            bm.update_stream(c.benthos_stream_id(), stream_config)

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-005: DELETE /api/connectors/opcda/<id> — 삭제
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>", methods=["DELETE"])
def delete_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            bm.stop_opcda_stream(c)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "opcda", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-006: POST /api/connectors/opcda/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/start", methods=["POST"])
def start_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_opcda_stream(c, callback_url)
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
# OPCDA-007: POST /api/connectors/opcda/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/stop", methods=["POST"])
def stop_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_opcda_stream(c)
        c.status = "stopped"
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-008: POST /api/connectors/opcda/<id>/restart — 재시작
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/restart", methods=["POST"])
def restart_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_opcda_stream(c)

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_opcda_stream(c, callback_url)
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
# OPCDA-009: POST /api/connectors/opcda/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/test", methods=["POST"])
def test_connector(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        ok, msg, info = bm.test_opcda_connection(
            c.server_name, c.host, c.dcom_auth, c.username, c.password,
        )
        return _ok({"success": ok, "message": msg, "info": info})
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-010: POST /api/connectors/opcda/test-connection — 연결 테스트 (등록 전)
# ──────────────────────────────────────────────
@opcda_bp.route("/test-connection", methods=["POST"])
def test_connection_direct():
    body = request.get_json(force=True)
    server_name = body.get("serverName", "")
    host = body.get("host", "localhost")
    dcom_auth = body.get("dcomAuth", "default")
    username = body.get("username", "")
    password = body.get("password", "")

    ok, msg, info = bm.test_opcda_connection(server_name, host, dcom_auth, username, password)
    return _ok({"success": ok, "message": msg, "info": info})


# ──────────────────────────────────────────────
# OPCDA-011: GET /api/connectors/opcda/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/status", methods=["GET"])
def connector_status(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        stream = bm.get_opcda_stream_status(c)
        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "pointCount": c.point_count,
            "groupCount": c.group_count,
            "errorCount": c.error_count,
            "lastCollectedAt": c.last_collected_at.isoformat() if c.last_collected_at else None,
            "lastError": c.last_error,
            "benthos": stream,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-012: GET /api/connectors/opcda/<id>/tags — 태그 목록
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/tags", methods=["GET"])
def list_tags(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        tags = db.query(OpcdaTag).filter_by(connector_id=cid).order_by(OpcdaTag.id).all()
        return _ok([t.to_dict() for t in tags])
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCDA-013: POST /api/connectors/opcda/<id>/tags — 태그 등록
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/tags", methods=["POST"])
def create_tag(cid):
    db = _db()
    try:
        c = db.query(OpcdaConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        group_name = body.get("groupName", "").strip()
        item_name = body.get("itemName", "").strip()
        tag_name = body.get("tagName", "").strip()
        if not group_name or not item_name or not tag_name:
            return _err("groupName, itemName, tagName은 필수입니다.", "VALIDATION")

        tag = OpcdaTag(
            connector_id=cid,
            group_name=group_name,
            item_name=item_name,
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
# OPCDA-014: DELETE /api/connectors/opcda/<id>/tags/<tagId> — 태그 삭제
# ──────────────────────────────────────────────
@opcda_bp.route("/<int:cid>/tags/<int:tid>", methods=["DELETE"])
def delete_tag(cid, tid):
    db = _db()
    try:
        tag = db.query(OpcdaTag).filter_by(id=tid, connector_id=cid).first()
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
# OPCDA-015: GET /api/connectors/opcda/summary — 대시보드 통계
# ──────────────────────────────────────────────
@opcda_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(OpcdaConnector.id)).scalar()
        running = db.query(func.count(OpcdaConnector.id)).filter(OpcdaConnector.status == "running").scalar()
        total_points = db.query(func.coalesce(func.sum(OpcdaConnector.point_count), 0)).scalar()

        # Count total unique groups from tags
        all_tags = db.query(OpcdaTag.group_name).distinct().count()

        return _ok({
            "totalConnectors": total,
            "runningConnectors": running,
            "totalPointCount": int(total_points),
            "totalGroups": all_tags,
            "benthos_running": bm.is_running(),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/connectors/opcda/callback — Benthos 메시지 콜백
# ──────────────────────────────────────────────
@opcda_bp.route("/callback", methods=["POST"])
def message_callback():
    """Receives data from Benthos HTTP output. Updates connector stats."""
    db = _db()
    try:
        body = request.get_json(force=True)
        meta = body.get("_meta", {})
        connector_id = meta.get("connector_id")

        if connector_id:
            c = db.query(OpcdaConnector).get(connector_id)
            if c:
                c.point_count = (c.point_count or 0) + 1
                c.group_count = db.query(OpcdaTag.group_name).filter_by(connector_id=connector_id).distinct().count()
                c.last_collected_at = datetime.utcnow()
                db.commit()
                from backend.services.metadata_tracker import ensure_connector_catalog
                ensure_connector_catalog("opcda", connector_id, c.name)

        return "", 200
    except Exception:
        return "", 200
    finally:
        db.close()


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    return "http://localhost:5001/api/connectors/opcda/callback"
