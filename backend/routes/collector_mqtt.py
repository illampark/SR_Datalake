from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from backend.database import SessionLocal
from backend.models.collector import MqttConnector, MqttTag
from backend.services import benthos_manager as bm
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size

mqtt_bp = Blueprint("collector_mqtt", __name__, url_prefix="/api/connectors/mqtt")


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
# CONN-001: GET /api/connectors/mqtt — 목록 조회
# ──────────────────────────────────────────────
@mqtt_bp.route("", methods=["GET"])
def list_connectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        status_filter = request.args.get("status", "")
        search = (request.args.get("q") or "").strip()

        q = db.query(MqttConnector)
        if status_filter:
            q = q.filter(MqttConnector.status == status_filter)
        if search:
            like = f"%{search}%"
            q = q.filter(or_(MqttConnector.name.ilike(like),
                             MqttConnector.description.ilike(like)))

        total = q.count()
        rows = q.order_by(MqttConnector.id).offset((page - 1) * size).limit(size).all()

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
# CONN-002: GET /api/connectors/mqtt/<id> — 상세 조회
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>", methods=["GET"])
def get_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        stream = bm.get_mqtt_stream_status(c)
        d["benthos_stream"] = stream
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-003: POST /api/connectors/mqtt — 등록
# ──────────────────────────────────────────────
@mqtt_bp.route("", methods=["POST"])
@audit_route("connector", "connector.mqtt.create", target_type="mqtt_connector",
             detail_keys=["name", "host", "port", "tls"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        if db.query(MqttConnector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        host = body.get("host", "localhost")
        port = int(body.get("port", 1883))

        config = {
            "clientId": body.get("clientId", f"sdl-mqtt-{name}"),
            "username": body.get("username", ""),
            "password": body.get("password", ""),
            "qos": int(body.get("qos", 1)),
            "tls": bool(body.get("tls", False)),
            "topics": body.get("topics", []),
            "keepAlive": int(body.get("keepAlive", 60)),
            "cleanSession": bool(body.get("cleanSession", True)),
        }

        c = MqttConnector(
            name=name,
            description=body.get("description", ""),
            host=host,
            port=port,
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
# CONN-004: PUT /api/connectors/mqtt/<id> — 수정
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>", methods=["PUT"])
@audit_route("connector", "connector.mqtt.update", target_type="mqtt_connector",
             target_name_kwarg="cid",
             detail_keys=["name", "host", "port", "tls", "enabled"])
def update_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)

        if "description" in body:
            c.description = body["description"]
        if "host" in body:
            c.host = body["host"]
        if "port" in body:
            c.port = int(body["port"])

        # Merge config fields
        cfg = c.config or {}
        for key in ["clientId", "username", "password", "qos", "tls", "topics", "keepAlive", "cleanSession"]:
            if key in body:
                if key == "password" and body[key] == "":
                    continue  # 빈 패스워드는 기존 값 유지
                cfg[key] = body[key]
        c.config = cfg
        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "mqtt", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "mqtt", cid, body["name"])

        db.commit()
        db.refresh(c)

        # If running, update Benthos stream too
        if c.status == "running":
            callback_url = _callback_url()
            stream_config = bm.build_mqtt_stream_config(c, callback_url)
            bm.update_stream(c.benthos_stream_id(), stream_config)

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-005: DELETE /api/connectors/mqtt/<id> — 삭제
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>", methods=["DELETE"])
@audit_route("connector", "connector.mqtt.delete", target_type="mqtt_connector",
             target_name_kwarg="cid")
def delete_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # Stop Benthos stream if running
        if c.status == "running":
            bm.stop_mqtt_stream(c)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "mqtt", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-006: POST /api/connectors/mqtt/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/start", methods=["POST"])
@audit_route("connector", "connector.mqtt.start", target_type="mqtt_connector",
             target_name_kwarg="cid")
def start_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        # Ensure Benthos is running
        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_mqtt_stream(c, callback_url)
        if not ok:
            c.status = "error"
            c.last_error = err or "스트림 생성 실패"
            db.commit()
            return _err(f"스트림 시작 실패: {err}", "STREAM_ERROR", 500)

        c.status = "running"
        c.last_error = ""
        c.message_count = 0
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
# CONN-007: POST /api/connectors/mqtt/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/stop", methods=["POST"])
@audit_route("connector", "connector.mqtt.stop", target_type="mqtt_connector",
             target_name_kwarg="cid")
def stop_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_mqtt_stream(c)
        c.status = "stopped"
        c.message_rate = 0
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-008: POST /api/connectors/mqtt/<id>/restart — 재시작
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/restart", methods=["POST"])
@audit_route("connector", "connector.mqtt.restart", target_type="mqtt_connector",
             target_name_kwarg="cid")
def restart_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_mqtt_stream(c)

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_mqtt_stream(c, callback_url)
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
# CONN-009: POST /api/connectors/mqtt/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/test", methods=["POST"])
@audit_route("connector", "connector.mqtt.test", target_type="mqtt_connector",
             target_name_kwarg="cid")
def test_connector(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        cfg = c.config or {}
        ok, msg = bm.test_mqtt_connection(
            c.host, c.port,
            tls=cfg.get("tls", False),
            username=cfg.get("username"),
            password=cfg.get("password"),
        )
        return _ok({"success": ok, "message": msg, "host": c.host, "port": c.port})
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-010: GET /api/connectors/mqtt/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/status", methods=["GET"])
def connector_status(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        stream = bm.get_mqtt_stream_status(c)
        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "messageRate": c.message_rate,
            "messageCount": c.message_count,
            "errorCount": c.error_count,
            "lastMessageAt": c.last_message_at.isoformat() if c.last_message_at else None,
            "lastError": c.last_error,
            "benthos": stream,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-011: GET /api/connectors/mqtt/<id>/tags — 태그 목록
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/tags", methods=["GET"])
def list_tags(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        tags = db.query(MqttTag).filter_by(connector_id=cid).order_by(MqttTag.id).all()
        return _ok([t.to_dict() for t in tags])
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-012: POST /api/connectors/mqtt/<id>/tags — 태그 등록
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/tags", methods=["POST"])
@audit_route("connector", "connector.mqtt.tag.create", target_type="mqtt_tag",
             detail_keys=["topic", "tagName", "dataType"])
def create_tag(cid):
    db = _db()
    try:
        c = db.query(MqttConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        topic = body.get("topic", "").strip()
        tag_name = body.get("tagName", "").strip()
        if not topic or not tag_name:
            return _err("topic과 tagName은 필수입니다.", "VALIDATION")

        tag = MqttTag(
            connector_id=cid,
            topic=topic,
            tag_name=tag_name,
            data_type=body.get("dataType", "string"),
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
# CONN-013: DELETE /api/connectors/mqtt/<id>/tags/<tagId> — 태그 삭제
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/tags/<int:tid>", methods=["DELETE"])
@audit_route("connector", "connector.mqtt.tag.delete", target_type="mqtt_tag",
             target_name_kwarg="tid")
def delete_tag(cid, tid):
    db = _db()
    try:
        tag = db.query(MqttTag).filter_by(id=tid, connector_id=cid).first()
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
# CONN-014: GET /api/connectors/mqtt/summary — 대시보드 통계
# ──────────────────────────────────────────────
@mqtt_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(MqttConnector.id)).scalar()
        running = db.query(func.count(MqttConnector.id)).filter(MqttConnector.status == "running").scalar()
        total_rate = db.query(func.coalesce(func.sum(MqttConnector.message_rate), 0)).scalar()

        # Count topics from config
        all_connectors = db.query(MqttConnector).all()
        topic_count = 0
        for c in all_connectors:
            cfg = c.config or {}
            topics = cfg.get("topics", [])
            topic_count += len(topics)

        return _ok({
            "totalConnectors": total,
            "runningConnectors": running,
            "totalMessageRate": float(total_rate),
            "totalTopics": topic_count,
            "benthos_running": bm.is_running(),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/connectors/mqtt/callback — Benthos 메시지 콜백
# ──────────────────────────────────────────────
@mqtt_bp.route("/callback", methods=["POST"])
def message_callback():
    """
    Receives messages from Benthos HTTP output.
    Updates connector stats (message count, rate, last_message_at).
    """
    db = _db()
    try:
        body = request.get_json(force=True)
        meta = body.get("_meta", {})
        connector_id = meta.get("connector_id")

        if connector_id:
            c = db.query(MqttConnector).get(connector_id)
            if c:
                c.message_count = (c.message_count or 0) + 1
                c.last_message_at = datetime.utcnow()
                db.commit()
                from backend.services.metadata_tracker import ensure_connector_catalog
                ensure_connector_catalog("mqtt", connector_id, c.name)

        return "", 200
    except Exception:
        return "", 200
    finally:
        db.close()


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    """Build the callback URL for Benthos HTTP output."""
    return "http://localhost:5001/api/connectors/mqtt/callback"
