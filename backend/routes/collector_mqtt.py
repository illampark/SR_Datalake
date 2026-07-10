from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from backend.database import SessionLocal
from backend.models.collector import MqttConnector, MqttTag
from backend.services import benthos_manager as bm
from backend.services import mqtt_manager
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size
from backend.services.tenant_filter import filter_by_tenant, get_by_id_tenant, inject_tenant

mqtt_bp = Blueprint("collector_mqtt", __name__, url_prefix="/api/connectors/mqtt")


# ── role 기반 필터용 module-level state (gunicorn worker 별로 유지) ──
# static: 세션 내 첫 값만 발행 → 이후 skip
# change: 이전 값과 다를 때만 발행
_static_seen = set()          # {(connector_id, tag_id)}
_last_value = {}              # {(connector_id, tag_id): value}


def _cast_value(v, data_type):
    """AAS value_type 기반 정규화. 실패 시 원본 유지."""
    if v is None:
        return None
    dt = (data_type or "").lower()
    try:
        if dt == "float" and not isinstance(v, float):
            return float(v)
        if dt == "int" and not isinstance(v, int):
            if isinstance(v, float):
                return int(v)
            return int(str(v).strip())
        if dt == "bool" and not isinstance(v, bool):
            s = str(v).strip().lower()
            if s in ("true", "1", "yes", "on"):
                return True
            if s in ("false", "0", "no", "off"):
                return False
    except (ValueError, TypeError):
        pass
    return v


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

        q = filter_by_tenant(db.query(MqttConnector), MqttConnector)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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

        if filter_by_tenant(db.query(MqttConnector), MqttConnector).filter_by(name=name).first():
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
            "assetIdJsonPath": (body.get("assetIdJsonPath") or "").strip(),
            "timestampJsonPath": (body.get("timestampJsonPath") or "").strip(),
        }

        c = MqttConnector(
            name=name,
            description=body.get("description", ""),
            host=host,
            port=port,
            config=config,
        )
        inject_tenant(c)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        for key in ["clientId", "username", "password", "qos", "tls", "topics", "keepAlive", "cleanSession", "assetIdJsonPath", "timestampJsonPath"]:
            if key in body:
                if key == "password" and body[key] == "":
                    continue  # 빈 패스워드는 기존 값 유지
                cfg[key] = body[key]
        c.config = cfg
        c.updated_at = datetime.utcnow()
        # config 변경 트래킹 — reconciler 가 감지해서 worker 자동 재로드
        c.config_version = (c.config_version or 1) + 1

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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
        c = get_by_id_tenant(db, MqttConnector, cid)
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
             detail_keys=["topic", "tagName", "dataType", "jsonPath"])
def create_tag(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, MqttConnector, cid)
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
            json_path=(body.get("jsonPath") or "").strip(),
            description=body.get("description", ""),
        )
        inject_tenant(tag)
        db.add(tag)
        c.config_version = (c.config_version or 1) + 1
        db.commit()
        db.refresh(tag)
        return _ok(tag.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CONN-013a: PUT /api/connectors/mqtt/<id>/tags/<tagId> — 태그 수정
# ──────────────────────────────────────────────
@mqtt_bp.route("/<int:cid>/tags/<int:tid>", methods=["PUT"])
@audit_route("connector", "connector.mqtt.tag.update", target_type="mqtt_tag",
             target_name_kwarg="tid",
             detail_keys=["topic", "tagName", "dataType", "jsonPath"])
def update_tag(cid, tid):
    db = _db()
    try:
        # 커넥터 tenant 스코프 검증
        c = get_by_id_tenant(db, MqttConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        tag = db.query(MqttTag).filter_by(id=tid, connector_id=cid).first()
        if not tag:
            return _err("태그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True) or {}
        if "topic" in body:
            v = (body.get("topic") or "").strip()
            if not v:
                return _err("topic 은 비워둘 수 없습니다.", "VALIDATION")
            tag.topic = v
        if "tagName" in body:
            v = (body.get("tagName") or "").strip()
            if not v:
                return _err("tagName 은 비워둘 수 없습니다.", "VALIDATION")
            tag.tag_name = v
        if "dataType" in body:
            tag.data_type = body["dataType"] or "string"
        if "jsonPath" in body:
            tag.json_path = (body.get("jsonPath") or "").strip()
        if "submodelIdShort" in body:
            tag.submodel_id_short = (body.get("submodelIdShort") or "").strip()
        if "submodelRole" in body:
            v = (body.get("submodelRole") or "").strip().lower()
            if v not in ("", "stream", "change", "static"):
                return _err("submodelRole 는 stream/change/static 중 하나이거나 빈 문자열.", "VALIDATION")
            tag.submodel_role = v
        if "semanticId" in body:
            tag.semantic_id = (body.get("semanticId") or "").strip()
        if "description" in body:
            tag.description = (body["description"] or "")[:500]

        c.config_version = (c.config_version or 1) + 1
        db.commit()
        db.refresh(tag)
        return _ok(tag.to_dict())
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
        c = db.query(MqttConnector).get(cid)
        if c:
            c.config_version = (c.config_version or 1) + 1
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
        total = filter_by_tenant(db.query(func.count(MqttConnector.id)), MqttConnector).scalar()
        running = filter_by_tenant(db.query(func.count(MqttConnector.id)), MqttConnector).filter(MqttConnector.status == "running").scalar()
        total_rate = filter_by_tenant(db.query(func.coalesce(func.sum(MqttConnector.message_rate), 0)), MqttConnector).scalar()

        # Count topics from config
        all_connectors = filter_by_tenant(db.query(MqttConnector), MqttConnector).all()
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
    Receives messages from Benthos HTTP output and fans out to
    ``sdl/raw/mqtt/{cid}/{tag}`` for each matching MqttTag.
    """
    db = SessionLocal()
    try:
        body = request.get_json(force=True) or {}
        meta = body.get("_meta") or {}
        connector_id = meta.get("connector_id")
        if not connector_id:
            return "", 200

        c = db.query(MqttConnector).get(connector_id)
        if not c:
            return "", 200

        raw_topic = meta.get("topic") or ""
        raw_str = body.get("_raw_str") or ""
        raw_json = body.get("_raw_json")

        c.message_count = (c.message_count or 0) + 1
        c.last_message_at = datetime.utcnow()
        db.commit()

        try:
            from backend.services.metadata_tracker import ensure_connector_catalog
            ensure_connector_catalog("mqtt", connector_id, c.name)
        except Exception:
            pass

        # 커넥터 config 에서 asset_id / source timestamp 파싱 경로
        conn_cfg = c.config or {}
        asset_path = (conn_cfg.get("assetIdJsonPath") or "").strip()
        ts_path = (conn_cfg.get("timestampJsonPath") or "").strip()
        asset_id = ""
        source_ts = None
        if raw_json is not None:
            if asset_path:
                v = _extract_json_path(raw_json, asset_path)
                if v is not None:
                    asset_id = str(v)
            if ts_path:
                v = _extract_json_path(raw_json, ts_path)
                if v is not None:
                    source_ts = str(v)

        # payload 에서 asset_id 를 얻지 못했으면 AASX 의 static asset_id 로 fallback.
        # 1 커넥터 = 1 설비 시나리오에서 payload 에 asset_id 를 넣지 않아도 되도록.
        if not asset_id:
            shells = ((conn_cfg.get("aasMeta") or {}).get("shells") or [])
            if shells:
                asset_id = shells[0].get("asset_id") or shells[0].get("id_short") or ""

        tags = db.query(MqttTag).filter_by(connector_id=connector_id).all()
        for tag in tags:
            if not _mqtt_topic_match(tag.topic or "", raw_topic):
                continue
            path = (tag.json_path or "").strip()
            if path:
                if raw_json is None:
                    continue
                value = _extract_json_path(raw_json, path)
                if value is None:
                    continue
            else:
                value = raw_json if raw_json is not None else raw_str
            # value_type 캐스팅 (B1) — 실패 시 원본 유지
            value = _cast_value(value, tag.data_type or "string")
            # role 기반 필터 (A4)
            role = (tag.submodel_role or "").lower()
            key = (connector_id, tag.id)
            if role == "static":
                if key in _static_seen:
                    continue  # 이번 프로세스 세션 내 이미 발행함
                _static_seen.add(key)
            elif role == "change":
                prev = _last_value.get(key)
                if prev == value:
                    continue
                _last_value[key] = value
            try:
                mqtt_manager.publish_raw(
                    "mqtt", connector_id, tag.tag_name, value,
                    data_type=tag.data_type or "string",
                    asset_id=asset_id,
                    source_timestamp=source_ts,
                )
            except Exception:
                pass

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


def _mqtt_topic_match(pattern, topic):
    """MQTT wildcard 매칭 (# 다중 레벨, + 단일 레벨)."""
    if not pattern or not topic:
        return False
    if pattern == topic:
        return True
    p = pattern.split("/")
    t = topic.split("/")
    for i, seg in enumerate(p):
        if seg == "#":
            return True
        if i >= len(t):
            return False
        if seg == "+":
            continue
        if seg != t[i]:
            return False
    return len(p) == len(t)


def _extract_json_path(data, path):
    """단순 dot-notation JSONPath. ``$.a.b`` / ``a.b`` / ``a[0].b`` 지원."""
    if not path:
        return data
    p = path.lstrip("$").lstrip(".")
    cur = data
    for raw in p.split("."):
        if not raw:
            continue
        key = raw
        idx = None
        if "[" in key and key.endswith("]"):
            key, rest = key.split("[", 1)
            try:
                idx = int(rest[:-1])
            except ValueError:
                idx = None
        if key:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        if idx is not None:
            if not isinstance(cur, list) or idx >= len(cur):
                return None
            cur = cur[idx]
        if cur is None:
            return None
    return cur


# ══════════════════════════════════════════════
# AASX (Asset Administration Shell) 연동
# ══════════════════════════════════════════════

@mqtt_bp.route("/aasx-preview", methods=["POST"])
def aasx_preview():
    """AASX 파일 업로드 → 파싱 결과 반환 (저장 없음).

    body 옵션:
      exampleTopic — examplePayload 의 topic 문자열 (기본 'sdl/factory/asset/A')
    """
    f = request.files.get("file")
    if not f:
        return _err("파일이 필요합니다 (form-data 'file').", "VALIDATION")
    try:
        from backend.services.aasx_parser import parse_aasx, generate_example_payload
        data = parse_aasx(f.read())
    except Exception as e:
        return _err(f"AASX 파싱 실패: {e}", "PARSE_ERROR", 400)
    example_topic = (request.form.get("exampleTopic") or "sdl/factory/asset/A").strip()
    data["examplePayload"] = generate_example_payload(data, topic=example_topic)
    return _ok(data)


@mqtt_bp.route("/<int:cid>/aasx-apply", methods=["POST"])
@audit_route("connector", "connector.mqtt.aasx.apply", target_type="mqtt_connector",
             target_name_kwarg="cid",
             detail_keys=["submodelIdShort"])
def aasx_apply(cid):
    """AASX 파일 저장 (MinIO) + config.aasMeta 병합 + 선택 property → MqttTag 등록.

    body 는 form-data:
      file                    : .aasx 파일
      selections (신규 권장)  : JSON [{"submodelIdShort":"OperationalData","propertyPaths":["mode","setpoint"]}, ...]
      submodelIdShort         : (legacy) 단일 submodel
      selectedPropertyPaths   : (legacy) 단일 submodel property 목록
      autoAssetId, autoTimestamp : true|false (기본 true)
    """
    db = _db()
    try:
        c = get_by_id_tenant(db, MqttConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        f = request.files.get("file")
        if not f:
            return _err("파일이 필요합니다 (form-data 'file').", "VALIDATION")
        content = f.read()

        try:
            from backend.services.aasx_parser import parse_aasx
            aas_data = parse_aasx(content)
        except Exception as e:
            return _err(f"AASX 파싱 실패: {e}", "PARSE_ERROR", 400)

        import json as _json
        selections_raw = request.form.get("selections")
        legacy_paths = _json.loads(request.form.get("selectedPropertyPaths") or "[]")
        legacy_sm = request.form.get("submodelIdShort") or ""
        if selections_raw:
            try:
                selections = _json.loads(selections_raw)
                if not isinstance(selections, list):
                    return _err("selections 는 배열이어야 합니다.", "VALIDATION")
            except Exception:
                return _err("selections JSON 파싱 실패", "VALIDATION")
        elif legacy_paths:
            selections = [{"submodelIdShort": legacy_sm, "propertyPaths": legacy_paths}]
        else:
            selections = []
        auto_asset_id = (request.form.get("autoAssetId", "true").lower() == "true")
        auto_ts = (request.form.get("autoTimestamp", "true").lower() == "true")

        try:
            from backend.services.minio_client import get_minio_client
            from backend.services.minio_buckets import bucket_for
            from io import BytesIO
            client = get_minio_client(db)
            bucket = bucket_for("files", tenant_id=c.tenant_id)
            object_key = f"aasx/{cid}.aasx"
            client.put_object(
                bucket, object_key, BytesIO(content), len(content),
                content_type="application/asset-administration-shell-package",
            )
        except Exception as e:
            return _err(f"MinIO 업로드 실패: {e}", "STORAGE_ERROR", 500)

        # aasMeta 에 submodel 요약 (id_short/role/property 수) 도 함께 저장 → UI 매핑 상태 판단용
        submodel_summaries = [
            {
                "id_short": sm["id_short"],
                "role": sm.get("role", ""),
                "semantic_id": sm.get("semantic_id", ""),
                "property_count": len(sm.get("properties") or []),
            }
            for sm in aas_data["submodels"]
        ]

        cfg = dict(c.config or {})
        cfg["aasxObjectKey"] = f"{bucket}/{object_key}"
        cfg["aasMeta"] = {
            "shells": aas_data["shells"],
            "technicalData": aas_data["technical_data"],
            "digitalNameplate": aas_data["digital_nameplate"],
            "submodels": submodel_summaries,
        }
        if auto_asset_id and not cfg.get("assetIdJsonPath"):
            cfg["assetIdJsonPath"] = "$.asset_id"
        if auto_ts and not cfg.get("timestampJsonPath"):
            cfg["timestampJsonPath"] = "$.timestamp"
        c.config = cfg

        # submodel 별 property map 미리 구성
        sm_index = {sm["id_short"]: sm for sm in aas_data["submodels"]}

        topics = cfg.get("topics") or []
        topic_default = topics[0].replace("#", "").rstrip("/") if topics else ""

        added_tags = []
        applied_submodels = []
        for sel in selections:
            sm_key = sel.get("submodelIdShort") or ""
            paths = sel.get("propertyPaths") or []
            sm = sm_index.get(sm_key) if sm_key else (aas_data["submodels"][0] if aas_data["submodels"] else None)
            if sm is None:
                continue
            applied_submodels.append(sm["id_short"])
            prop_map = {p["path"]: p for p in sm.get("properties", [])}
            sm_role = sm.get("role") or ""
            for path in paths:
                p = prop_map.get(path)
                if not p:
                    continue
                exists = db.query(MqttTag).filter_by(
                    connector_id=cid, tag_name=p["id_short"]).first()
                if exists:
                    # 이미 있으면 AAS 메타만 갱신 (기존 topic/json_path 는 유지)
                    exists.submodel_id_short = sm["id_short"]
                    exists.submodel_role = sm_role
                    exists.semantic_id = p.get("semantic_id", "") or ""
                    continue
                tag = MqttTag(
                    connector_id=cid,
                    topic=topic_default or "#",
                    tag_name=p["id_short"],
                    data_type=p.get("value_type") or "string",
                    json_path=f"$.{path.replace('/', '.')}",
                    submodel_id_short=sm["id_short"],
                    submodel_role=sm_role,
                    semantic_id=p.get("semantic_id", "") or "",
                    description=(p.get("description") or "")[:500],
                )
                inject_tenant(tag)
                db.add(tag)
                added_tags.append({
                    "tagName": tag.tag_name,
                    "jsonPath": tag.json_path,
                    "dataType": tag.data_type,
                    "submodelIdShort": sm["id_short"],
                    "submodelRole": sm_role,
                    "semanticId": p.get("semantic_id", ""),
                })

        c.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(c)

        return _ok({
            "connector": c.to_dict(),
            "aasxObjectKey": cfg["aasxObjectKey"],
            "addedTags": added_tags,
            "submodels": applied_submodels,
        })
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


@mqtt_bp.route("/<int:cid>/aasx-file", methods=["GET"])
def aasx_download(cid):
    """저장된 AASX 원본 파일 다운로드 (tenant 스코프)."""
    db = _db()
    try:
        c = get_by_id_tenant(db, MqttConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        cfg = c.config or {}
        key = cfg.get("aasxObjectKey", "")
        if not key or "/" not in key:
            return _err("연동된 AASX 파일이 없습니다.", "NOT_FOUND", 404)
        bucket, _, obj = key.partition("/")
        try:
            from backend.services.minio_client import get_minio_client
            client = get_minio_client(db)
            resp = client.get_object(bucket, obj)
            data = resp.read()
            resp.close()
            resp.release_conn()
        except Exception as e:
            return _err(f"MinIO 다운로드 실패: {e}", "STORAGE_ERROR", 500)
        from flask import Response
        return Response(
            data,
            mimetype="application/asset-administration-shell-package",
            headers={
                "Content-Disposition": f'attachment; filename="mqtt-{cid}.aasx"',
                "Content-Length": str(len(data)),
            },
        )
    finally:
        db.close()


@mqtt_bp.route("/<int:cid>/aasx-content", methods=["GET"])
def aasx_content(cid):
    """저장된 AASX 원본을 재파싱해 shells/submodels/properties 를 반환 (미리보기와 동일 스키마)."""
    db = _db()
    try:
        c = get_by_id_tenant(db, MqttConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        cfg = c.config or {}
        key = cfg.get("aasxObjectKey", "")
        if not key or "/" not in key:
            return _err("연동된 AASX 파일이 없습니다.", "NOT_FOUND", 404)
        bucket, _, obj = key.partition("/")
        try:
            from backend.services.minio_client import get_minio_client
            client = get_minio_client(db)
            resp = client.get_object(bucket, obj)
            data = resp.read()
            resp.close()
            resp.release_conn()
        except Exception as e:
            return _err(f"MinIO 다운로드 실패: {e}", "STORAGE_ERROR", 500)
        try:
            from backend.services.aasx_parser import parse_aasx, generate_example_payload
            parsed = parse_aasx(data)
        except Exception as e:
            return _err(f"AASX 파싱 실패: {e}", "PARSE_ERROR", 400)
        # 커넥터의 실제 subscribe topic 을 예시 topic 으로 사용 (없으면 default)
        topics = (cfg.get("topics") or [])
        first_topic = ""
        if topics:
            first_topic = topics[0].replace("#", "").rstrip("/") + "/A"
        parsed["examplePayload"] = generate_example_payload(
            parsed, topic=first_topic or "sdl/factory/asset/A"
        )
        return _ok(parsed)
    finally:
        db.close()


@mqtt_bp.route("/<int:cid>/aasx-unlink", methods=["POST"])
@audit_route("connector", "connector.mqtt.aasx.unlink", target_type="mqtt_connector",
             target_name_kwarg="cid")
def aasx_unlink(cid):
    """AASX 연동 해제 — MinIO 원본 삭제 + config.aas* 초기화. 등록된 태그는 유지."""
    db = _db()
    try:
        c = get_by_id_tenant(db, MqttConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        cfg = dict(c.config or {})
        key = cfg.get("aasxObjectKey", "")
        removed = False
        if key and "/" in key:
            try:
                from backend.services.minio_client import get_minio_client
                client = get_minio_client(db)
                bucket, _, obj = key.partition("/")
                client.remove_object(bucket, obj)
                removed = True
            except Exception:
                pass  # 파일이 이미 없어도 config 는 정리
        for k in ("aasxObjectKey", "aasMeta"):
            cfg.pop(k, None)
        c.config = cfg
        c.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(c)
        return _ok({"connector": c.to_dict(), "fileRemoved": removed})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()
