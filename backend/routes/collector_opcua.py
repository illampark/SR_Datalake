from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from backend.database import SessionLocal
from backend.models.collector import OpcuaConnector, OpcuaTag
from backend.services import benthos_manager as bm
from backend.services import connector_workers
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size
from backend.services.tenant_filter import filter_by_tenant, get_by_id_tenant, inject_tenant

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
        size = request.args.get("size", get_default_page_size(), type=int)
        status_filter = request.args.get("status", "")
        search = (request.args.get("q") or "").strip()

        q = filter_by_tenant(db.query(OpcuaConnector), OpcuaConnector)
        if status_filter:
            q = q.filter(OpcuaConnector.status == status_filter)
        if search:
            like = f"%{search}%"
            q = q.filter(or_(OpcuaConnector.name.ilike(like),
                             OpcuaConnector.description.ilike(like)))

        total = q.count()
        rows = q.order_by(OpcuaConnector.id).offset((page - 1) * size).limit(size).all()

        # 실 커넥터 상태 — python-thread 워커 alive 여부
        items = []
        for r in rows:
            d = r.to_dict()
            d["workerAlive"] = connector_workers.is_running("opcua", r.id)
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
        c = get_by_id_tenant(db, OpcuaConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        d["workerAlive"] = connector_workers.is_running("opcua", cid)
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# OPCUA-003: POST /api/connectors/opcua — 등록
# ──────────────────────────────────────────────
@opcua_bp.route("", methods=["POST"])
@audit_route("connector", "connector.opcua.create", target_type="opcua_connector",
             detail_keys=["name", "serverUrl", "securityPolicy", "securityMode", "authType", "pollingInterval"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        if filter_by_tenant(db.query(OpcuaConnector), OpcuaConnector).filter_by(name=name).first():
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
# OPCUA-004: PUT /api/connectors/opcua/<id> — 수정
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>", methods=["PUT"])
@audit_route("connector", "connector.opcua.update", target_type="opcua_connector",
             target_name_kwarg="cid",
             detail_keys=["name", "serverUrl", "securityPolicy", "securityMode", "authType", "pollingInterval", "enabled"])
def update_connector(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
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
        # config 변경 트래킹 — reconciler 가 감지해서 worker 자동 재로드
        c.config_version = (c.config_version or 1) + 1

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

        # If running, restart worker so config changes apply
        if c.status == "running":
            connector_workers.stop_worker("opcua", cid)
            connector_workers.start_worker("opcua", cid)

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
@audit_route("connector", "connector.opcua.delete", target_type="opcua_connector",
             target_name_kwarg="cid")
def delete_connector(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connector_workers.stop_worker("opcua", cid)

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
@audit_route("connector", "connector.opcua.start", target_type="opcua_connector",
             target_name_kwarg="cid")
def start_connector(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running" and connector_workers.is_running("opcua", cid):
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        ok, err = connector_workers.start_worker("opcua", cid)
        if not ok:
            c.status = "error"
            c.last_error = err or "워커 시작 실패"
            db.commit()
            return _err(f"워커 시작 실패: {err}", "WORKER_ERROR", 500)

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
@audit_route("connector", "connector.opcua.stop", target_type="opcua_connector",
             target_name_kwarg="cid")
def stop_connector(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connector_workers.stop_worker("opcua", cid)
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
@audit_route("connector", "connector.opcua.restart", target_type="opcua_connector",
             target_name_kwarg="cid")
def restart_connector(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connector_workers.stop_worker("opcua", cid)

        ok, err = connector_workers.start_worker("opcua", cid)
        if not ok:
            c.status = "error"
            c.last_error = err or "재시작 실패"
            db.commit()
            return _err(f"재시작 실패: {err}", "WORKER_ERROR", 500)

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
@audit_route("connector", "connector.opcua.test", target_type="opcua_connector",
             target_name_kwarg="cid")
def test_connector(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
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
@audit_route("connector", "connector.opcua.test_connection", target_type="opcua_connector",
             detail_keys=["serverUrl", "securityPolicy", "authType"])
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
        c = get_by_id_tenant(db, OpcuaConnector, cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "pointCount": c.point_count,
            "errorCount": c.error_count,
            "lastCollectedAt": c.last_collected_at.isoformat() if c.last_collected_at else None,
            "lastError": c.last_error,
            "workerAlive": connector_workers.is_running("opcua", cid),
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
        c = get_by_id_tenant(db, OpcuaConnector, cid)
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
@audit_route("connector", "connector.opcua.tag.create", target_type="opcua_tag",
             detail_keys=["tagName", "nodeId", "dataType"])
def create_tag(cid):
    db = _db()
    try:
        c = get_by_id_tenant(db, OpcuaConnector, cid)
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
            tenant_id=c.tenant_id,
        )
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
# OPCUA-014: DELETE /api/connectors/opcua/<id>/tags/<tagId> — 태그 삭제
# ──────────────────────────────────────────────
@opcua_bp.route("/<int:cid>/tags/<int:tid>", methods=["DELETE"])
@audit_route("connector", "connector.opcua.tag.delete", target_type="opcua_tag",
             target_name_kwarg="tid")
def delete_tag(cid, tid):
    db = _db()
    try:
        tag = db.query(OpcuaTag).filter_by(id=tid, connector_id=cid).first()
        if not tag:
            return _err("태그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        db.delete(tag)
        c = db.query(OpcuaConnector).get(cid)
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
# OPCUA-015: GET /api/connectors/opcua/summary — 대시보드 통계
# ──────────────────────────────────────────────
@opcua_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = filter_by_tenant(db.query(func.count(OpcuaConnector.id)), OpcuaConnector).scalar()
        running = filter_by_tenant(db.query(func.count(OpcuaConnector.id)), OpcuaConnector).filter(OpcuaConnector.status == "running").scalar()
        total_points = filter_by_tenant(db.query(func.coalesce(func.sum(OpcuaConnector.point_count), 0)), OpcuaConnector).scalar()

        # Count total monitored node IDs
        all_connectors = filter_by_tenant(db.query(OpcuaConnector), OpcuaConnector).all()
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
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# NOTE: 이전에 있던 POST /callback 라우트 + _callback_url 헬퍼는 Benthos HTTP
# output 이 호출하던 경로. 커밋 b4e77e8 이후 python-thread 워커가
# mqtt_manager.publish_raw() 로 직접 발행하므로 dead code 였어 제거.
