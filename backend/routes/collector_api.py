"""API 커넥터 라우트 — REST API (MES/ERP/SCADA) 연동 커넥터 CRUD + 수집 제어"""

from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func
from backend.database import SessionLocal
from backend.models.collector import ApiConnector, ApiEndpoint
from backend.services import benthos_manager as bm
from backend.services import mqtt_manager

api_bp = Blueprint("collector_api", __name__, url_prefix="/api/connectors/api")


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
# API-001: GET /api/connectors/api — 목록 조회
# ──────────────────────────────────────────────
@api_bp.route("", methods=["GET"])
def list_connectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        status_filter = request.args.get("status", "")

        q = db.query(ApiConnector)
        if status_filter:
            q = q.filter(ApiConnector.status == status_filter)

        total = q.count()
        rows = q.order_by(ApiConnector.id).offset((page - 1) * size).limit(size).all()

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
# API-002: GET /api/connectors/api/<id> — 상세 조회
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>", methods=["GET"])
def get_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        d["endpoints"] = [e.to_dict() for e in c.endpoints]
        stream = bm.get_api_stream_status(c)
        d["benthos_stream"] = stream
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-003: POST /api/connectors/api — 등록
# ──────────────────────────────────────────────
@api_bp.route("", methods=["POST"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        base_url = body.get("baseUrl", "").strip()
        if not base_url:
            return _err("Base URL은 필수입니다.", "VALIDATION")

        if db.query(ApiConnector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        c = ApiConnector(
            name=name,
            description=body.get("description", ""),
            base_url=base_url,
            auth_type=body.get("authType", "none"),
            auth_config=body.get("authConfig", {}),
            request_format=body.get("requestFormat", "json"),
            schedule=body.get("schedule", "*/5 * * * *"),
            timeout=int(body.get("timeout", 30)),
            custom_headers=body.get("customHeaders", {}),
        )
        db.add(c)
        db.flush()

        # 엔드포인트 동시 등록
        for ep_data in body.get("endpoints", []):
            db.add(ApiEndpoint(
                connector_id=c.id,
                tag_name=ep_data.get("tagName", ""),
                method=ep_data.get("method", "GET"),
                path=ep_data.get("path", "/"),
                body_template=ep_data.get("bodyTemplate", ""),
                response_path=ep_data.get("responsePath", ""),
                data_type=ep_data.get("dataType", "json"),
                enabled=ep_data.get("enabled", True),
                description=ep_data.get("description", ""),
            ))

        db.commit()
        db.refresh(c)
        d = c.to_dict()
        d["endpoints"] = [e.to_dict() for e in c.endpoints]
        return _ok(d), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-004: PUT /api/connectors/api/<id> — 수정
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>", methods=["PUT"])
def update_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)

        if "description" in body:
            c.description = body["description"]
        if "baseUrl" in body:
            c.base_url = body["baseUrl"]
        if "authType" in body:
            c.auth_type = body["authType"]
        if "authConfig" in body:
            c.auth_config = body["authConfig"]
        if "requestFormat" in body:
            c.request_format = body["requestFormat"]
        if "schedule" in body:
            c.schedule = body["schedule"]
        if "timeout" in body:
            c.timeout = int(body["timeout"])
        if "customHeaders" in body:
            c.custom_headers = body["customHeaders"]

        # 엔드포인트 교체 (전체 교체 방식)
        if "endpoints" in body:
            db.query(ApiEndpoint).filter_by(connector_id=cid).delete()
            for ep_data in body["endpoints"]:
                db.add(ApiEndpoint(
                    connector_id=cid,
                    tag_name=ep_data.get("tagName", ""),
                    method=ep_data.get("method", "GET"),
                    path=ep_data.get("path", "/"),
                    body_template=ep_data.get("bodyTemplate", ""),
                    response_path=ep_data.get("responsePath", ""),
                    data_type=ep_data.get("dataType", "json"),
                    enabled=ep_data.get("enabled", True),
                    description=ep_data.get("description", ""),
                ))

        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "api", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "api", cid, body["name"])

        db.commit()
        db.refresh(c)

        # Hot reload: running 중이면 Benthos 스트림도 업데이트
        if c.status == "running":
            callback_url = _callback_url()
            stream_config = bm.build_api_stream_config(c, callback_url)
            bm.update_stream(c.benthos_stream_id(), stream_config)

        d = c.to_dict()
        d["endpoints"] = [e.to_dict() for e in c.endpoints]
        return _ok(d)
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-005: DELETE /api/connectors/api/<id> — 삭제
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>", methods=["DELETE"])
def delete_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            bm.stop_api_stream(c)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "api", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-006: POST /api/connectors/api/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/start", methods=["POST"])
def start_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_api_stream(c, callback_url)
        if not ok:
            c.status = "error"
            c.last_error = err or "스트림 생성 실패"
            db.commit()
            return _err(f"스트림 시작 실패: {err}", "STREAM_ERROR", 500)

        c.status = "running"
        c.last_error = ""
        c.request_count = 0
        c.success_count = 0
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
# API-007: POST /api/connectors/api/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/stop", methods=["POST"])
def stop_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_api_stream(c)
        c.status = "stopped"
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-008: POST /api/connectors/api/<id>/restart — 재시작
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/restart", methods=["POST"])
def restart_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        bm.stop_api_stream(c)

        if not bm.is_running():
            if not bm.start_benthos():
                return _err("Benthos 프로세스를 시작할 수 없습니다.", "BENTHOS_ERROR", 500)

        callback_url = _callback_url()
        ok, err = bm.start_api_stream(c, callback_url)
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
# API-009: POST /api/connectors/api/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/test", methods=["POST"])
def test_connector(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        ok, msg, info = bm.test_api_connection(
            c.base_url, "GET", c.custom_headers,
            c.auth_type, c.auth_config, c.timeout,
        )
        return _ok({"success": ok, "message": msg, "info": info})
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-010: POST /api/connectors/api/test-connection — 등록 전 테스트
# ──────────────────────────────────────────────
@api_bp.route("/test-connection", methods=["POST"])
def test_connection_direct():
    body = request.get_json(force=True)
    base_url = body.get("baseUrl", "")
    auth_type = body.get("authType", "none")
    auth_config = body.get("authConfig", {})
    custom_headers = body.get("customHeaders", {})
    timeout = int(body.get("timeout", 10))

    ok, msg, info = bm.test_api_connection(
        base_url, "GET", custom_headers,
        auth_type, auth_config, timeout,
    )
    return _ok({"success": ok, "message": msg, "info": info})


# ──────────────────────────────────────────────
# API-011: GET /api/connectors/api/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/status", methods=["GET"])
def connector_status(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        stream = bm.get_api_stream_status(c)
        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "requestCount": c.request_count,
            "successCount": c.success_count,
            "errorCount": c.error_count,
            "avgResponseMs": c.avg_response_ms,
            "lastCalledAt": c.last_called_at.isoformat() if c.last_called_at else None,
            "lastStatusCode": c.last_status_code,
            "lastError": c.last_error,
            "benthos": stream,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-012: GET /api/connectors/api/<id>/endpoints — 엔드포인트 목록
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/endpoints", methods=["GET"])
def list_endpoints(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        eps = db.query(ApiEndpoint).filter_by(connector_id=cid).order_by(ApiEndpoint.id).all()
        return _ok([e.to_dict() for e in eps])
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-013: POST /api/connectors/api/<id>/endpoints — 엔드포인트 등록
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/endpoints", methods=["POST"])
def create_endpoint(cid):
    db = _db()
    try:
        c = db.query(ApiConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        tag_name = body.get("tagName", "").strip()
        if not tag_name:
            return _err("tagName은 필수입니다.", "VALIDATION")

        ep = ApiEndpoint(
            connector_id=cid,
            tag_name=tag_name,
            method=body.get("method", "GET"),
            path=body.get("path", "/"),
            body_template=body.get("bodyTemplate", ""),
            response_path=body.get("responsePath", ""),
            data_type=body.get("dataType", "json"),
            enabled=body.get("enabled", True),
            description=body.get("description", ""),
        )
        db.add(ep)
        db.commit()
        db.refresh(ep)
        return _ok(ep.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-014: DELETE /api/connectors/api/<id>/endpoints/<eid> — 엔드포인트 삭제
# ──────────────────────────────────────────────
@api_bp.route("/<int:cid>/endpoints/<int:eid>", methods=["DELETE"])
def delete_endpoint(cid, eid):
    db = _db()
    try:
        ep = db.query(ApiEndpoint).filter_by(id=eid, connector_id=cid).first()
        if not ep:
            return _err("엔드포인트를 찾을 수 없습니다.", "NOT_FOUND", 404)

        db.delete(ep)
        db.commit()
        return _ok({"deleted": eid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-015: GET /api/connectors/api/summary — 대시보드 통계
# ──────────────────────────────────────────────
@api_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(ApiConnector.id)).scalar()
        running = db.query(func.count(ApiConnector.id)).filter(ApiConnector.status == "running").scalar()
        total_requests = db.query(func.coalesce(func.sum(ApiConnector.request_count), 0)).scalar()
        total_errors = db.query(func.coalesce(func.sum(ApiConnector.error_count), 0)).scalar()
        avg_resp = db.query(func.coalesce(func.avg(ApiConnector.avg_response_ms), 0)).scalar()

        total_endpoints = db.query(func.count(ApiEndpoint.id)).scalar()

        return _ok({
            "totalConnectors": total,
            "runningConnectors": running,
            "totalRequests": int(total_requests),
            "totalErrors": int(total_errors),
            "avgResponseMs": round(float(avg_resp), 1),
            "totalEndpoints": total_endpoints,
            "benthos_running": bm.is_running(),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# API-016: POST /api/connectors/api/callback — Benthos 데이터 콜백
# ──────────────────────────────────────────────
@api_bp.route("/callback", methods=["POST"])
def message_callback():
    """
    Benthos 스트림에서 수집된 데이터를 수신하고,
    커넥터 통계를 업데이트한 후 MQTT raw 토픽으로 발행하여
    파이프라인에서 사용할 수 있도록 연결한다.
    """
    db = _db()
    try:
        body = request.get_json(force=True)
        meta = body.get("_meta", {})
        connector_id = meta.get("connector_id")

        if not connector_id:
            return "", 200

        c = db.query(ApiConnector).get(connector_id)
        if not c:
            return "", 200

        from backend.services.metadata_tracker import ensure_connector_catalog
        ensure_connector_catalog("api", connector_id, c.name)

        # 통계 업데이트
        c.request_count = (c.request_count or 0) + 1
        c.success_count = (c.success_count or 0) + 1
        c.last_called_at = datetime.utcnow()
        c.last_status_code = 200

        # 각 활성 엔드포인트에 대해 MQTT raw 토픽 발행
        # 이를 통해 PipelineBinding(connector_type="api")이 자동으로 수신
        for ep in c.endpoints:
            if not ep.enabled:
                continue
            value = body  # 전체 응답을 값으로 전달
            mqtt_manager.publish_raw(
                "api", connector_id, ep.tag_name, value,
                data_type=ep.data_type,
            )

        db.commit()
        return "", 200
    except Exception:
        return "", 200  # Benthos 재시도 방지를 위해 항상 200 반환
    finally:
        db.close()


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    return "http://localhost:5001/api/connectors/api/callback"
