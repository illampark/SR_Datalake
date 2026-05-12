from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from backend.database import SessionLocal
from backend.models.collector import ModbusConnector, ModbusTag
from backend.services import benthos_manager as bm
from backend.services import connector_workers
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size

modbus_bp = Blueprint("collector_modbus", __name__, url_prefix="/api/connectors/modbus")


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
# MODBUS-001: GET /api/connectors/modbus — 목록 조회
# ──────────────────────────────────────────────
@modbus_bp.route("", methods=["GET"])
def list_connectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        status_filter = request.args.get("status", "")
        modbus_type_filter = request.args.get("modbusType", "")
        search = (request.args.get("q") or "").strip()

        q = db.query(ModbusConnector)
        if status_filter:
            q = q.filter(ModbusConnector.status == status_filter)
        if modbus_type_filter:
            q = q.filter(ModbusConnector.modbus_type == modbus_type_filter)
        if search:
            like = f"%{search}%"
            q = q.filter(or_(ModbusConnector.name.ilike(like),
                             ModbusConnector.description.ilike(like)))

        total = q.count()
        rows = q.order_by(ModbusConnector.id).offset((page - 1) * size).limit(size).all()

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
# MODBUS-002: GET /api/connectors/modbus/<id> — 상세 조회
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>", methods=["GET"])
def get_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        stream = bm.get_modbus_stream_status(c)
        d["benthos_stream"] = stream
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-003: POST /api/connectors/modbus — 등록
# ──────────────────────────────────────────────
@modbus_bp.route("", methods=["POST"])
@audit_route("connector", "connector.modbus.create", target_type="modbus_connector",
             detail_keys=["name", "modbusType", "host", "port", "slaveIds", "pollingInterval"])
def create_connector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")

        if db.query(ModbusConnector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        modbus_type = body.get("modbusType", "tcp")
        function_codes = body.get("functionCodes", [3, 4])

        config = {}
        register_map = body.get("registerMap", "")
        if register_map:
            config["registerMap"] = register_map

        c = ModbusConnector(
            name=name,
            description=body.get("description", ""),
            modbus_type=modbus_type,
            host=body.get("host", "localhost"),
            port=int(body.get("port", 502)),
            serial_port=body.get("serialPort", ""),
            baudrate=int(body.get("baudrate", 9600)),
            data_bits=int(body.get("dataBits", 8)),
            parity=body.get("parity", "N"),
            stop_bits=int(body.get("stopBits", 1)),
            slave_ids=body.get("slaveIds", "1"),
            polling_interval=int(body.get("pollingInterval", 1000)),
            function_codes=function_codes,
            timeout=int(body.get("timeout", 3000)),
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
# MODBUS-004: PUT /api/connectors/modbus/<id> — 수정
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>", methods=["PUT"])
@audit_route("connector", "connector.modbus.update", target_type="modbus_connector",
             target_name_kwarg="cid",
             detail_keys=["name", "modbusType", "host", "port", "slaveIds", "pollingInterval", "enabled"])
def update_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)

        if "description" in body:
            c.description = body["description"]
        if "modbusType" in body:
            c.modbus_type = body["modbusType"]
        if "host" in body:
            c.host = body["host"]
        if "port" in body:
            c.port = int(body["port"])
        if "serialPort" in body:
            c.serial_port = body["serialPort"]
        if "baudrate" in body:
            c.baudrate = int(body["baudrate"])
        if "dataBits" in body:
            c.data_bits = int(body["dataBits"])
        if "parity" in body:
            c.parity = body["parity"]
        if "stopBits" in body:
            c.stop_bits = int(body["stopBits"])
        if "slaveIds" in body:
            c.slave_ids = body["slaveIds"]
        if "pollingInterval" in body:
            c.polling_interval = int(body["pollingInterval"])
        if "functionCodes" in body:
            c.function_codes = body["functionCodes"]
        if "timeout" in body:
            c.timeout = int(body["timeout"])

        # Merge config fields
        cfg = c.config or {}
        if "registerMap" in body:
            cfg["registerMap"] = body["registerMap"]
        c.config = cfg
        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "modbus", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "modbus", cid, body["name"])

        db.commit()
        db.refresh(c)

        # If running, restart worker so config changes apply
        if c.status == "running":
            connector_workers.stop_worker("modbus", cid)
            connector_workers.start_worker("modbus", cid)

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-005: DELETE /api/connectors/modbus/<id> — 삭제
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>", methods=["DELETE"])
@audit_route("connector", "connector.modbus.delete", target_type="modbus_connector",
             target_name_kwarg="cid")
def delete_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connector_workers.stop_worker("modbus", cid)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "modbus", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-006: POST /api/connectors/modbus/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/start", methods=["POST"])
@audit_route("connector", "connector.modbus.start", target_type="modbus_connector",
             target_name_kwarg="cid")
def start_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running" and connector_workers.is_running("modbus", cid):
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        ok, err = connector_workers.start_worker("modbus", cid)
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
# MODBUS-007: POST /api/connectors/modbus/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/stop", methods=["POST"])
@audit_route("connector", "connector.modbus.stop", target_type="modbus_connector",
             target_name_kwarg="cid")
def stop_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connector_workers.stop_worker("modbus", cid)
        c.status = "stopped"
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-008: POST /api/connectors/modbus/<id>/restart — 재시작
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/restart", methods=["POST"])
@audit_route("connector", "connector.modbus.restart", target_type="modbus_connector",
             target_name_kwarg="cid")
def restart_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        connector_workers.stop_worker("modbus", cid)

        ok, err = connector_workers.start_worker("modbus", cid)
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
# MODBUS-009: POST /api/connectors/modbus/<id>/test — 연결 테스트
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/test", methods=["POST"])
def test_connector(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        ok, msg, info = bm.test_modbus_connection(
            c.modbus_type, c.host, c.port, c.serial_port, c.baudrate,
            c.slave_ids, c.timeout,
        )
        return _ok({"success": ok, "message": msg, "info": info})
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-010: POST /api/connectors/modbus/test-connection — 연결 테스트 (등록 전)
# ──────────────────────────────────────────────
@modbus_bp.route("/test-connection", methods=["POST"])
def test_connection_direct():
    body = request.get_json(force=True)
    modbus_type = body.get("modbusType", "tcp")
    host = body.get("host", "localhost")
    port = int(body.get("port", 502))
    serial_port = body.get("serialPort", "")
    baudrate = int(body.get("baudrate", 9600))
    slave_ids = body.get("slaveIds", "1")
    timeout = int(body.get("timeout", 3000))

    ok, msg, info = bm.test_modbus_connection(
        modbus_type, host, port, serial_port, baudrate, slave_ids, timeout,
    )
    return _ok({"success": ok, "message": msg, "info": info})


# ──────────────────────────────────────────────
# MODBUS-011: GET /api/connectors/modbus/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/status", methods=["GET"])
def connector_status(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
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
            "workerAlive": connector_workers.is_running("modbus", cid),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-012: GET /api/connectors/modbus/<id>/tags — 태그 목록
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/tags", methods=["GET"])
def list_tags(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        tags = db.query(ModbusTag).filter_by(connector_id=cid).order_by(ModbusTag.id).all()
        return _ok([t.to_dict() for t in tags])
    finally:
        db.close()


# ──────────────────────────────────────────────
# MODBUS-013: POST /api/connectors/modbus/<id>/tags — 태그 등록
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/tags", methods=["POST"])
def create_tag(cid):
    db = _db()
    try:
        c = db.query(ModbusConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        tag_name = body.get("tagName", "").strip()
        if not tag_name:
            return _err("tagName은 필수입니다.", "VALIDATION")

        register_address = body.get("registerAddress")
        if register_address is None:
            return _err("registerAddress는 필수입니다.", "VALIDATION")

        tag = ModbusTag(
            connector_id=cid,
            tag_name=tag_name,
            function_code=int(body.get("functionCode", 3)),
            register_address=int(register_address),
            register_count=int(body.get("registerCount", 1)),
            data_type=body.get("dataType", "int16"),
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
# MODBUS-014: DELETE /api/connectors/modbus/<id>/tags/<tagId> — 태그 삭제
# ──────────────────────────────────────────────
@modbus_bp.route("/<int:cid>/tags/<int:tid>", methods=["DELETE"])
def delete_tag(cid, tid):
    db = _db()
    try:
        tag = db.query(ModbusTag).filter_by(id=tid, connector_id=cid).first()
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
# MODBUS-015: GET /api/connectors/modbus/summary — 대시보드 통계
# ──────────────────────────────────────────────
@modbus_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(ModbusConnector.id)).scalar()
        running = db.query(func.count(ModbusConnector.id)).filter(ModbusConnector.status == "running").scalar()
        total_points = db.query(func.coalesce(func.sum(ModbusConnector.point_count), 0)).scalar()
        tcp_count = db.query(func.count(ModbusConnector.id)).filter(ModbusConnector.modbus_type == "tcp").scalar()
        rtu_count = db.query(func.count(ModbusConnector.id)).filter(ModbusConnector.modbus_type == "rtu").scalar()

        return _ok({
            "totalConnectors": total,
            "runningConnectors": running,
            "totalPointCount": int(total_points),
            "tcpCount": tcp_count,
            "rtuCount": rtu_count,
            "benthos_running": bm.is_running(),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/connectors/modbus/callback — Benthos 메시지 콜백
# ──────────────────────────────────────────────
@modbus_bp.route("/callback", methods=["POST"])
def message_callback():
    """Receives data from Benthos HTTP output. Updates connector stats."""
    db = _db()
    try:
        body = request.get_json(force=True)
        meta = body.get("_meta", {})
        connector_id = meta.get("connector_id")

        if connector_id:
            c = db.query(ModbusConnector).get(connector_id)
            if c:
                c.point_count = (c.point_count or 0) + 1
                c.last_collected_at = datetime.utcnow()
                db.commit()
                from backend.services.metadata_tracker import ensure_connector_catalog
                ensure_connector_catalog("modbus", connector_id, c.name)

        return "", 200
    except Exception:
        return "", 200
    finally:
        db.close()


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    return "http://localhost:5001/api/connectors/modbus/callback"
