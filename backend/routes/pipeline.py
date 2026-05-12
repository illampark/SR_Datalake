"""파이프라인 API — Pipeline CRUD + Steps + Bindings + 제어"""

from datetime import datetime
from flask import Blueprint, request, jsonify
from backend.database import SessionLocal
from backend.models.pipeline import (
    Pipeline, PipelineStep, PipelineBinding,
    NormalizeRule, UnitConversion, FilterRule, AnomalyConfig,
    AggregateConfig, EnrichConfig, ScriptConfig,
)
from backend.models.collector import (
    OpcuaConnector, ModbusConnector,
    MqttConnector, ApiConnector, FileCollector, DbConnector, DbTag,
    ImportCollector,
)
from backend.services.audit_logger import audit_route
from backend.services.system_settings import get_default_page_size

pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/api/pipeline")


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


_CONNECTOR_MODELS = {
    "opcua": OpcuaConnector,
    "modbus": ModbusConnector,
    "mqtt": MqttConnector,
    "api": ApiConnector,
    "file": FileCollector,
    "db": DbConnector,
    "import": ImportCollector,
}


def _resolve_connector_name(db, connector_type, connector_id):
    model = _CONNECTOR_MODELS.get(connector_type)
    if model:
        obj = db.query(model).get(connector_id)
        if obj:
            return obj.name
    return None


def _enrich_bindings(db, bindings):
    result = []
    for b in bindings:
        bd = b.to_dict()
        bd["connectorName"] = _resolve_connector_name(db, b.connector_type, b.connector_id) or f"ID {b.connector_id}"
        result.append(bd)
    return result


# ══════════════════════════════════════════════
# Pipeline CRUD
# ══════════════════════════════════════════════

# PIP-001: GET /api/pipeline — 목록
@pipeline_bp.route("", methods=["GET"])
def list_pipelines():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        status_filter = request.args.get("status", "")

        q = db.query(Pipeline)
        if status_filter:
            q = q.filter(Pipeline.status == status_filter)

        total = q.count()
        rows = q.order_by(Pipeline.id).offset((page - 1) * size).limit(size).all()
        return _ok([r.to_dict() for r in rows], {"page": page, "size": size, "total": total})
    finally:
        db.close()


# PIP-002: GET /api/pipeline/<id>
@pipeline_bp.route("/<int:pid>", methods=["GET"])
def get_pipeline(pid):
    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = p.to_dict()
        d["steps"] = [s.to_dict() for s in p.steps]
        d["bindings"] = _enrich_bindings(db, p.bindings)
        return _ok(d)
    finally:
        db.close()


# PIP-003: POST /api/pipeline — 생성
def _require_source_step(steps):
    """파이프라인은 최소 1개의 소스 노드(*_source)를 가져야 한다.

    Option C 아키텍처: 노드(필수) ↔ 바인딩(자동 매핑). 노드 없는 파이프라인은
    런타임에 데이터가 흐를 곳이 없으므로 저장 단계에서 차단한다.
    """
    has_source = any(
        (s.get("moduleType") or "").endswith("_source") for s in (steps or [])
    )
    if not has_source:
        return ("최소 1개 이상의 소스 노드가 필요합니다. 팔레트에서 소스(*_source) 노드를 추가하세요.",
                "NO_SOURCE_STEP")
    return None


@pipeline_bp.route("", methods=["POST"])
@audit_route("pipeline", "pipeline.create", target_type="pipeline",
             detail_keys=["name", "description", "sourceType", "sourceId"])
def create_pipeline():
    db = _db()
    try:
        body = request.get_json(force=True)
        # 신규 생성은 draft 로 허용 (소스 노드 검증은 PUT 저장 시점에 적용).
        # 빌더 UI 가 "빈 파이프라인 생성 → select → 노드 추가 → 저장" 흐름이라
        # POST 에서 소스를 강제하면 사용자가 노드를 추가할 컨텍스트(selectedPipelineId)를
        # 만들 수 없어 데드락이 발생함.
        p = Pipeline(
            name=body.get("name", ""),
            description=body.get("description", ""),
            enabled=body.get("enabled", True),
        )
        db.add(p)
        db.flush()

        # 자동 토픽 설정
        p.input_topic = f"sdl/raw/#"
        p.output_topic = f"sdl/processed/{p.id}/#"

        # Steps 추가
        for i, step_data in enumerate(body.get("steps", [])):
            db.add(PipelineStep(
                pipeline_id=p.id,
                step_order=i,
                module_type=step_data.get("moduleType", ""),
                enabled=step_data.get("enabled", True),
                config=step_data.get("config", {}),
            ))

        # Bindings 추가
        for bind_data in body.get("bindings", []):
            db.add(PipelineBinding(
                pipeline_id=p.id,
                connector_type=bind_data.get("connectorType", ""),
                connector_id=bind_data.get("connectorId", 0),
                tag_filter=bind_data.get("tagFilter", "*"),
                enabled=bind_data.get("enabled", True),
            ))

        db.commit()
        db.refresh(p)

        # 싱크 카탈로그 자동 생성/갱신
        try:
            from backend.services.catalog_sync import sync_pipeline_catalogs
            sync_pipeline_catalogs(db, p.id, p.name, p.steps)
        except Exception:
            pass

        d = p.to_dict()
        d["steps"] = [s.to_dict() for s in p.steps]
        d["bindings"] = _enrich_bindings(db, p.bindings)
        return _ok(d), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-004: PUT /api/pipeline/<id> — 수정
@pipeline_bp.route("/<int:pid>", methods=["PUT"])
@audit_route("pipeline", "pipeline.update", target_type="pipeline",
             target_name_kwarg="pid",
             detail_keys=["name", "description", "sourceType", "sourceId", "enabled"])
def update_pipeline(pid):
    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        # steps 가 함께 전달될 때만 검증 (메타 only 업데이트는 통과)
        if "steps" in body:
            err = _require_source_step(body["steps"])
            if err:
                return _err(err[0], err[1])
        for js_key, col in {"name": "name", "description": "description", "enabled": "enabled"}.items():
            if js_key in body:
                setattr(p, col, body[js_key])

        # Steps 교체
        if "steps" in body:
            db.query(PipelineStep).filter_by(pipeline_id=pid).delete()
            for i, step_data in enumerate(body["steps"]):
                db.add(PipelineStep(
                    pipeline_id=pid,
                    step_order=i,
                    module_type=step_data.get("moduleType", ""),
                    enabled=step_data.get("enabled", True),
                    config=step_data.get("config", {}),
                ))

        # Bindings 교체
        if "bindings" in body:
            db.query(PipelineBinding).filter_by(pipeline_id=pid).delete()
            for bind_data in body["bindings"]:
                db.add(PipelineBinding(
                    pipeline_id=pid,
                    connector_type=bind_data.get("connectorType", ""),
                    connector_id=bind_data.get("connectorId", 0),
                    tag_filter=bind_data.get("tagFilter", "*"),
                    enabled=bind_data.get("enabled", True),
                ))

        db.commit()
        db.refresh(p)

        # 싱크 카탈로그 갱신 (싱크 변경 시 기존 카탈로그 정리 + 새 카탈로그 생성)
        try:
            from backend.services.catalog_sync import sync_pipeline_catalogs
            sync_pipeline_catalogs(db, p.id, p.name, p.steps)
        except Exception:
            pass

        d = p.to_dict()
        d["steps"] = [s.to_dict() for s in p.steps]
        d["bindings"] = _enrich_bindings(db, p.bindings)
        return _ok(d)
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-005: DELETE /api/pipeline/<id>
@pipeline_bp.route("/<int:pid>", methods=["DELETE"])
@audit_route("pipeline", "pipeline.delete", target_type="pipeline",
             target_name_kwarg="pid")
def delete_pipeline(pid):
    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)
        # 실행 중이면 먼저 정지
        if p.status == "running":
            return _err("실행 중인 파이프라인은 먼저 정지해주세요.", "CONFLICT", 409)

        # 싱크 카탈로그 삭제
        try:
            from backend.services.catalog_sync import delete_pipeline_catalogs
            delete_pipeline_catalogs(db, pid)
        except Exception:
            pass

        db.delete(p)
        db.commit()
        return _ok({"deleted": pid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ══════════════════════════════════════════════
# Pipeline Control (Start / Stop)
# ══════════════════════════════════════════════

# PIP-006: POST /api/pipeline/<id>/start
@pipeline_bp.route("/<int:pid>/start", methods=["POST"])
@audit_route("pipeline", "pipeline.start", target_type="pipeline",
             target_name_kwarg="pid")
def start_pipeline(pid):
    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)
        if p.status == "running":
            return _ok({"message": "이미 실행 중입니다."})

        # 파이프라인 엔진에 시작 요청
        from backend.services import pipeline_engine
        ok = pipeline_engine.start_pipeline(pid)
        if ok:
            p.status = "running"
            db.commit()
            return _ok({"message": "파이프라인 시작", "pipelineId": pid})
        else:
            return _err("파이프라인 시작 실패")
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-007: POST /api/pipeline/<id>/stop
@pipeline_bp.route("/<int:pid>/stop", methods=["POST"])
@audit_route("pipeline", "pipeline.stop", target_type="pipeline",
             target_name_kwarg="pid")
def stop_pipeline(pid):
    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)
        if p.status != "running":
            return _ok({"message": "실행 중이 아닙니다."})

        from backend.services import pipeline_engine
        pipeline_engine.stop_pipeline(pid)
        p.status = "stopped"
        db.commit()
        return _ok({"message": "파이프라인 정지", "pipelineId": pid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-007b: POST /api/pipeline/<id>/run-file-source
@pipeline_bp.route("/<int:pid>/run-file-source", methods=["POST"])
@audit_route("pipeline", "pipeline.run", target_type="pipeline",
             target_name_kwarg="pid")
def run_pipeline_file_source(pid):
    """파일 소스 모드 파이프라인 즉시 트리거 (백그라운드 실행).

    Multi-worker 환경에서 어떤 워커가 받아도 동일하게 동작하도록 DB 에서
    파이프라인 정의를 직접 검증한다. _running_pipelines in-memory state 에 의존 X.
    """
    import threading
    from backend.models.pipeline import Pipeline, PipelineStep
    from backend.services import pipeline_engine

    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)
        if p.status != "running":
            return _err(
                "파이프라인이 실행 중이 아닙니다. 먼저 [시작]을 누른 뒤 다시 시도하세요.",
                "NOT_RUNNING", 400,
            )
        has_file_source = db.query(PipelineStep).filter_by(
            pipeline_id=pid, enabled=True,
        ).filter(
            PipelineStep.module_type.in_(["import_source", "internal_file_source"])
        ).count() > 0
        if not has_file_source:
            return _err("이 파이프라인은 파일 소스 step 이 없습니다.", "WRONG_MODE", 400)
    finally:
        db.close()

    def _run():
        try:
            result = pipeline_engine.run_file_source(pid)
            pipeline_engine.logger.info("file-source thread done pipeline=%s: %s", pid, result)
        except Exception as ex:
            pipeline_engine.logger.error("file-source thread error pipeline=%s: %s", pid, ex)

    t = threading.Thread(target=_run, daemon=True, name=f"file-src-{pid}")
    t.start()
    return _ok({"pipelineId": pid, "status": "triggered"})


# PIP-008: GET /api/pipeline/<id>/status
@pipeline_bp.route("/<int:pid>/status", methods=["GET"])
def pipeline_status(pid):
    db = _db()
    try:
        p = db.query(Pipeline).get(pid)
        if not p:
            return _err("파이프라인을 찾을 수 없습니다.", "NOT_FOUND", 404)

        from backend.services import pipeline_engine
        runtime = pipeline_engine.get_pipeline_status(pid)
        return _ok({
            "pipelineId": pid,
            "name": p.name,
            "status": p.status,
            "processedCount": p.processed_count,
            "errorCount": p.error_count,
            "runtime": runtime,
        })
    finally:
        db.close()


# ══════════════════════════════════════════════
# Module Rule Libraries (NormalizeRule, UnitConversion, FilterRule, AnomalyConfig)
# ══════════════════════════════════════════════

# PIP-009: GET /api/pipeline/modules/normalize-rules
@pipeline_bp.route("/modules/normalize-rules", methods=["GET"])
def list_normalize_rules():
    db = _db()
    try:
        rows = db.query(NormalizeRule).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/normalize-rules", methods=["POST"])
def create_normalize_rule():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("규칙명을 입력하세요.", "VALIDATION")
        r = NormalizeRule(
            name=name,
            source_type=body.get("sourceType", ""),
            target_type=body.get("targetType", "float"),
            null_strategy=body.get("nullStrategy", "skip"),
            trim_whitespace=body.get("trimWhitespace", True),
            config=body.get("config", {}),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/normalize-rules/<int:rid>", methods=["GET"])
def get_normalize_rule(rid):
    db = _db()
    try:
        r = db.query(NormalizeRule).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/normalize-rules/<int:rid>", methods=["PUT"])
def update_normalize_rule(rid):
    db = _db()
    try:
        r = db.query(NormalizeRule).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("규칙명을 입력하세요.", "VALIDATION")
            r.name = name
        if "sourceType" in body:
            r.source_type = body["sourceType"]
        if "targetType" in body:
            r.target_type = body["targetType"]
        if "nullStrategy" in body:
            r.null_strategy = body["nullStrategy"]
        if "trimWhitespace" in body:
            r.trim_whitespace = body["trimWhitespace"]
        if "config" in body:
            r.config = body["config"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/normalize-rules/<int:rid>", methods=["DELETE"])
def delete_normalize_rule(rid):
    db = _db()
    try:
        r = db.query(NormalizeRule).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-010: GET /api/pipeline/modules/unit-conversions
@pipeline_bp.route("/modules/unit-conversions", methods=["GET"])
def list_unit_conversions():
    db = _db()
    try:
        rows = db.query(UnitConversion).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/unit-conversions", methods=["POST"])
def create_unit_conversion():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("규칙명을 입력하세요.", "VALIDATION")
        r = UnitConversion(
            name=name,
            category=body.get("category", "temperature"),
            source_unit=body.get("sourceUnit", ""),
            target_unit=body.get("targetUnit", ""),
            formula=body.get("formula", ""),
            factor=body.get("factor", 1.0),
            offset=body.get("offset", 0.0),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/unit-conversions/<int:rid>", methods=["GET"])
def get_unit_conversion(rid):
    db = _db()
    try:
        r = db.query(UnitConversion).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/unit-conversions/<int:rid>", methods=["PUT"])
def update_unit_conversion(rid):
    db = _db()
    try:
        r = db.query(UnitConversion).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("규칙명을 입력하세요.", "VALIDATION")
            r.name = name
        if "category" in body:
            r.category = body["category"]
        if "sourceUnit" in body:
            r.source_unit = body["sourceUnit"]
        if "targetUnit" in body:
            r.target_unit = body["targetUnit"]
        if "formula" in body:
            r.formula = body["formula"]
        if "factor" in body:
            r.factor = body["factor"]
        if "offset" in body:
            r.offset = body["offset"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/unit-conversions/<int:rid>", methods=["DELETE"])
def delete_unit_conversion(rid):
    db = _db()
    try:
        r = db.query(UnitConversion).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-011: Filter Rules CRUD
@pipeline_bp.route("/modules/filter-rules", methods=["GET"])
def list_filter_rules():
    db = _db()
    try:
        rows = db.query(FilterRule).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/filter-rules", methods=["POST"])
def create_filter_rule():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("규칙명을 입력하세요.", "VALIDATION")
        r = FilterRule(
            name=name,
            filter_type=body.get("filterType", "range"),
            field=body.get("field", "value"),
            min_value=body.get("minValue"),
            max_value=body.get("maxValue"),
            condition=body.get("condition", ""),
            action=body.get("action", "drop"),
            config=body.get("config", {}),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/filter-rules/<int:rid>", methods=["GET"])
def get_filter_rule(rid):
    db = _db()
    try:
        r = db.query(FilterRule).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/filter-rules/<int:rid>", methods=["PUT"])
def update_filter_rule(rid):
    db = _db()
    try:
        r = db.query(FilterRule).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("규칙명을 입력하세요.", "VALIDATION")
            r.name = name
        if "filterType" in body:
            r.filter_type = body["filterType"]
        if "field" in body:
            r.field = body["field"]
        if "minValue" in body:
            r.min_value = body["minValue"]
        if "maxValue" in body:
            r.max_value = body["maxValue"]
        if "condition" in body:
            r.condition = body["condition"]
        if "action" in body:
            r.action = body["action"]
        if "config" in body:
            r.config = body["config"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/filter-rules/<int:rid>", methods=["DELETE"])
def delete_filter_rule(rid):
    db = _db()
    try:
        r = db.query(FilterRule).get(rid)
        if not r:
            return _err("규칙을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-012: Anomaly Configs CRUD
@pipeline_bp.route("/modules/anomaly-configs", methods=["GET"])
def list_anomaly_configs():
    db = _db()
    try:
        rows = db.query(AnomalyConfig).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/anomaly-configs", methods=["POST"])
def create_anomaly_config():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("설정명을 입력하세요.", "VALIDATION")
        r = AnomalyConfig(
            name=name,
            method=body.get("method", "zscore"),
            threshold=body.get("threshold", 3.0),
            window_size=body.get("windowSize", 60),
            action=body.get("action", "flag"),
            replace_strategy=body.get("replaceStrategy", "mean"),
            config=body.get("config", {}),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/anomaly-configs/<int:rid>", methods=["GET"])
def get_anomaly_config(rid):
    db = _db()
    try:
        r = db.query(AnomalyConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/anomaly-configs/<int:rid>", methods=["PUT"])
def update_anomaly_config(rid):
    db = _db()
    try:
        r = db.query(AnomalyConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("설정명을 입력하세요.", "VALIDATION")
            r.name = name
        if "method" in body:
            r.method = body["method"]
        if "threshold" in body:
            r.threshold = body["threshold"]
        if "windowSize" in body:
            r.window_size = body["windowSize"]
        if "action" in body:
            r.action = body["action"]
        if "replaceStrategy" in body:
            r.replace_strategy = body["replaceStrategy"]
        if "config" in body:
            r.config = body["config"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/anomaly-configs/<int:rid>", methods=["DELETE"])
def delete_anomaly_config(rid):
    db = _db()
    try:
        r = db.query(AnomalyConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-013: Aggregate Configs CRUD
@pipeline_bp.route("/modules/aggregate-configs", methods=["GET"])
def list_aggregate_configs():
    db = _db()
    try:
        rows = db.query(AggregateConfig).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/aggregate-configs", methods=["POST"])
def create_aggregate_config():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("설정명을 입력하세요.", "VALIDATION")
        r = AggregateConfig(
            name=name,
            window_seconds=body.get("windowSeconds", 60),
            functions=body.get("functions", ["avg"]),
            emit_mode=body.get("emitMode", "end"),
            config=body.get("config", {}),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/aggregate-configs/<int:rid>", methods=["GET"])
def get_aggregate_config(rid):
    db = _db()
    try:
        r = db.query(AggregateConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/aggregate-configs/<int:rid>", methods=["PUT"])
def update_aggregate_config(rid):
    db = _db()
    try:
        r = db.query(AggregateConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("설정명을 입력하세요.", "VALIDATION")
            r.name = name
        if "windowSeconds" in body:
            r.window_seconds = body["windowSeconds"]
        if "functions" in body:
            r.functions = body["functions"]
        if "emitMode" in body:
            r.emit_mode = body["emitMode"]
        if "config" in body:
            r.config = body["config"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/aggregate-configs/<int:rid>", methods=["DELETE"])
def delete_aggregate_config(rid):
    db = _db()
    try:
        r = db.query(AggregateConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-014: Enrich Configs CRUD
@pipeline_bp.route("/modules/enrich-configs", methods=["GET"])
def list_enrich_configs():
    db = _db()
    try:
        rows = db.query(EnrichConfig).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/enrich-configs", methods=["POST"])
def create_enrich_config():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("설정명을 입력하세요.", "VALIDATION")
        r = EnrichConfig(
            name=name,
            fields=body.get("fields", {}),
            lookup_table=body.get("lookupTable", ""),
            add_timestamp=body.get("addTimestamp", True),
            config=body.get("config", {}),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/enrich-configs/<int:rid>", methods=["GET"])
def get_enrich_config(rid):
    db = _db()
    try:
        r = db.query(EnrichConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/enrich-configs/<int:rid>", methods=["PUT"])
def update_enrich_config(rid):
    db = _db()
    try:
        r = db.query(EnrichConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("설정명을 입력하세요.", "VALIDATION")
            r.name = name
        if "fields" in body:
            r.fields = body["fields"]
        if "lookupTable" in body:
            r.lookup_table = body["lookupTable"]
        if "addTimestamp" in body:
            r.add_timestamp = body["addTimestamp"]
        if "config" in body:
            r.config = body["config"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/enrich-configs/<int:rid>", methods=["DELETE"])
def delete_enrich_config(rid):
    db = _db()
    try:
        r = db.query(EnrichConfig).get(rid)
        if not r:
            return _err("설정을 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# PIP-015: Script Configs CRUD
@pipeline_bp.route("/modules/script-configs", methods=["GET"])
def list_script_configs():
    db = _db()
    try:
        rows = db.query(ScriptConfig).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


@pipeline_bp.route("/modules/script-configs", methods=["POST"])
def create_script_config():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = (body.get("name") or "").strip()
        if not name:
            return _err("스크립트명을 입력하세요.", "VALIDATION")
        r = ScriptConfig(
            name=name,
            language=body.get("language", "python"),
            code=body.get("code", ""),
            timeout=body.get("timeout", 5),
            description=body.get("description", ""),
            config=body.get("config", {}),
        )
        db.add(r)
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/script-configs/<int:rid>", methods=["GET"])
def get_script_config(rid):
    db = _db()
    try:
        r = db.query(ScriptConfig).get(rid)
        if not r:
            return _err("스크립트를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


@pipeline_bp.route("/modules/script-configs/<int:rid>", methods=["PUT"])
def update_script_config(rid):
    db = _db()
    try:
        r = db.query(ScriptConfig).get(rid)
        if not r:
            return _err("스크립트를 찾을 수 없습니다.", "NOT_FOUND", 404)
        body = request.get_json(force=True)
        if "name" in body:
            name = (body["name"] or "").strip()
            if not name:
                return _err("스크립트명을 입력하세요.", "VALIDATION")
            r.name = name
        if "language" in body:
            r.language = body["language"]
        if "code" in body:
            r.code = body["code"]
        if "timeout" in body:
            r.timeout = body["timeout"]
        if "description" in body:
            r.description = body["description"]
        if "config" in body:
            r.config = body["config"]
        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


@pipeline_bp.route("/modules/script-configs/<int:rid>", methods=["DELETE"])
def delete_script_config(rid):
    db = _db()
    try:
        r = db.query(ScriptConfig).get(rid)
        if not r:
            return _err("스크립트를 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(r)
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ══════════════════════════════════════════════
# Connector Listing (for Pipeline Binding Selection)
# ══════════════════════════════════════════════

# PIP-014: GET /api/pipeline/connectors/<type>
@pipeline_bp.route("/connectors/<connector_type>", methods=["GET"])
def list_connectors_by_type(connector_type):
    """Return registered connectors for a given type (for pipeline binding selection)."""
    db = _db()
    try:
        model = _CONNECTOR_MODELS.get(connector_type)
        if not model:
            return _err(f"지원하지 않는 커넥터 유형: {connector_type}", "INVALID_TYPE")
        rows = db.query(model).all()
        return _ok([{
            "id": r.id,
            "name": r.name,
            "status": getattr(r, "status", "unknown"),
        } for r in rows])
    finally:
        db.close()


# PIP-015: GET /api/pipeline/connectors/db/<id>/tables
@pipeline_bp.route("/connectors/db/<int:cid>/tables", methods=["GET"])
def get_db_connector_tables(cid):
    """Return configured tables and registered tags for a DB connector.

    Used by the pipeline builder UI to show table selection checkboxes.
    Returns a flat list of subscribable MQTT names:
      - If a table has tags: one entry per tag (mqttName = tagName)
      - If a table has no tags: one entry using table name (mqttName = tableName)
    """
    db = _db()
    try:
        c = db.query(DbConnector).get(cid)
        if not c:
            return _err("커넥터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        cfg = c.config or {}
        tables_raw = cfg.get("tables", [])
        tags = db.query(DbTag).filter_by(connector_id=cid).all()

        result = []
        for t in tables_raw:
            if isinstance(t, str):
                tbl_name = t
            elif isinstance(t, dict):
                tbl_name = t.get("name", "")
            else:
                continue
            if not tbl_name:
                continue

            matched_tags = [tag for tag in tags if tag.table_name == tbl_name]
            if matched_tags:
                for tag in matched_tags:
                    result.append({
                        "mqttName": tag.tag_name,
                        "tableName": tbl_name,
                        "isTag": True,
                        "description": tag.description or "",
                    })
            else:
                result.append({
                    "mqttName": tbl_name,
                    "tableName": tbl_name,
                    "isTag": False,
                    "description": "",
                })

        return _ok(result)
    finally:
        db.close()


# ══════════════════════════════════════════════
# MQTT Status
# ══════════════════════════════════════════════

# PIP-013: GET /api/pipeline/mqtt/status
@pipeline_bp.route("/mqtt/status", methods=["GET"])
def mqtt_status():
    from backend.services import mqtt_manager
    return _ok(mqtt_manager.get_status())
