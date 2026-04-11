"""메타데이터 API — 태그 메타데이터 + 데이터 계보 + 통합 검색"""

import re
from collections import defaultdict

from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_

from backend.database import SessionLocal
from backend.models.metadata import TagMetadata, DataLineage
from backend.models.catalog import DataCatalog
from backend.models.pipeline import Pipeline, PipelineBinding

metadata_bp = Blueprint("metadata", __name__, url_prefix="/api/metadata")


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
# META-001: GET /api/metadata/tags — 태그 메타 목록
# ──────────────────────────────────────────────
@metadata_bp.route("/tags", methods=["GET"])
def list_tag_metadata():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        connector_type = request.args.get("connector_type", "")
        connector_id = request.args.get("connector_id", 0, type=int)
        active_only = request.args.get("active", "").lower() == "true"
        search = request.args.get("search", "")
        category_filter = request.args.get("category", "")
        sensitivity_filter = request.args.get("sensitivity", "")
        data_level_filter = request.args.get("data_level", "")

        q = db.query(TagMetadata)
        if connector_type:
            q = q.filter(TagMetadata.connector_type == connector_type)
        if connector_id:
            q = q.filter(TagMetadata.connector_id == connector_id)
        if active_only:
            q = q.filter(TagMetadata.is_active == True)  # noqa: E712
        if search:
            q = q.filter(TagMetadata.tag_name.ilike(f"%{search}%"))
        if category_filter:
            q = q.filter(TagMetadata.category == category_filter)
        if sensitivity_filter:
            q = q.filter(TagMetadata.sensitivity == sensitivity_filter)
        if data_level_filter:
            q = q.filter(TagMetadata.data_level == data_level_filter)

        total = q.count()
        rows = q.order_by(TagMetadata.id).offset((page - 1) * size).limit(size).all()

        # 집계 통계 (stat 카드용)
        total_tags_count = db.query(func.count(TagMetadata.id)).scalar() or 0
        active_tags_count = db.query(func.count(TagMetadata.id)).filter(
            TagMetadata.is_active == True  # noqa: E712
        ).scalar() or 0
        avg_quality_val = db.query(func.avg(TagMetadata.quality_score)).filter(
            TagMetadata.is_active == True  # noqa: E712
        ).scalar()
        lineage_count_val = db.query(func.count(DataLineage.id)).scalar() or 0

        return _ok({
            "items": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
            "totalTags": total_tags_count,
            "activeTags": active_tags_count,
            "avgQuality": round(float(avg_quality_val or 0), 1),
            "lineageCount": lineage_count_val,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# META-002: GET /api/metadata/tags/<id>
# ──────────────────────────────────────────────
@metadata_bp.route("/tags/<int:tid>", methods=["GET"])
def get_tag_metadata(tid):
    db = _db()
    try:
        m = db.query(TagMetadata).get(tid)
        if not m:
            return _err("메타데이터를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = m.to_dict()
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# META-002b: PUT /api/metadata/tags/<id> — 거버넌스 편집
# ──────────────────────────────────────────────
@metadata_bp.route("/tags/<int:tid>", methods=["PUT"])
def update_tag_metadata(tid):
    db = _db()
    try:
        m = db.query(TagMetadata).get(tid)
        if not m:
            return _err("메타데이터를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(silent=True) or {}
        gov_fields = {
            "description": "description", "owner": "owner",
            "category": "category", "dataLevel": "data_level",
            "sensitivity": "sensitivity", "retentionPolicy": "retention_policy",
            "isPublished": "is_published", "isDeprecated": "is_deprecated",
            "unit": "unit",
        }
        for api_key, db_key in gov_fields.items():
            if api_key in body:
                setattr(m, db_key, body[api_key])

        db.commit()
        db.refresh(m)
        return _ok(m.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# META-003: GET /api/metadata/tags/summary — 커넥터 타입별 요약
# ──────────────────────────────────────────────
@metadata_bp.route("/tags/summary", methods=["GET"])
def tag_summary():
    db = _db()
    try:
        from sqlalchemy import Integer
        rows = db.query(
            TagMetadata.connector_type,
            func.count(TagMetadata.id).label("total"),
            func.sum(func.cast(TagMetadata.is_active == True, Integer)).label("active"),  # noqa: E712
            func.avg(TagMetadata.quality_score).label("avg_quality"),
        ).group_by(TagMetadata.connector_type).all()

        summary = []
        for r in rows:
            summary.append({
                "connectorType": r[0],
                "totalTags": r[1],
                "activeTags": int(r[2] or 0),
                "avgQuality": round(float(r[3] or 0), 1),
            })
        return _ok(summary)
    except Exception:
        # 간단 fallback
        rows = db.query(TagMetadata).all()
        by_type = {}
        for r in rows:
            t = r.connector_type
            if t not in by_type:
                by_type[t] = {"total": 0, "active": 0, "quality_sum": 0}
            by_type[t]["total"] += 1
            if r.is_active:
                by_type[t]["active"] += 1
            by_type[t]["quality_sum"] += (r.quality_score or 0)

        summary = []
        for t, v in by_type.items():
            summary.append({
                "connectorType": t,
                "totalTags": v["total"],
                "activeTags": v["active"],
                "avgQuality": round(v["quality_sum"] / max(v["total"], 1), 1),
            })
        return _ok(summary)
    finally:
        db.close()


# ──────────────────────────────────────────────
# META-004: GET /api/metadata/lineage — 데이터 계보 조회
# ──────────────────────────────────────────────
@metadata_bp.route("/lineage", methods=["GET"])
def list_lineage():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        pipeline_id = request.args.get("pipeline_id", 0, type=int)
        connector_type = request.args.get("connector_type", "")

        q = db.query(DataLineage)
        if pipeline_id:
            q = q.filter(DataLineage.pipeline_id == pipeline_id)
        if connector_type:
            q = q.filter(DataLineage.source_connector_type == connector_type)

        total = q.count()
        rows = q.order_by(DataLineage.id.desc()).offset((page - 1) * size).limit(size).all()
        return _ok({
            "items": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# META-005: GET /api/metadata/lineage/<id>
# ──────────────────────────────────────────────
@metadata_bp.route("/lineage/<int:lid>", methods=["GET"])
def get_lineage(lid):
    db = _db()
    try:
        l = db.query(DataLineage).get(lid)
        if not l:
            return _err("계보 정보를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(l.to_dict())
    finally:
        db.close()


# ══════════════════════════════════════════════
# UC-1,3,5,6: 통합 태그 검색 (횡단 검색 + 파이프라인 연결 상태 + 카탈로그 매핑)
# ══════════════════════════════════════════════

_CONNECTOR_LABELS = {
    "opcua": "OPC-UA", "opcda": "OPC-DA", "modbus": "Modbus",
    "mqtt": "MQTT", "db": "DB", "file": "File", "api": "API",
}


@metadata_bp.route("/search/unified", methods=["GET"])
def unified_tag_search():
    """UC-1,3,6: 전체 커넥터 횡단 태그 검색 — 메타데이터+파이프라인+카탈로그 통합"""
    db = _db()
    try:
        q_str = request.args.get("q", "").strip()
        connector_type = request.args.get("connector_type", "")
        category = request.args.get("category", "")
        active_only = request.args.get("active", "").lower() == "true"
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)

        q = db.query(TagMetadata)
        if q_str:
            q = q.filter(or_(
                TagMetadata.tag_name.ilike(f"%{q_str}%"),
                TagMetadata.connector_name.ilike(f"%{q_str}%"),
                TagMetadata.unit.ilike(f"%{q_str}%"),
            ))
        if connector_type:
            q = q.filter(TagMetadata.connector_type == connector_type)
        if active_only:
            q = q.filter(TagMetadata.is_active == True)  # noqa: E712

        total = q.count()
        rows = q.order_by(
            TagMetadata.connector_type,
            TagMetadata.connector_id,
            TagMetadata.tag_name,
        ).offset((page - 1) * size).limit(size).all()

        # 배치로 파이프라인 바인딩 조회
        all_bindings = db.query(PipelineBinding).join(Pipeline).filter(
            Pipeline.enabled == True  # noqa: E712
        ).all()
        binding_map = {}
        for b in all_bindings:
            key = f"{b.connector_type}:{b.connector_id}"
            if key not in binding_map:
                binding_map[key] = []
            binding_map[key].append({
                "pipelineId": b.pipeline_id,
                "pipelineName": b.pipeline.name if b.pipeline else "",
                "pipelineStatus": b.pipeline.status if b.pipeline else "",
                "tagFilter": b.tag_filter,
            })

        # 카탈로그 매핑 배치 조회
        catalog_map = {}
        catalogs = db.query(DataCatalog).filter(
            DataCatalog.is_deprecated == False  # noqa: E712
        ).all()
        for c in catalogs:
            ckey = f"{c.connector_type}:{c.connector_id}:{c.tag_name}"
            catalog_map[ckey] = {
                "id": c.id, "name": c.name, "category": c.category,
                "dataLevel": c.data_level, "isPublished": c.is_published,
            }

        # 카테고리 필터 (카탈로그의 category 기반)
        items = []
        for m in rows:
            ckey = f"{m.connector_type}:{m.connector_id}:{m.tag_name}"
            cat_info = catalog_map.get(ckey)

            if category and (not cat_info or cat_info.get("category") != category):
                continue

            bkey = f"{m.connector_type}:{m.connector_id}"
            pipelines = []
            for bp in binding_map.get(bkey, []):
                tf = bp["tagFilter"]
                if tf == "*" or tf in m.tag_name or m.tag_name in tf:
                    pipelines.append(bp)

            items.append({
                **m.to_dict(),
                "connectorLabel": _CONNECTOR_LABELS.get(m.connector_type, m.connector_type),
                "pipelines": pipelines,
                "pipelineLinked": len(pipelines) > 0,
                "catalog": cat_info,
            })

        return _ok({
            "items": items,
            "total": total,
            "page": page,
            "size": size,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# UC-2: 태그 커버리지 — 파이프라인 미연결 태그 조회
# ──────────────────────────────────────────────
@metadata_bp.route("/search/coverage", methods=["GET"])
def tag_coverage():
    """UC-2: 커넥터별 태그의 파이프라인 연결/미연결 현황"""
    db = _db()
    try:
        connector_type = request.args.get("connector_type", "")
        connector_id = request.args.get("connector_id", 0, type=int)

        q = db.query(TagMetadata)
        if connector_type:
            q = q.filter(TagMetadata.connector_type == connector_type)
        if connector_id:
            q = q.filter(TagMetadata.connector_id == connector_id)

        tags = q.order_by(TagMetadata.connector_type, TagMetadata.tag_name).all()

        # 바인딩 조회
        bindings = db.query(PipelineBinding).join(Pipeline).all()
        binding_map = defaultdict(list)
        for b in bindings:
            key = f"{b.connector_type}:{b.connector_id}"
            binding_map[key].append({
                "pipelineId": b.pipeline_id,
                "pipelineName": b.pipeline.name if b.pipeline else "",
                "pipelineStatus": b.pipeline.status if b.pipeline else "",
                "tagFilter": b.tag_filter,
            })

        linked = []
        unlinked = []
        for m in tags:
            bkey = f"{m.connector_type}:{m.connector_id}"
            matched = []
            for bp in binding_map.get(bkey, []):
                tf = bp["tagFilter"]
                if tf == "*" or tf in m.tag_name or m.tag_name in tf:
                    matched.append(bp)

            item = {
                "id": m.id,
                "tagName": m.tag_name,
                "connectorType": m.connector_type,
                "connectorId": m.connector_id,
                "connectorName": m.connector_name,
                "connectorLabel": _CONNECTOR_LABELS.get(m.connector_type, m.connector_type),
                "isActive": m.is_active,
                "sampleCount": m.sample_count,
                "qualityScore": m.quality_score,
                "pipelines": matched,
            }
            if matched:
                linked.append(item)
            else:
                unlinked.append(item)

        return _ok({
            "linked": linked,
            "unlinked": unlinked,
            "linkedCount": len(linked),
            "unlinkedCount": len(unlinked),
            "totalCount": len(linked) + len(unlinked),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# UC-4: 태그별 파이프라인 역검색
# ──────────────────────────────────────────────
@metadata_bp.route("/search/tag-pipelines", methods=["GET"])
def tag_pipelines():
    """UC-4: 특정 태그를 처리하는 파이프라인 목록 조회"""
    db = _db()
    try:
        tag_name = request.args.get("tag", "")
        connector_type = request.args.get("connector_type", "")
        connector_id = request.args.get("connector_id", 0, type=int)

        if not tag_name:
            return _err("태그명(tag) 파라미터가 필요합니다.", "VALIDATION")

        # 바인딩에서 매칭되는 파이프라인 찾기
        bq = db.query(PipelineBinding).join(Pipeline)
        if connector_type:
            bq = bq.filter(PipelineBinding.connector_type == connector_type)
        if connector_id:
            bq = bq.filter(PipelineBinding.connector_id == connector_id)

        bindings = bq.all()
        matched_pipelines = []
        seen = set()

        for b in bindings:
            tf = b.tag_filter or "*"
            if tf == "*" or tf in tag_name or tag_name in tf:
                if b.pipeline_id not in seen:
                    seen.add(b.pipeline_id)
                    p = b.pipeline
                    # 이 파이프라인의 sink 카탈로그 조회
                    from backend.models.catalog import DataCatalog
                    sink_cats = db.query(DataCatalog).filter_by(
                        connector_type="pipeline", pipeline_id=p.id
                    ).all()

                    matched_pipelines.append({
                        "pipelineId": p.id,
                        "pipelineName": p.name,
                        "pipelineStatus": p.status,
                        "enabled": p.enabled,
                        "processedCount": p.processed_count,
                        "errorCount": p.error_count,
                        "inputTopic": p.input_topic,
                        "stepCount": len(p.steps) if p.steps else 0,
                        "bindingConnectorType": b.connector_type,
                        "bindingConnectorId": b.connector_id,
                        "tagFilter": b.tag_filter,
                        "sinkCatalogs": [{"id": sc.id, "name": sc.name, "sinkType": sc.sink_type} for sc in sink_cats],
                    })

        # DataLineage에서 추가 이력 조회
        lineage_count = db.query(func.count(DataLineage.id)).filter(
            DataLineage.source_tag == tag_name,
        ).scalar() or 0

        return _ok({
            "tagName": tag_name,
            "pipelines": matched_pipelines,
            "pipelineCount": len(matched_pipelines),
            "lineageRecords": lineage_count,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# UC-5: 태그명 패턴 분석
# ──────────────────────────────────────────────
@metadata_bp.route("/search/patterns", methods=["GET"])
def tag_patterns():
    """UC-5: 커넥터 타입별 태그명 네이밍 패턴 분석"""
    db = _db()
    try:
        connector_type = request.args.get("connector_type", "")

        q = db.query(TagMetadata)
        if connector_type:
            q = q.filter(TagMetadata.connector_type == connector_type)
        tags = q.all()

        # 접두사 패턴 추출 (언더스코어/하이픈 기준)
        prefix_groups = defaultdict(lambda: {"count": 0, "tags": [], "connectorTypes": set()})
        for t in tags:
            name = t.tag_name or ""
            parts = re.split(r"[_\-.]", name)
            prefix = parts[0].upper() if parts else "OTHER"

            prefix_groups[prefix]["count"] += 1
            if len(prefix_groups[prefix]["tags"]) < 5:
                prefix_groups[prefix]["tags"].append(name)
            prefix_groups[prefix]["connectorTypes"].add(t.connector_type)

        patterns = []
        for prefix, info in sorted(prefix_groups.items(), key=lambda x: -x[1]["count"]):
            patterns.append({
                "prefix": prefix,
                "count": info["count"],
                "sampleTags": info["tags"],
                "connectorTypes": sorted(info["connectorTypes"]),
            })

        # 커넥터 타입별 집계
        by_type = defaultdict(list)
        for t in tags:
            by_type[t.connector_type].append(t.tag_name)

        type_summary = []
        for ct, names in sorted(by_type.items()):
            type_summary.append({
                "connectorType": ct,
                "connectorLabel": _CONNECTOR_LABELS.get(ct, ct),
                "tagCount": len(names),
                "sampleTags": names[:5],
            })

        return _ok({
            "patterns": patterns,
            "byConnectorType": type_summary,
            "totalTags": len(tags),
        })
    finally:
        db.close()
