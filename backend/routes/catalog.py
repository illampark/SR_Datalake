"""카탈로그 API — 사용자 수준 데이터 카탈로그 CRUD + 검색 + 데이터 조회/다운로드"""

import csv
import io
import os
from datetime import datetime, timedelta

from flask import Blueprint, request, jsonify, Response, send_file, session
from sqlalchemy import func

from backend.database import SessionLocal
from backend.models.catalog import DataCatalog, CatalogSearchTag, DataRecipe, AggregatedData
from backend.models.storage import TimeSeriesData

catalog_bp = Blueprint("catalog", __name__, url_prefix="/api/catalog")


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
# CAT-001: GET /api/catalog — 카탈로그 목록
# ──────────────────────────────────────────────
@catalog_bp.route("", methods=["GET"])
def list_catalogs():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        connector_type = request.args.get("connector_type", "")
        category = request.args.get("category", "")
        search = request.args.get("search", "")
        published_only = request.args.get("published", "").lower() == "true"
        tag_scope = request.args.get("tag_scope", "")
        data_level = request.args.get("data_level", "")

        q = db.query(DataCatalog)
        if connector_type:
            q = q.filter(DataCatalog.connector_type == connector_type)
        if category:
            q = q.filter(DataCatalog.category == category)
        if published_only:
            q = q.filter(DataCatalog.is_published == True)  # noqa: E712
        if tag_scope == "tag":
            q = q.filter(DataCatalog.tag_name != "")
        elif tag_scope == "connector":
            q = q.filter(DataCatalog.tag_name == "")
        if data_level:
            q = q.filter(DataCatalog.data_level == data_level)
        if search:
            q = q.filter(
                (DataCatalog.name.ilike(f"%{search}%")) |
                (DataCatalog.description.ilike(f"%{search}%")) |
                (DataCatalog.connector_description.ilike(f"%{search}%"))
            )

        total = q.count()
        rows = q.order_by(DataCatalog.id.desc()).offset((page - 1) * size).limit(size).all()

        # 집계 통계 (stat 카드용)
        published_count = db.query(func.count(DataCatalog.id)).filter(
            DataCatalog.is_published == True  # noqa: E712
        ).scalar() or 0
        connector_type_count = db.query(
            func.count(func.distinct(DataCatalog.connector_type))
        ).scalar() or 0

        return _ok({
            "items": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
            "publishedCount": published_count,
            "connectorTypeCount": connector_type_count,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-002: GET /api/catalog/<id>
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>", methods=["GET"])
def get_catalog(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        d["searchTags"] = [t.to_dict() for t in c.search_tags]

        # 실시간 메타데이터 통계 조회
        from backend.models.metadata import TagMetadata
        if c.tag_name:
            meta = db.query(TagMetadata).filter_by(
                connector_type=c.connector_type,
                connector_id=c.connector_id,
                tag_name=c.tag_name,
            ).first()
            if meta:
                d["liveStats"] = {
                    "qualityScore": meta.quality_score,
                    "sampleCount": meta.sample_count,
                    "lastValue": meta.last_value,
                    "isActive": meta.is_active,
                    "minObserved": meta.min_observed,
                    "maxObserved": meta.max_observed,
                    "avgObserved": meta.avg_observed,
                    "lastSeenAt": meta.last_seen_at.isoformat() if meta.last_seen_at else None,
                    "mqttTopic": meta.mqtt_topic,
                }
        else:
            # 커넥터 레벨: 전체 태그 집계 통계
            metas = db.query(TagMetadata).filter_by(
                connector_type=c.connector_type,
                connector_id=c.connector_id,
            ).all()
            if metas:
                d["connectorStats"] = {
                    "tagCount": len(metas),
                    "activeTagCount": sum(1 for m in metas if m.is_active),
                    "totalSampleCount": sum(m.sample_count or 0 for m in metas),
                    "avgQualityScore": round(
                        sum(m.quality_score or 0 for m in metas) / len(metas), 1
                    ),
                    "lastSeenAt": max(
                        (m.last_seen_at for m in metas if m.last_seen_at),
                        default=None,
                    ),
                }
                if d["connectorStats"]["lastSeenAt"]:
                    d["connectorStats"]["lastSeenAt"] = d["connectorStats"]["lastSeenAt"].isoformat()

        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-004: PUT /api/catalog/<id> — 카탈로그 수정
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>", methods=["PUT"])
def update_catalog(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        fields = {
            "name": "name", "description": "description",
            "connectorType": "connector_type", "connectorId": "connector_id",
            "tagName": "tag_name", "owner": "owner", "category": "category",
            "dataLevel": "data_level", "sensitivity": "sensitivity",
            "retentionPolicy": "retention_policy", "accessUrl": "access_url",
            "format": "format", "schemaInfo": "schema_info",
            "isPublished": "is_published", "isDeprecated": "is_deprecated",
        }
        for js_key, col in fields.items():
            if js_key in body:
                setattr(c, col, body[js_key])

        # 커넥터 변경 시 커넥터 설명 재조회
        if "connectorType" in body or "connectorId" in body:
            from backend.services.catalog_sync import get_connector_description
            c.connector_description = get_connector_description(
                db, c.connector_type, c.connector_id)

        # 검색 태그 교체
        if "searchTags" in body:
            db.query(CatalogSearchTag).filter_by(catalog_id=cid).delete()
            for tag_str in body["searchTags"]:
                db.add(CatalogSearchTag(catalog_id=cid, tag=tag_str))

        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-005: DELETE /api/catalog/<id>
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>", methods=["DELETE"])
def delete_catalog(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-006: GET /api/catalog/search — 통합 검색
# ──────────────────────────────────────────────
@catalog_bp.route("/search", methods=["GET"])
def search_catalog():
    db = _db()
    try:
        q_str = request.args.get("q", "")
        if not q_str:
            return _err("검색어를 입력해주세요.")

        # 카탈로그 이름/설명/커넥터 설명 검색 + 검색 태그 검색
        by_name = db.query(DataCatalog).filter(
            (DataCatalog.name.ilike(f"%{q_str}%")) |
            (DataCatalog.description.ilike(f"%{q_str}%")) |
            (DataCatalog.connector_description.ilike(f"%{q_str}%"))
        ).all()

        tag_ids = db.query(CatalogSearchTag.catalog_id).filter(
            CatalogSearchTag.tag.ilike(f"%{q_str}%")
        ).distinct().all()
        tag_catalog_ids = {r[0] for r in tag_ids}

        by_tag = []
        if tag_catalog_ids:
            by_tag = db.query(DataCatalog).filter(
                DataCatalog.id.in_(tag_catalog_ids)
            ).all()

        # 합집합 (중복 제거)
        seen = set()
        results = []
        for c in by_name + by_tag:
            if c.id not in seen:
                seen.add(c.id)
                results.append(c.to_dict())

        return _ok(results, {"total": len(results), "query": q_str})
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-007: GET /api/catalog/categories — 카테고리 목록
# ──────────────────────────────────────────────
@catalog_bp.route("/categories", methods=["GET"])
def list_categories():
    db = _db()
    try:
        rows = db.query(DataCatalog.category).distinct().all()
        categories = [r[0] for r in rows if r[0]]
        return _ok(categories)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-008: GET /api/catalog/tree — 커넥터 기반 계층 트리
# ──────────────────────────────────────────────
@catalog_bp.route("/tree", methods=["GET"])
def catalog_tree():
    db = _db()
    try:
        rows = db.query(DataCatalog).filter(
            DataCatalog.is_deprecated == False  # noqa: E712
        ).order_by(DataCatalog.connector_type, DataCatalog.connector_id).all()

        tree = {}
        for r in rows:
            ct = r.connector_type
            ci = str(r.connector_id or 0)
            if ct not in tree:
                tree[ct] = {}
            if ci not in tree[ct]:
                tree[ct][ci] = []
            tree[ct][ci].append({
                "id": r.id,
                "name": r.name,
                "tagName": r.tag_name,
                "category": r.category,
                "dataLevel": r.data_level,
            })

        return _ok(tree)
    finally:
        db.close()


# ──────────────────────────────────────────────
# 파이프라인 싱크 데이터 조회 헬퍼
# ──────────────────────────────────────────────

def _query_pipeline_tsdb(db, c, page, size, date_from, date_to, quality_min):
    """TSDB 싱크 — TimeSeriesData에서 pipeline_id + measurement 필터로 조회"""
    q = db.query(TimeSeriesData).filter(
        TimeSeriesData.pipeline_id == c.pipeline_id,
    )
    if c.tag_name:
        q = q.filter(TimeSeriesData.measurement == c.tag_name)

    if date_from:
        try:
            q = q.filter(TimeSeriesData.timestamp >= datetime.fromisoformat(date_from))
        except ValueError:
            pass
    if date_to:
        try:
            dt_to = datetime.fromisoformat(date_to)
            if dt_to.hour == 0 and dt_to.minute == 0:
                dt_to += timedelta(days=1)
            q = q.filter(TimeSeriesData.timestamp < dt_to)
        except ValueError:
            pass
    if quality_min > 0:
        q = q.filter(TimeSeriesData.quality >= quality_min)

    total = q.count()
    rows = q.order_by(TimeSeriesData.timestamp.desc()).offset((page - 1) * size).limit(size).all()

    # 통계
    stats_q = db.query(
        func.count(TimeSeriesData.id),
        func.min(TimeSeriesData.value),
        func.max(TimeSeriesData.value),
        func.avg(TimeSeriesData.value),
        func.min(TimeSeriesData.timestamp),
        func.max(TimeSeriesData.timestamp),
    ).filter(
        TimeSeriesData.pipeline_id == c.pipeline_id,
    )
    if c.tag_name:
        stats_q = stats_q.filter(TimeSeriesData.measurement == c.tag_name)
    sr = stats_q.first()

    return _ok({
        "catalog": {"id": c.id, "name": c.name, "tagName": c.tag_name,
                    "connectorType": c.connector_type, "pipelineId": c.pipeline_id},
        "sinkType": "internal_tsdb_sink",
        "isConnectorLevel": False,
        "items": [r.to_dict() for r in rows],
        "total": total,
        "page": page,
        "size": size,
        "stats": {
            "count": sr[0] if sr else 0,
            "min": round(sr[1], 4) if sr and sr[1] is not None else None,
            "max": round(sr[2], 4) if sr and sr[2] is not None else None,
            "avg": round(sr[3], 4) if sr and sr[3] is not None else None,
            "firstAt": sr[4].isoformat() if sr and sr[4] else None,
            "lastAt": sr[5].isoformat() if sr and sr[5] else None,
        },
    })


def _get_pipeline_sink_config(db, pipeline_id, sink_type):
    """파이프라인의 특정 싱크 타입 설정 조회"""
    from backend.models.pipeline import PipelineStep
    step = db.query(PipelineStep).filter_by(
        pipeline_id=pipeline_id,
        module_type=sink_type,
    ).first()
    return step.config if step else {}


def _query_pipeline_rdbms(db, c, page, size, date_from, date_to):
    """RDBMS 싱크 — 외부 DB에서 직접 조회"""
    from backend.models.storage import RdbmsConfig
    import logging
    _log = logging.getLogger(__name__)

    cfg = _get_pipeline_sink_config(db, c.pipeline_id, "internal_rdbms_sink")
    rdbms_id = cfg.get("rdbmsId", 0)
    table_name = cfg.get("tableName", "")

    if not rdbms_id or not table_name:
        return _err("RDBMS 싱크 설정을 찾을 수 없습니다.", "CONFIG_NOT_FOUND")

    rdbms = db.query(RdbmsConfig).get(rdbms_id)
    if not rdbms:
        return _err("RDBMS 설정을 찾을 수 없습니다.", "CONFIG_NOT_FOUND")

    db_type = (rdbms.db_type or "").lower()
    try:
        if "mysql" in db_type or "maria" in db_type:
            columns, items, total = _rdbms_read_mysql(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                table_name, page, size, date_from, date_to)
        else:
            columns, items, total = _rdbms_read_pg(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                rdbms.schema_name or "public",
                table_name, page, size, date_from, date_to)
    except Exception as e:
        _log.error("RDBMS 싱크 조회 실패 (rdbms=%d, table=%s): %s", rdbms_id, table_name, e)
        return _err("외부 DB 연결/조회에 실패했습니다.", "RDBMS_ERROR")

    return _ok({
        "catalog": {"id": c.id, "name": c.name, "tagName": c.tag_name,
                    "connectorType": c.connector_type, "pipelineId": c.pipeline_id},
        "sinkType": "internal_rdbms_sink",
        "columns": columns,
        "items": items,
        "total": total,
        "page": page,
        "size": size,
        "rdbmsInfo": {
            "dbType": rdbms.db_type,
            "database": rdbms.database_name,
            "tableName": table_name,
        },
    })


def _rdbms_read_mysql(host, port, database, username, password,
                       table_name, page, size, date_from, date_to):
    import pymysql
    conn = pymysql.connect(
        host=host, port=port, database=database,
        user=username, password=password,
        charset="utf8mb4", connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        cur = conn.cursor()

        # 날짜 필터 (collected_at 컬럼이 있는 경우)
        where = ""
        params = []
        if date_from or date_to:
            conditions = []
            if date_from:
                conditions.append("collected_at >= %s")
                params.append(date_from)
            if date_to:
                conditions.append("collected_at < %s")
                dt_to = date_to
                try:
                    dt = datetime.fromisoformat(date_to)
                    if dt.hour == 0 and dt.minute == 0:
                        dt += timedelta(days=1)
                    dt_to = dt.isoformat()
                except ValueError:
                    pass
                params.append(dt_to)
            where = " WHERE " + " AND ".join(conditions)

        # 총 건수
        cur.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`{where}", params)
        total = cur.fetchone()["cnt"]

        # 데이터 조회
        offset = (page - 1) * size
        cur.execute(f"SELECT * FROM `{table_name}`{where} ORDER BY id DESC LIMIT %s OFFSET %s",
                    params + [size, offset])
        rows = cur.fetchall()

        columns = list(rows[0].keys()) if rows else []
        items = []
        for r in rows:
            items.append({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()})

        return columns, items, total
    finally:
        conn.close()


def _rdbms_read_pg(host, port, database, username, password,
                    schema, table_name, page, size, date_from, date_to):
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(
        host=host, port=port, dbname=database or "postgres",
        user=username, password=password,
        connect_timeout=10,
    )
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        full_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

        # 날짜 필터
        where = ""
        params = []
        if date_from or date_to:
            conditions = []
            if date_from:
                conditions.append("collected_at >= %s")
                params.append(date_from)
            if date_to:
                dt_to = date_to
                try:
                    dt = datetime.fromisoformat(date_to)
                    if dt.hour == 0 and dt.minute == 0:
                        dt += timedelta(days=1)
                    dt_to = dt.isoformat()
                except ValueError:
                    pass
                conditions.append("collected_at < %s")
                params.append(dt_to)
            where = " WHERE " + " AND ".join(conditions)

        # 총 건수
        cur.execute(f"SELECT COUNT(*) as cnt FROM {full_table}{where}", params)
        total = cur.fetchone()["cnt"]

        # 데이터 조회
        offset = (page - 1) * size
        cur.execute(f"SELECT * FROM {full_table}{where} ORDER BY id DESC LIMIT %s OFFSET %s",
                    params + [size, offset])
        rows = cur.fetchall()

        columns = list(rows[0].keys()) if rows else []
        items = []
        for r in rows:
            items.append({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()})

        return columns, items, total
    finally:
        conn.close()


def _query_pipeline_files(c):
    """File 싱크 — /files API 사용 안내"""
    return _ok({
        "catalog": {"id": c.id, "name": c.name, "tagName": c.tag_name,
                    "connectorType": c.connector_type, "pipelineId": c.pipeline_id},
        "sinkType": "internal_file_sink",
        "useFilesApi": True,
    })


# ──────────────────────────────────────────────
# CAT-009: GET /api/catalog/<id>/data — 정형 시계열 데이터 조회
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/data", methods=["GET"])
def query_catalog_data(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 파일 커넥터는 비정형 → /files API 사용 안내
        if c.connector_type == "file":
            return _err("파일 커넥터는 /api/catalog/<id>/files 를 사용해주세요.", "USE_FILES_API")

        page = request.args.get("page", 1, type=int)
        size = min(request.args.get("size", 100, type=int), 1000)
        date_from = request.args.get("from", "")
        date_to = request.args.get("to", "")
        quality_min = request.args.get("quality_min", 0, type=int)

        # 파이프라인 싱크 카탈로그 → 싱크 타입별 라우팅
        if c.connector_type == "pipeline" and c.sink_type:
            if c.sink_type == "internal_tsdb_sink":
                return _query_pipeline_tsdb(db, c, page, size, date_from, date_to, quality_min)
            elif c.sink_type == "internal_rdbms_sink":
                return _query_pipeline_rdbms(db, c, page, size, date_from, date_to)
            elif c.sink_type == "internal_file_sink":
                return _query_pipeline_files(c)

        # 레시피 카탈로그 → AggregatedData 또는 실시간 실행
        if c.connector_type == "recipe":
            return _query_recipe_data(db, c, page, size)

        is_connector_level = not c.tag_name
        q = db.query(TimeSeriesData).filter(
            TimeSeriesData.connector_type == c.connector_type,
            TimeSeriesData.connector_id == c.connector_id,
        )
        if not is_connector_level:
            q = q.filter(TimeSeriesData.tag_name == c.tag_name)

        if date_from:
            try:
                q = q.filter(TimeSeriesData.timestamp >= datetime.fromisoformat(date_from))
            except ValueError:
                pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                if dt_to.hour == 0 and dt_to.minute == 0:
                    dt_to += timedelta(days=1)
                q = q.filter(TimeSeriesData.timestamp < dt_to)
            except ValueError:
                pass
        if quality_min > 0:
            q = q.filter(TimeSeriesData.quality >= quality_min)

        total = q.count()
        rows = q.order_by(TimeSeriesData.timestamp.desc()).offset((page - 1) * size).limit(size).all()

        # 기간 내 통계
        stats_q = db.query(
            func.count(TimeSeriesData.id),
            func.min(TimeSeriesData.value),
            func.max(TimeSeriesData.value),
            func.avg(TimeSeriesData.value),
            func.min(TimeSeriesData.timestamp),
            func.max(TimeSeriesData.timestamp),
        ).filter(
            TimeSeriesData.connector_type == c.connector_type,
            TimeSeriesData.connector_id == c.connector_id,
        )
        if not is_connector_level:
            stats_q = stats_q.filter(TimeSeriesData.tag_name == c.tag_name)
        if date_from:
            try:
                stats_q = stats_q.filter(TimeSeriesData.timestamp >= datetime.fromisoformat(date_from))
            except ValueError:
                pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                if dt_to.hour == 0 and dt_to.minute == 0:
                    dt_to += timedelta(days=1)
                stats_q = stats_q.filter(TimeSeriesData.timestamp < dt_to)
            except ValueError:
                pass
        sr = stats_q.first()

        return _ok({
            "catalog": {"id": c.id, "name": c.name, "tagName": c.tag_name,
                        "connectorType": c.connector_type, "connectorId": c.connector_id},
            "isConnectorLevel": is_connector_level,
            "items": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
            "stats": {
                "count": sr[0] if sr else 0,
                "min": round(sr[1], 4) if sr and sr[1] is not None else None,
                "max": round(sr[2], 4) if sr and sr[2] is not None else None,
                "avg": round(sr[3], 4) if sr and sr[3] is not None else None,
                "firstAt": sr[4].isoformat() if sr and sr[4] else None,
                "lastAt": sr[5].isoformat() if sr and sr[5] else None,
            },
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-010: GET /api/catalog/<id>/data/export — CSV 내보내기
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/data/export", methods=["GET"])
def export_catalog_data(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)
        if c.connector_type == "file":
            return _err("파일 커넥터는 /api/catalog/<id>/files/download 를 사용해주세요.", "USE_FILES_API")

        date_from = request.args.get("from", "")
        date_to = request.args.get("to", "")
        fmt = request.args.get("format", "csv")

        # 파이프라인 싱크 내보내기 분기
        if c.connector_type == "pipeline" and c.sink_type:
            if c.sink_type == "internal_tsdb_sink":
                return _export_pipeline_tsdb(db, c, date_from, date_to, fmt)
            elif c.sink_type == "internal_rdbms_sink":
                return _export_pipeline_rdbms(db, c, date_from, date_to, fmt)
            elif c.sink_type == "internal_file_sink":
                return _err("파일 싱크는 /api/catalog/<id>/files/download 를 사용해주세요.", "USE_FILES_API")

        # 레시피 카탈로그 내보내기
        if c.connector_type == "recipe":
            return _export_recipe_data(db, c, fmt)

        is_connector_level = not c.tag_name
        q = db.query(TimeSeriesData).filter(
            TimeSeriesData.connector_type == c.connector_type,
            TimeSeriesData.connector_id == c.connector_id,
        )
        if not is_connector_level:
            q = q.filter(TimeSeriesData.tag_name == c.tag_name)
        if date_from:
            try:
                q = q.filter(TimeSeriesData.timestamp >= datetime.fromisoformat(date_from))
            except ValueError:
                pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                if dt_to.hour == 0 and dt_to.minute == 0:
                    dt_to += timedelta(days=1)
                q = q.filter(TimeSeriesData.timestamp < dt_to)
            except ValueError:
                pass

        rows = q.order_by(TimeSeriesData.timestamp.asc()).limit(50000).all()

        fname_prefix = c.tag_name or f"{c.connector_type}_{c.connector_id}"
        if fmt == "json":
            data = [r.to_dict() for r in rows]
            resp = jsonify(data)
            resp.headers["Content-Disposition"] = (
                f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.json"'
            )
            return resp

        # CSV (default)
        output = io.StringIO()
        writer = csv.writer(output)
        if is_connector_level:
            writer.writerow(["timestamp", "tag_name", "value", "value_str", "data_type", "unit", "quality", "measurement"])
        else:
            writer.writerow(["timestamp", "value", "value_str", "data_type", "unit", "quality", "measurement"])
        for r in rows:
            row = [r.timestamp.isoformat() if r.timestamp else ""]
            if is_connector_level:
                row.append(r.tag_name or "")
            row.extend([
                r.value if r.value is not None else "",
                r.value_str or "",
                r.data_type or "",
                r.unit or "",
                r.quality if r.quality is not None else "",
                r.measurement or "",
            ])
            writer.writerow(row)

        csv_bytes = output.getvalue().encode("utf-8-sig")
        return Response(
            csv_bytes,
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.csv"',
            },
        )
    finally:
        db.close()


# ──────────────────────────────────────────────
# 파이프라인 싱크 내보내기 헬퍼
# ──────────────────────────────────────────────

def _export_pipeline_tsdb(db, c, date_from, date_to, fmt):
    """TSDB 싱크 데이터 CSV/JSON 내보내기"""
    q = db.query(TimeSeriesData).filter(
        TimeSeriesData.pipeline_id == c.pipeline_id,
    )
    if c.tag_name:
        q = q.filter(TimeSeriesData.measurement == c.tag_name)
    if date_from:
        try:
            q = q.filter(TimeSeriesData.timestamp >= datetime.fromisoformat(date_from))
        except ValueError:
            pass
    if date_to:
        try:
            dt_to = datetime.fromisoformat(date_to)
            if dt_to.hour == 0 and dt_to.minute == 0:
                dt_to += timedelta(days=1)
            q = q.filter(TimeSeriesData.timestamp < dt_to)
        except ValueError:
            pass

    rows = q.order_by(TimeSeriesData.timestamp.asc()).limit(50000).all()
    fname_prefix = f"pipeline_{c.pipeline_id}_{c.tag_name or 'all'}"

    if fmt == "json":
        data = [r.to_dict() for r in rows]
        resp = jsonify(data)
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.json"'
        )
        return resp

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "measurement", "tag_name", "value", "value_str", "data_type", "unit", "quality"])
    for r in rows:
        writer.writerow([
            r.timestamp.isoformat() if r.timestamp else "",
            r.measurement or "", r.tag_name or "",
            r.value if r.value is not None else "",
            r.value_str or "", r.data_type or "",
            r.unit or "", r.quality if r.quality is not None else "",
        ])

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        csv_bytes, mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.csv"'},
    )


def _export_pipeline_rdbms(db, c, date_from, date_to, fmt):
    """RDBMS 싱크 데이터 CSV/JSON 내보내기"""
    from backend.models.storage import RdbmsConfig
    import logging
    _log = logging.getLogger(__name__)

    cfg = _get_pipeline_sink_config(db, c.pipeline_id, "internal_rdbms_sink")
    rdbms_id = cfg.get("rdbmsId", 0)
    table_name = cfg.get("tableName", "")

    if not rdbms_id or not table_name:
        return _err("RDBMS 싱크 설정을 찾을 수 없습니다.", "CONFIG_NOT_FOUND")

    rdbms = db.query(RdbmsConfig).get(rdbms_id)
    if not rdbms:
        return _err("RDBMS 설정을 찾을 수 없습니다.", "CONFIG_NOT_FOUND")

    db_type = (rdbms.db_type or "").lower()
    try:
        # 전체 데이터 조회 (최대 50000건)
        if "mysql" in db_type or "maria" in db_type:
            columns, items, _ = _rdbms_read_mysql(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                table_name, 1, 50000, date_from, date_to)
        else:
            columns, items, _ = _rdbms_read_pg(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                rdbms.schema_name or "public",
                table_name, 1, 50000, date_from, date_to)
    except Exception as e:
        _log.error("RDBMS 싱크 내보내기 실패: %s", e)
        return _err("외부 DB 연결/조회에 실패했습니다.", "RDBMS_ERROR")

    fname_prefix = f"pipeline_{c.pipeline_id}_{table_name}"

    if fmt == "json":
        resp = jsonify(items)
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.json"'
        )
        return resp

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for item in items:
        writer.writerow([str(item.get(col, "")) if item.get(col) is not None else "" for col in columns])

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        csv_bytes, mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.csv"'},
    )


def _export_recipe_data(db, c, fmt):
    """레시피 카탈로그 데이터 CSV/JSON 내보내기."""
    recipe = db.query(DataRecipe).filter_by(catalog_id=c.id).first()
    if not recipe:
        return _err("연결된 레시피를 찾을 수 없습니다.", "RECIPE_NOT_FOUND", 404)

    if recipe.execution_mode == "view":
        # 실시간 실행 → 결과 내보내기
        from backend.services.recipe_executor import execute_recipe_preview
        result = execute_recipe_preview(db, recipe)
        if "error" in result:
            return _err(result["error"], "QUERY_ERROR")
        columns = result.get("columns", [])
        items = result.get("rows", [])
    else:
        # snapshot → AggregatedData
        rows = (
            db.query(AggregatedData)
            .filter_by(recipe_id=recipe.id)
            .order_by(AggregatedData.row_index)
            .limit(50000)
            .all()
        )
        items = [r.row_data for r in rows]
        columns = list(items[0].keys()) if items else []

    fname_prefix = f"recipe_{recipe.id}_{recipe.name.replace(' ', '_')}"

    if fmt == "json":
        resp = jsonify(items)
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.json"'
        )
        return resp

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    for item in items:
        writer.writerow([str(item.get(col, "")) if item.get(col) is not None else "" for col in columns])

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        csv_bytes, mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname_prefix}_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.csv"'},
    )


# ──────────────────────────────────────────────
# CAT-011: GET /api/catalog/<id>/files — 비정형 파일 목록 (MinIO)
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/files", methods=["GET"])
def list_catalog_files(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        search = request.args.get("search", "").lower()
        date_filter = request.args.get("date", "")

        from minio.error import S3Error
        from backend.config import MINIO_BUCKETS
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)

        # 파이프라인 File 싱크: sink config의 bucket/pathPrefix 사용
        if c.connector_type == "pipeline" and c.sink_type == "internal_file_sink":
            sink_cfg = _get_pipeline_sink_config(db, c.pipeline_id, "internal_file_sink")
            sink_bucket = sink_cfg.get("bucket", "sdl-files")
            prefix = sink_cfg.get("pathPrefix", f"pipeline/{c.pipeline_id}/")
            if prefix and not prefix.endswith("/"):
                prefix += "/"
            bucket = sink_bucket
        elif c.connector_type == "file":
            # 파일 커넥터: raw/{connector_id}/ 경로
            prefix = f"raw/{c.connector_id}/"
        else:
            prefix = f"raw/{c.connector_id}/"

        if date_filter:
            prefix = prefix + date_filter + "/"

        # 파이프라인 File 싱크에서 이미 bucket이 설정된 경우 유지
        if not (c.connector_type == "pipeline" and c.sink_type == "internal_file_sink"):
            bucket = MINIO_BUCKETS[0] if MINIO_BUCKETS else "sdl-files"
        items = []
        try:
            for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
                filename = os.path.basename(obj.object_name)
                if not filename:
                    continue
                if search and search not in filename.lower():
                    continue

                ext = os.path.splitext(filename)[1].lower()
                ftype = _file_type(ext)

                items.append({
                    "name": filename,
                    "objectName": obj.object_name,
                    "bucket": bucket,
                    "type": ftype,
                    "extension": ext,
                    "size": obj.size,
                    "sizeDisplay": _fmt_bytes(obj.size),
                    "path": "/" + os.path.dirname(obj.object_name) + "/",
                    "modifiedAt": obj.last_modified.isoformat() if obj.last_modified else None,
                })
        except S3Error:
            pass

        # pipeline 출력 파일도 포함 (파이프라인 File 싱크 카탈로그는 이미 prefix로 조회했으므로 스킵)
        if not (c.connector_type == "pipeline" and c.sink_type == "internal_file_sink"):
            try:
                for obj in client.list_objects(bucket, prefix="pipeline/", recursive=True):
                    filename = os.path.basename(obj.object_name)
                    if not filename:
                        continue
                    # 이 카탈로그의 커넥터/태그와 관련된 파일만 포함
                    tag_lower = (c.tag_name or "").lower()
                    if tag_lower and tag_lower not in filename.lower():
                        continue
                    if search and search not in filename.lower():
                        continue

                    ext = os.path.splitext(filename)[1].lower()
                    ftype = _file_type(ext)
                    items.append({
                        "name": filename,
                        "objectName": obj.object_name,
                        "bucket": bucket,
                        "type": ftype,
                        "extension": ext,
                        "size": obj.size,
                        "sizeDisplay": _fmt_bytes(obj.size),
                        "path": "/" + os.path.dirname(obj.object_name) + "/",
                        "modifiedAt": obj.last_modified.isoformat() if obj.last_modified else None,
                    })
            except S3Error:
                pass

        items.sort(key=lambda x: x["modifiedAt"] or "", reverse=True)
        total = len(items)
        start = (page - 1) * size
        paged = items[start:start + size]

        return _ok({
            "items": paged,
            "total": total,
            "page": page,
            "size": size,
            "catalog": {"id": c.id, "name": c.name, "connectorType": c.connector_type},
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-012: GET /api/catalog/<id>/files/download — 개별 파일 다운로드
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/files/download", methods=["GET"])
def download_catalog_file(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        object_name = request.args.get("objectName", "")
        bucket = request.args.get("bucket", "")
        if not object_name:
            return _err("다운로드할 파일의 objectName이 필요합니다.", "VALIDATION")

        from minio.error import S3Error
        from backend.config import MINIO_BUCKETS
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)

        if not bucket:
            bucket = MINIO_BUCKETS[0] if MINIO_BUCKETS else "sdl-files"

        try:
            stat = client.stat_object(bucket, object_name)
        except S3Error:
            return _err(f"파일을 찾을 수 없습니다: {object_name}", "NOT_FOUND", 404)

        response = client.get_object(bucket, object_name)
        file_data = response.read()
        response.close()
        response.release_conn()

        filename = os.path.basename(object_name)
        return send_file(
            io.BytesIO(file_data),
            mimetype=stat.content_type or "application/octet-stream",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        return _err(f"다운로드 실패: {e}", "SERVER_ERROR", 500)
    finally:
        db.close()


# ── 유틸리티 ──
_FILE_TYPE_MAP = {
    ".log": "log", ".csv": "csv", ".tsv": "csv", ".jsonl": "data",
    ".json": "data", ".xml": "data",
    ".png": "image", ".jpg": "image", ".jpeg": "image",
    ".gif": "image", ".bmp": "image", ".svg": "image",
    ".pdf": "doc", ".doc": "doc", ".docx": "doc",
    ".xls": "excel", ".xlsx": "excel",
    ".txt": "doc", ".tar": "archive", ".gz": "archive",
    ".zip": "archive",
}


def _file_type(ext):
    return _FILE_TYPE_MAP.get(ext, "other")


def _fmt_bytes(b):
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


# ──────────────────────────────────────────────
# 레시피 카탈로그 데이터 조회 헬퍼
# ──────────────────────────────────────────────

def _query_recipe_data(db, c, page, size):
    """레시피 카탈로그 데이터 조회 — snapshot: AggregatedData, view: 실시간 실행."""
    recipe = db.query(DataRecipe).filter_by(catalog_id=c.id).first()
    if not recipe:
        return _err("연결된 레시피를 찾을 수 없습니다.", "RECIPE_NOT_FOUND", 404)

    if recipe.execution_mode == "view":
        # 실시간 실행
        from backend.services.recipe_executor import execute_recipe_preview
        result = execute_recipe_preview(db, recipe)
        if "error" in result:
            return _err(result["error"], "QUERY_ERROR")
        return _ok({
            "catalog": {"id": c.id, "name": c.name, "connectorType": "recipe"},
            "sinkType": "recipe",
            "executionMode": "view",
            "columns": result.get("columns", []),
            "items": result.get("rows", []),
            "total": result.get("rowCount", 0),
        })

    # snapshot 모드: AggregatedData에서 조회
    q = db.query(AggregatedData).filter_by(recipe_id=recipe.id)
    total = q.count()
    rows = q.order_by(AggregatedData.row_index).offset((page - 1) * size).limit(size).all()

    items = [r.row_data for r in rows]
    columns = list(items[0].keys()) if items else []

    return _ok({
        "catalog": {"id": c.id, "name": c.name, "connectorType": "recipe"},
        "sinkType": "recipe",
        "executionMode": "snapshot",
        "columns": columns,
        "items": items,
        "total": total,
        "lastExecutedAt": recipe.last_executed_at.isoformat() if recipe.last_executed_at else None,
    })


# ══════════════════════════════════════════════
# 데이터 레시피 API
# ══════════════════════════════════════════════

# ── RCP-001: GET /api/catalog/recipe/sources ──
@catalog_bp.route("/recipe/sources", methods=["GET"])
def recipe_sources():
    """레시피 소스 목록 — RDBMS/TSDB 인스턴스 반환."""
    db = _db()
    try:
        from backend.models.storage import RdbmsConfig, TsdbConfig
        rdbms_list = [{"id": r.id, "name": r.name, "dbType": r.db_type,
                       "host": r.host, "port": r.port,
                       "database": r.database_name, "status": r.status}
                      for r in db.query(RdbmsConfig).all()]
        tsdb_list = [{"id": t.id, "name": t.name, "dbType": t.db_type,
                      "host": t.host, "port": t.port,
                      "database": t.database_name, "status": t.status}
                     for t in db.query(TsdbConfig).all()]
        return _ok({"rdbms": rdbms_list, "tsdb": tsdb_list})
    finally:
        db.close()


# ── RCP-002: GET /api/catalog/recipe/tables ──
@catalog_bp.route("/recipe/tables", methods=["GET"])
def recipe_tables():
    """RDBMS/TSDB 테이블 목록 프록시."""
    db = _db()
    try:
        rdbms_id = request.args.get("rdbmsId", 0, type=int)
        tsdb_id = request.args.get("tsdbId", 0, type=int)

        if not rdbms_id and not tsdb_id:
            return _err("rdbmsId 또는 tsdbId가 필요합니다.", "VALIDATION")

        import psycopg2

        if rdbms_id:
            from backend.models.storage import RdbmsConfig
            rdbms = db.query(RdbmsConfig).get(rdbms_id)
            if not rdbms:
                return _err("RDBMS를 찾을 수 없습니다.", "NOT_FOUND", 404)
            conn = psycopg2.connect(
                host=rdbms.host, port=rdbms.port,
                dbname=rdbms.database_name or "postgres",
                user=rdbms.username or "", password=rdbms.password or "",
                connect_timeout=10,
            )
            tables = []
            try:
                schema = rdbms.schema_name or "public"
                cur = conn.cursor()
                cur.execute("""
                    SELECT c.relname, c.reltuples::bigint, pg_total_relation_size(c.oid)
                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relkind = 'r'
                    ORDER BY pg_total_relation_size(c.oid) DESC
                """, (schema,))
                for tbl, rows_est, sz in cur.fetchall():
                    tables.append({
                        "name": tbl,
                        "rowCount": max(rows_est, 0),
                        "size": _fmt_bytes(sz),
                    })
                cur.close()
                conn.close()
            except Exception as e:
                return _err(f"테이블 조회 실패: {e}", "DB_ERROR")
            return _ok(tables)

        else:
            # TSDB: 시계열 데이터는 메인 앱 DB의 time_series_data 테이블에 저장됨
            # measurement 값을 "가상 테이블"로 반환
            from backend.models.storage import TsdbConfig
            tsdb = db.query(TsdbConfig).get(tsdb_id)
            if not tsdb:
                return _err("TSDB를 찾을 수 없습니다.", "NOT_FOUND", 404)
            from sqlalchemy import text as sa_text
            rows = db.execute(sa_text("""
                SELECT measurement, COUNT(*) as cnt
                FROM time_series_data
                WHERE tsdb_id = :tid
                GROUP BY measurement
                ORDER BY cnt DESC
            """), {"tid": tsdb_id}).fetchall()
            tables = []
            for measurement, cnt in rows:
                tables.append({
                    "name": measurement,
                    "rowCount": cnt,
                    "size": "-",
                })
            return _ok(tables)
    finally:
        db.close()


# ── RCP-003: GET /api/catalog/recipe/columns ──
@catalog_bp.route("/recipe/columns", methods=["GET"])
def recipe_columns():
    """RDBMS/TSDB 테이블 컬럼 스키마."""
    db = _db()
    try:
        rdbms_id = request.args.get("rdbmsId", 0, type=int)
        tsdb_id = request.args.get("tsdbId", 0, type=int)
        table = request.args.get("table", "")
        if (not rdbms_id and not tsdb_id) or not table:
            return _err("rdbmsId/tsdbId와 table이 필요합니다.", "VALIDATION")

        import psycopg2

        if rdbms_id:
            from backend.models.storage import RdbmsConfig
            rdbms = db.query(RdbmsConfig).get(rdbms_id)
            if not rdbms:
                return _err("RDBMS를 찾을 수 없습니다.", "NOT_FOUND", 404)
            conn = psycopg2.connect(
                host=rdbms.host, port=rdbms.port,
                dbname=rdbms.database_name or "postgres",
                user=rdbms.username or "", password=rdbms.password or "",
                connect_timeout=10,
            )
            columns = []
            try:
                schema = rdbms.schema_name or "public"
                cur = conn.cursor()
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                for col_name, dtype, nullable in cur.fetchall():
                    columns.append({
                        "name": col_name,
                        "dataType": dtype.upper(),
                        "nullable": nullable == "YES",
                        "isNumeric": dtype in (
                            "integer", "bigint", "smallint", "numeric", "decimal",
                            "real", "double precision", "float",
                        ),
                    })
                cur.close()
                conn.close()
            except Exception as e:
                return _err(f"컬럼 조회 실패: {e}", "DB_ERROR")
            return _ok({"tableName": table, "columns": columns})

        else:
            # TSDB: table 파라미터는 measurement 이름
            # time_series_data 테이블의 주요 컬럼을 반환
            _TSDB_COLUMNS = [
                {"name": "timestamp", "dataType": "TIMESTAMP", "nullable": False, "isNumeric": False},
                {"name": "tag_name", "dataType": "VARCHAR(200)", "nullable": False, "isNumeric": False},
                {"name": "value", "dataType": "DOUBLE PRECISION", "nullable": True, "isNumeric": True},
                {"name": "value_str", "dataType": "TEXT", "nullable": True, "isNumeric": False},
                {"name": "data_type", "dataType": "VARCHAR(30)", "nullable": True, "isNumeric": False},
                {"name": "unit", "dataType": "VARCHAR(50)", "nullable": True, "isNumeric": False},
                {"name": "quality", "dataType": "INTEGER", "nullable": True, "isNumeric": True},
                {"name": "tags", "dataType": "JSON", "nullable": True, "isNumeric": False},
                {"name": "connector_type", "dataType": "VARCHAR(50)", "nullable": True, "isNumeric": False},
                {"name": "connector_id", "dataType": "INTEGER", "nullable": True, "isNumeric": True},
                {"name": "pipeline_id", "dataType": "INTEGER", "nullable": True, "isNumeric": True},
                {"name": "measurement", "dataType": "VARCHAR(200)", "nullable": False, "isNumeric": False},
                {"name": "created_at", "dataType": "TIMESTAMP", "nullable": True, "isNumeric": False},
            ]
            return _ok({"tableName": table, "columns": _TSDB_COLUMNS})
    finally:
        db.close()


# ── RCP-004: GET /api/catalog/recipe/tags ──
@catalog_bp.route("/recipe/tags", methods=["GET"])
def recipe_tags():
    """TSDB measurement의 tag_name 목록."""
    tsdb_id = request.args.get("tsdbId", 0, type=int)
    measurement = request.args.get("measurement", "")
    if not tsdb_id or not measurement:
        return _err("tsdbId와 measurement가 필요합니다.", "VALIDATION")

    db = _db()
    try:
        from sqlalchemy import text as sa_text
        rows = db.execute(sa_text("""
            SELECT tag_name, COUNT(*) as cnt,
                   MIN(value) as min_val, MAX(value) as max_val,
                   MAX(unit) as unit
            FROM time_series_data
            WHERE tsdb_id = :tid AND measurement = :meas
            GROUP BY tag_name
            ORDER BY tag_name
        """), {"tid": tsdb_id, "meas": measurement}).fetchall()
        tags = []
        for tag_name, cnt, min_val, max_val, unit in rows:
            tags.append({
                "name": tag_name,
                "count": cnt,
                "minVal": min_val,
                "maxVal": max_val,
                "unit": unit or "",
            })
        return _ok(tags)
    finally:
        db.close()


# ── RCP-010: GET /api/catalog/recipe ──
@catalog_bp.route("/recipe", methods=["GET"])
def list_recipes():
    """레시피 목록."""
    db = _db()
    try:
        rows = db.query(DataRecipe).order_by(DataRecipe.id.desc()).all()
        return _ok([r.to_dict() for r in rows])
    finally:
        db.close()


# ── RCP-011: POST /api/catalog/recipe ──
@catalog_bp.route("/recipe", methods=["POST"])
def create_recipe():
    """레시피 생성."""
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("레시피 이름이 필요합니다.", "VALIDATION")

        source_type = body.get("sourceType", "")
        if source_type not in ("tsdb", "rdbms"):
            return _err("소스 타입은 tsdb 또는 rdbms여야 합니다.", "VALIDATION")

        recipe = DataRecipe(
            name=name,
            description=body.get("description", ""),
            source_type=source_type,
            rdbms_id=body.get("rdbmsId") if source_type == "rdbms" else None,
            tsdb_id=body.get("tsdbId") if source_type == "tsdb" else None,
            query_mode=body.get("queryMode", "sql"),
            query_sql=body.get("querySql", ""),
            visual_config=body.get("visualConfig", {}),
            execution_mode=body.get("executionMode", "snapshot"),
            owner=body.get("owner", ""),
            category=body.get("category", ""),
        )
        db.add(recipe)
        db.commit()
        db.refresh(recipe)
        return _ok(recipe.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ── RCP-012: GET /api/catalog/recipe/<id> ──
@catalog_bp.route("/recipe/<int:rid>", methods=["GET"])
def get_recipe(rid):
    """레시피 상세."""
    db = _db()
    try:
        r = db.query(DataRecipe).get(rid)
        if not r:
            return _err("레시피를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(r.to_dict())
    finally:
        db.close()


# ── RCP-013: PUT /api/catalog/recipe/<id> ──
@catalog_bp.route("/recipe/<int:rid>", methods=["PUT"])
def update_recipe(rid):
    """레시피 수정."""
    db = _db()
    try:
        r = db.query(DataRecipe).get(rid)
        if not r:
            return _err("레시피를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        for js, col in [("name", "name"), ("description", "description"),
                        ("querySql", "query_sql"), ("queryMode", "query_mode"),
                        ("visualConfig", "visual_config"),
                        ("executionMode", "execution_mode"),
                        ("owner", "owner"), ("category", "category")]:
            if js in body:
                setattr(r, col, body[js])

        db.commit()
        db.refresh(r)
        return _ok(r.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ── RCP-014: DELETE /api/catalog/recipe/<id> ──
@catalog_bp.route("/recipe/<int:rid>", methods=["DELETE"])
def delete_recipe(rid):
    """레시피 삭제 — 관련 카탈로그 + AggregatedData도 삭제."""
    db = _db()
    try:
        r = db.query(DataRecipe).get(rid)
        if not r:
            return _err("레시피를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 관련 카탈로그 삭제
        if r.catalog_id:
            cat = db.query(DataCatalog).get(r.catalog_id)
            if cat:
                db.delete(cat)

        db.delete(r)  # cascade → AggregatedData도 삭제
        db.commit()
        return _ok({"deleted": rid})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ── RCP-020: POST /api/catalog/recipe/<id>/preview ──
@catalog_bp.route("/recipe/<int:rid>/preview", methods=["POST"])
def preview_recipe(rid):
    """레시피 미리보기 — LIMIT 100."""
    db = _db()
    try:
        r = db.query(DataRecipe).get(rid)
        if not r:
            return _err("레시피를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 비주얼 모드 → SQL 변환
        if r.query_mode == "visual" and r.visual_config:
            from backend.services.recipe_executor import build_sql_from_visual
            from backend.models.storage import RdbmsConfig
            db_type = "postgresql"
            if r.source_type == "rdbms" and r.rdbms_id:
                rdbms = db.query(RdbmsConfig).get(r.rdbms_id)
                if rdbms:
                    db_type = "mysql" if "mysql" in (rdbms.db_type or "").lower() else "postgresql"
            try:
                r.query_sql = build_sql_from_visual(r.visual_config, db_type)
                db.commit()
            except ValueError as ve:
                return _err(str(ve), "VALIDATION")

        if not r.query_sql:
            return _err("SQL이 정의되지 않았습니다.", "VALIDATION")

        from backend.services.recipe_executor import execute_recipe_preview
        result = execute_recipe_preview(db, r)
        if "error" in result:
            return _err(result["error"], "QUERY_ERROR")
        return _ok(result)
    finally:
        db.close()


# ── RCP-021: POST /api/catalog/recipe/<id>/execute ──
@catalog_bp.route("/recipe/<int:rid>/execute", methods=["POST"])
def execute_recipe_api(rid):
    """레시피 실행 — 결과 저장 + 카탈로그 생성."""
    db = _db()
    try:
        r = db.query(DataRecipe).get(rid)
        if not r:
            return _err("레시피를 찾을 수 없습니다.", "NOT_FOUND", 404)

        # 비주얼 모드 → SQL 변환
        if r.query_mode == "visual" and r.visual_config:
            from backend.services.recipe_executor import build_sql_from_visual
            from backend.models.storage import RdbmsConfig
            db_type = "postgresql"
            if r.source_type == "rdbms" and r.rdbms_id:
                rdbms = db.query(RdbmsConfig).get(r.rdbms_id)
                if rdbms:
                    db_type = "mysql" if "mysql" in (rdbms.db_type or "").lower() else "postgresql"
            try:
                r.query_sql = build_sql_from_visual(r.visual_config, db_type)
                db.commit()
            except ValueError as ve:
                return _err(str(ve), "VALIDATION")

        if not r.query_sql:
            return _err("SQL이 정의되지 않았습니다.", "VALIDATION")

        from backend.services.recipe_executor import execute_recipe
        result = execute_recipe(db, r)
        if "error" in result:
            return _err(result["error"], "EXECUTION_ERROR")
        return _ok(result)
    finally:
        db.close()


# ── RCP-022: POST /api/catalog/recipe/<id>/refresh ──
@catalog_bp.route("/recipe/<int:rid>/refresh", methods=["POST"])
def refresh_recipe(rid):
    """레시피 스냅샷 갱신 (재실행)."""
    db = _db()
    try:
        r = db.query(DataRecipe).get(rid)
        if not r:
            return _err("레시피를 찾을 수 없습니다.", "NOT_FOUND", 404)
        if r.execution_mode != "snapshot":
            return _err("뷰 모드 레시피는 갱신이 필요 없습니다.", "VALIDATION")

        from backend.services.recipe_executor import execute_recipe
        result = execute_recipe(db, r)
        if "error" in result:
            return _err(result["error"], "EXECUTION_ERROR")
        return _ok(result)
    finally:
        db.close()


# ── RCP-023: POST /api/catalog/recipe/preview-sql ──
@catalog_bp.route("/recipe/preview-sql", methods=["POST"])
def preview_sql_direct():
    """레시피 생성 전 SQL 직접 미리보기."""
    from backend.models.storage import RdbmsConfig, TsdbConfig
    from backend.services.recipe_executor import validate_sql, _connect_rdbms, _connect_tsdb, _execute_sql, _PREVIEW_ROWS
    import re

    body = request.get_json(force=True)
    sql = (body.get("querySql") or "").strip()
    source_type = body.get("sourceType", "rdbms")
    rdbms_id = body.get("rdbmsId")
    tsdb_id = body.get("tsdbId")

    if not sql:
        return _err("SQL이 비어 있습니다.", "VALIDATION")

    ok, err = validate_sql(sql)
    if not ok:
        return _err(err, "VALIDATION")

    # LIMIT 강제
    preview_sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {_PREVIEW_ROWS}', sql, flags=re.IGNORECASE)
    if 'LIMIT' not in preview_sql.upper():
        preview_sql += f'\nLIMIT {_PREVIEW_ROWS}'

    db = _db()
    try:
        if source_type == "rdbms":
            rdbms = db.query(RdbmsConfig).get(rdbms_id) if rdbms_id else None
            if not rdbms:
                return _err("RDBMS 설정을 찾을 수 없습니다.", "NOT_FOUND")
            conn, db_type = _connect_rdbms(rdbms)
        else:
            tsdb = db.query(TsdbConfig).get(tsdb_id) if tsdb_id else None
            if not tsdb:
                return _err("TSDB 설정을 찾을 수 없습니다.", "NOT_FOUND")
            conn = _connect_tsdb(tsdb)
            db_type = "postgresql"

        try:
            columns, items, count, elapsed = _execute_sql(conn, db_type, preview_sql, _PREVIEW_ROWS)
            return _ok({
                "columns": columns,
                "rows": items,
                "rowCount": count,
                "elapsedMs": elapsed,
            })
        finally:
            conn.close()
    except Exception as e:
        return _err(f"쿼리 실행 오류: {e}", "QUERY_ERROR")
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-013: POST /api/catalog/export — 대량 데이터 추출 요청 생성
# ──────────────────────────────────────────────
@catalog_bp.route("/export", methods=["POST"])
def create_export_request():
    from backend.models.dataset import DatasetRequest
    from backend.services import dataset_executor

    db = _db()
    try:
        body = request.get_json(force=True)

        name = (body.get("name") or "").strip()
        if not name:
            return _err("name은 필수 항목입니다.", "VALIDATION")

        # request_id 생성: ds-YYYYMMDD-NNN
        today_str = datetime.utcnow().strftime("%Y%m%d")
        prefix = f"ds-{today_str}-"
        last = (
            db.query(DatasetRequest)
            .filter(DatasetRequest.request_id.like(f"{prefix}%"))
            .order_by(DatasetRequest.id.desc())
            .first()
        )
        if last:
            try:
                seq = int(last.request_id.split("-")[-1]) + 1
            except (ValueError, IndexError):
                seq = 1
        else:
            seq = 1
        request_id = f"{prefix}{seq:03d}"

        # 날짜 파싱
        date_from = None
        date_to = None
        if body.get("dateFrom"):
            try:
                date_from = datetime.fromisoformat(body["dateFrom"])
            except ValueError:
                return _err("dateFrom 형식이 올바르지 않습니다.", "VALIDATION")
        if body.get("dateTo"):
            try:
                date_to = datetime.fromisoformat(body["dateTo"])
            except ValueError:
                return _err("dateTo 형식이 올바르지 않습니다.", "VALIDATION")

        fmt = body.get("format", "csv")
        if fmt not in ("csv", "json"):
            return _err("format은 csv 또는 json만 지원합니다.", "VALIDATION")

        compression = body.get("compression", "none")
        if compression not in ("none", "gzip"):
            return _err("compression은 none 또는 gzip만 지원합니다.", "VALIDATION")

        ds = DatasetRequest(
            request_id=request_id,
            name=name,
            description=body.get("description", ""),
            requested_by=session.get("username", "anonymous"),
            tags=body.get("tags", []),
            connector_types=body.get("connectorTypes", []),
            connector_ids=body.get("connectorIds", []),
            date_from=date_from,
            date_to=date_to,
            quality_min=body.get("qualityMin", 0),
            sampling_method=body.get("samplingMethod", "none"),
            sampling_interval=body.get("samplingInterval", ""),
            sampling_ratio=body.get("samplingRatio", 1.0),
            format=fmt,
            include_metadata=body.get("includeMetadata", True),
            compression=compression,
            status="queued",
            progress=0,
        )
        db.add(ds)
        db.commit()
        db.refresh(ds)

        # 비동기 실행 큐에 추가
        dataset_executor.enqueue(request_id)

        return _ok(ds.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-014: GET /api/catalog/export — 추출 요청 목록
# ──────────────────────────────────────────────
@catalog_bp.route("/export", methods=["GET"])
def list_export_requests():
    from backend.models.dataset import DatasetRequest

    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 20, type=int)
        status_filter = request.args.get("status", "")

        q = db.query(DatasetRequest)
        if status_filter:
            q = q.filter(DatasetRequest.status == status_filter)

        total = q.count()
        rows = q.order_by(DatasetRequest.created_at.desc()).offset((page - 1) * size).limit(size).all()

        # 상태별 집계
        counts = dict(
            db.query(DatasetRequest.status, func.count(DatasetRequest.id))
            .group_by(DatasetRequest.status)
            .all()
        )

        return _ok({
            "items": [r.to_dict() for r in rows],
            "total": total,
            "page": page,
            "size": size,
            "summary": {
                "total": sum(counts.values()),
                "queued": counts.get("queued", 0),
                "processing": counts.get("processing", 0),
                "ready": counts.get("ready", 0),
                "failed": counts.get("failed", 0),
            },
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-015: GET /api/catalog/export/<request_id> — 추출 요청 상세
# ──────────────────────────────────────────────
@catalog_bp.route("/export/<request_id>", methods=["GET"])
def get_export_request(request_id):
    from backend.models.dataset import DatasetRequest

    db = _db()
    try:
        ds = db.query(DatasetRequest).filter_by(request_id=request_id).first()
        if not ds:
            return _err("추출 요청을 찾을 수 없습니다.", "NOT_FOUND", 404)

        result = ds.to_dict()
        # ready 상태일 때 프로파일 데이터 포함
        if ds.status == "ready" and ds.profile:
            result["profile"] = ds.profile

        return _ok(result)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-016: GET /api/catalog/export/<request_id>/download — 파일 다운로드
# ──────────────────────────────────────────────
@catalog_bp.route("/export/<request_id>/download", methods=["GET"])
def download_export_file(request_id):
    from backend.models.dataset import DatasetRequest

    db = _db()
    try:
        ds = db.query(DatasetRequest).filter_by(request_id=request_id).first()
        if not ds:
            return _err("추출 요청을 찾을 수 없습니다.", "NOT_FOUND", 404)
        if ds.status != "ready":
            return _err(
                f"파일이 아직 준비되지 않았습니다. 현재 상태: {ds.status}",
                "NOT_READY", 400,
            )
        if not ds.file_name:
            return _err("파일 정보가 없습니다.", "NO_FILE", 404)

        # 로컬 파일인 경우
        if ds.file_name.startswith("local://"):
            local_path = ds.file_name[len("local://"):]
            if not os.path.exists(local_path):
                return _err("로컬 파일을 찾을 수 없습니다.", "NOT_FOUND", 404)
            return send_file(
                local_path,
                as_attachment=True,
                download_name=os.path.basename(local_path),
            )

        # MinIO에서 다운로드
        from minio import Minio
        from minio.error import S3Error
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)

        bucket = ds.storage_bucket or "sdl-files"
        try:
            client.stat_object(bucket, ds.file_name)
        except S3Error:
            return _err(f"파일을 찾을 수 없습니다: {ds.file_name}", "NOT_FOUND", 404)

        response = client.get_object(bucket, ds.file_name)
        file_data = response.read()
        response.close()
        response.release_conn()

        download_name = os.path.basename(ds.file_name)
        ext = download_name.rsplit(".", 1)[-1].lower() if "." in download_name else ""
        content_type = "application/gzip" if "gz" in ext else (
            "text/csv" if "csv" in ext else (
                "application/json" if "json" in ext else "application/octet-stream"
            )
        )

        return send_file(
            io.BytesIO(file_data),
            mimetype=content_type,
            as_attachment=True,
            download_name=download_name,
        )
    except Exception as e:
        return _err(f"다운로드 실패: {e}", "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-017: DELETE /api/catalog/export/<request_id> — 추출 요청 삭제
# ──────────────────────────────────────────────
@catalog_bp.route("/export/<request_id>", methods=["DELETE"])
def delete_export_request(request_id):
    from backend.models.dataset import DatasetRequest

    db = _db()
    try:
        ds = db.query(DatasetRequest).filter_by(request_id=request_id).first()
        if not ds:
            return _err("추출 요청을 찾을 수 없습니다.", "NOT_FOUND", 404)

        # MinIO에서 파일 삭제 (파일이 존재하는 경우)
        if ds.file_name and ds.status == "ready":
            if ds.file_name.startswith("local://"):
                local_path = ds.file_name[len("local://"):]
                if os.path.exists(local_path):
                    os.remove(local_path)
            else:
                try:
                    from backend.services.minio_client import get_minio_client
                    client = get_minio_client(db)
                    bucket = ds.storage_bucket or "sdl-files"
                    client.remove_object(bucket, ds.file_name)
                except Exception:
                    pass  # 파일 삭제 실패해도 DB 레코드는 삭제 진행

        db.delete(ds)
        db.commit()
        return _ok({"deleted": request_id})
    except Exception as e:
        db.rollback()
        return _err(str(e))
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-018: GET /api/catalog/<id>/api-info — REST API 접근 정보
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/api-info", methods=["GET"])
def get_catalog_api_info(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        base = request.host_url.rstrip("/")
        cat_base = f"{base}/api/catalog"

        endpoints = {
            "detail": {
                "method": "GET",
                "url": f"{cat_base}/{cid}",
                "description": "카탈로그 상세 정보 조회",
                "curl": f'curl -s "{cat_base}/{cid}" | python -m json.tool',
            },
            "data": {
                "method": "GET",
                "url": f"{cat_base}/{cid}/data",
                "description": "시계열 데이터 조회 (정형 데이터)",
                "params": {
                    "page": "페이지 번호 (기본: 1)",
                    "size": "페이지 크기 (기본: 100, 최대: 1000)",
                    "from": "시작 날짜 (ISO 8601, 예: 2024-01-01)",
                    "to": "종료 날짜 (ISO 8601, 예: 2024-12-31)",
                    "quality_min": "최소 품질 점수 (0~100)",
                },
                "curl": (
                    f'curl -s "{cat_base}/{cid}/data?from=2024-01-01&to=2024-12-31&size=100"'
                    " | python -m json.tool"
                ),
            },
            "export": {
                "method": "GET",
                "url": f"{cat_base}/{cid}/data/export",
                "description": "데이터 CSV/JSON 내보내기",
                "params": {
                    "from": "시작 날짜 (ISO 8601)",
                    "to": "종료 날짜 (ISO 8601)",
                    "format": "출력 형식 (csv | json, 기본: csv)",
                },
                "curl": (
                    f'curl -s "{cat_base}/{cid}/data/export?from=2024-01-01&to=2024-12-31&format=csv"'
                    " -o export.csv"
                ),
            },
            "files": {
                "method": "GET",
                "url": f"{cat_base}/{cid}/files",
                "description": "비정형 파일 목록 조회 (MinIO)",
                "params": {
                    "page": "페이지 번호 (기본: 1)",
                    "size": "페이지 크기 (기본: 50)",
                    "search": "파일명 검색어",
                    "date": "날짜 폴더 필터 (예: 2024-01-01)",
                },
                "curl": f'curl -s "{cat_base}/{cid}/files?page=1&size=50" | python -m json.tool',
            },
            "bulkExport": {
                "method": "POST",
                "url": f"{cat_base}/export",
                "description": "대량 데이터 추출 요청 생성 (비동기)",
                "body": {
                    "name": "내보내기 이름",
                    "tags": ["태그1", "태그2"],
                    "connectorTypes": ["opcua"],
                    "connectorIds": [1],
                    "dateFrom": "2024-01-01T00:00:00",
                    "dateTo": "2024-12-31T23:59:59",
                    "format": "csv",
                    "compression": "gzip",
                },
                "curl": (
                    f"curl -s -X POST \"{cat_base}/export\""
                    ' -H "Content-Type: application/json"'
                    " -d '{\"name\": \"my-export\", \"tags\": [\"" + (c.tag_name or "tag1")
                    + "\"], \"format\": \"csv\"}'"
                    " | python -m json.tool"
                ),
            },
        }

        return _ok({
            "catalogId": c.id,
            "catalogName": c.name,
            "connectorType": c.connector_type,
            "connectorId": c.connector_id,
            "tagName": c.tag_name,
            "baseUrl": base,
            "endpoints": endpoints,
        })
    finally:
        db.close()
