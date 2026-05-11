"""카탈로그 API — 사용자 수준 데이터 카탈로그 CRUD + 검색 + 데이터 조회/다운로드"""

import csv
import io
import json
import os
from datetime import datetime, timedelta

from flask import Blueprint, request, jsonify, Response, send_file, session, stream_with_context
from sqlalchemy import func

from backend.database import SessionLocal
from backend.models.catalog import DataCatalog, CatalogSearchTag, DataRecipe, AggregatedData
from backend.models.storage import TimeSeriesData
from backend.services.system_settings import get_default_page_size

# 대용량 export — 메모리 적재 없이 chunked HTTP 응답으로 흘려보냄.
# yield_per / server-side cursor 의 fetch 단위. 너무 작으면 RTT 비용, 너무 크면 메모리 ↑.
_EXPORT_CHUNK_ROWS = 10_000

# 안전 상한 — 사용자가 모르고 1억 행 dump 를 trigger 했을 때 보호용. 0=무제한.
# 운영 중 더 큰 값이 필요하면 ?limit=N 으로 오버라이드 가능.
_EXPORT_SAFETY_CAP = 0


def _utf8_bom():
    return "﻿"  # excel 호환


def _stream_csv_response(row_generator, fname_prefix):
    """row_generator: yield 하는 각 항목은 이미 직렬화된 CSV 라인(들)."""
    return Response(
        stream_with_context(row_generator),
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{fname_prefix}_'
                f'{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.csv"'
            ),
            # nginx buffering off — chunked 응답을 즉시 흘려보냄
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
        },
    )


def _stream_json_response(record_generator, fname_prefix):
    """JSON array 를 line 단위 객체로 streaming 출력 (NDJSON 가까움)."""
    def _gen():
        yield "["
        first = True
        for rec in record_generator:
            if first:
                first = False
            else:
                yield ","
            yield json.dumps(rec, ensure_ascii=False, default=str)
        yield "]"
    return Response(
        stream_with_context(_gen()),
        mimetype="application/json; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{fname_prefix}_'
                f'{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.json"'
            ),
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
        },
    )


def _csv_chunk_writer(rows, columns):
    """rows: list[dict] or list[list], columns: list[str]. 한 chunk 의 CSV 텍스트 반환."""
    buf = io.StringIO()
    w = csv.writer(buf)
    for row in rows:
        if isinstance(row, dict):
            w.writerow([_csv_safe(row.get(col)) for col in columns])
        else:
            w.writerow([_csv_safe(v) for v in row])
    return buf.getvalue()


def _csv_safe(v):
    """CSV 셀 한 개 — datetime → ISO, None → 빈 문자열."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _avg_row_bytes(sample_rows, columns):
    """샘플 row 기준 평균 row 바이트 추정. 빈 샘플이면 200 가정."""
    if not sample_rows:
        return 200
    chunk = _csv_chunk_writer(sample_rows, columns).encode("utf-8")
    return max(50, int(len(chunk) / max(1, len(sample_rows))))


def _estimate_seconds(row_count, avg_bytes):
    """경험적 추정 — 100MB/s 처리 가정 + 최소 1초."""
    total_mb = (row_count * avg_bytes) / (1024 * 1024)
    return max(1, int(total_mb / 100))


# RDBMS 카탈로그 데이터셋 범위 조정용 raw WHERE 절 입력 — 키워드 기반 검증.
# read-only 한정으로 운영(sink RDBMS 계정은 SELECT 권한만 권장)되더라도, 자명한 위협
# (스택 statement, DML/DDL, 주석을 통한 우회) 만큼은 입력 단계에서 차단.
_WHERE_MAX_LEN = 2000
_WHERE_BLOCKED_TOKENS = (
    ";", "--", "/*", "*/",
)
import re as _re_where  # noqa: E402

# 단어 경계 기반 — `update_at` 같은 컬럼명과 구분 위해 \b 사용
_WHERE_BLOCKED_KEYWORDS = _re_where.compile(
    r"\b(?:UPDATE|INSERT|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|"
    r"MERGE|COPY|EXEC|EXECUTE|CALL|INTO|VACUUM|ANALYZE|REINDEX|"
    r"DECLARE|BEGIN|COMMIT|ROLLBACK|SAVEPOINT)\b",
    _re_where.IGNORECASE,
)


def _validate_where_clause(s):
    """raw WHERE 입력 검증. 통과: (True, ""), 실패: (False, error_message)."""
    if not s:
        return True, ""
    s = s.strip()
    if not s:
        return True, ""
    if len(s) > _WHERE_MAX_LEN:
        return False, f"WHERE 절은 {_WHERE_MAX_LEN}자 이하여야 합니다."
    for tok in _WHERE_BLOCKED_TOKENS:
        if tok in s:
            return False, f"허용되지 않은 토큰이 포함되어 있습니다: {tok!r}"
    m = _WHERE_BLOCKED_KEYWORDS.search(s)
    if m:
        return False, f"허용되지 않은 SQL 키워드가 포함되어 있습니다: {m.group(0).upper()}"
    # 괄호 균형
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return False, "괄호가 맞지 않습니다."
    if depth != 0:
        return False, "괄호가 맞지 않습니다."
    return True, ""

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
        size = request.args.get("size", get_default_page_size(), type=int)
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
def _summary_minio(db, bucket, prefix):
    if not bucket or not prefix:
        return None
    try:
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)
        n = 0
        size = 0
        for o in client.list_objects(bucket, prefix=prefix, recursive=True):
            n += 1
            size += int(o.size or 0)
            if n >= 50000:
                break
        return {
            "kind": "minio",
            "location": f"s3://{bucket}/{prefix}",
            "bucket": bucket,
            "prefix": prefix,
            "count": n,
            "sizeBytes": size,
            "deletable": True,
        }
    except Exception as e:
        return {"kind": "minio", "location": f"s3://{bucket}/{prefix}",
                "error": str(e), "deletable": False}


def _summary_tsdb(db, kind, **kw):
    from sqlalchemy import text as _sql
    where = []
    params = {}
    if kind == "pipeline":
        where.append("connector_type = 'pipeline' AND connector_id = :cid")
        params["cid"] = kw.get("pipeline_id")
    elif kind == "connector":
        where.append("connector_type = :ctype AND connector_id = :cid")
        params["ctype"] = kw.get("connector_type")
        params["cid"] = kw.get("connector_id")
    elif kind == "tag":
        where.append(
            "connector_type = :ctype AND connector_id = :cid AND tag_name = :tag"
        )
        params["ctype"] = kw.get("connector_type")
        params["cid"] = kw.get("connector_id")
        params["tag"] = kw.get("tag_name")
    where_sql = " AND ".join(where)
    try:
        n = db.execute(_sql(f"SELECT COUNT(*) FROM time_series_data WHERE {where_sql}"),
                        params).scalar()
    except Exception as e:
        return {"kind": "tsdb", "location": "time_series_data",
                "error": str(e), "deletable": False}
    return {
        "kind": "tsdb",
        "location": "time_series_data",
        "filter": where_sql,
        "filterParams": params,
        "count": int(n or 0),
        "deletable": True,
    }


def _summary_rdbms(db, rdbms_id, table_name):
    if not rdbms_id or not table_name:
        return None
    from backend.models.storage import RdbmsConfig
    from backend.routes.storage_rdbms import _pg_connect
    rdbms = db.query(RdbmsConfig).get(rdbms_id)
    if not rdbms:
        return {"kind": "rdbms", "location": f"?/{table_name}",
                "error": "RDBMS instance not found", "deletable": False}
    try:
        conn = _pg_connect(rdbms)
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        n = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {
            "kind": "rdbms",
            "location": f"{rdbms.name}/{table_name}",
            "rdbmsId": rdbms_id,
            "tableName": table_name,
            "count": int(n or 0),
            "deletable": True,
        }
    except Exception as e:
        return {"kind": "rdbms", "location": f"{rdbms.name}/{table_name}",
                "rdbmsId": rdbms_id, "tableName": table_name,
                "error": str(e), "deletable": False}


def _get_data_summary(db, catalog):
    sources = []
    warnings = []

    if (catalog.sink_type or "").startswith("external_"):
        warnings.append(
            "외부 sink — 자동 삭제하지 않음. 외부 시스템에서 직접 정리하세요."
        )

    ctype = catalog.connector_type
    if ctype == "import":
        from backend.models.collector import ImportCollector
        imp = db.query(ImportCollector).get(catalog.connector_id)
        if imp:
            tt = imp.target_type
            if tt == "file":
                src = _summary_minio(db, imp.target_bucket or "sdl-files",
                                     f"import/{imp.id}/")
                if src:
                    sources.append(src)
            elif tt == "tsdb":
                sources.append(_summary_tsdb(db, "connector",
                    connector_type="import", connector_id=imp.id))
            elif tt == "rdbms":
                src = _summary_rdbms(db, imp.target_id, imp.target_table)
                if src:
                    sources.append(src)
        else:
            warnings.append("연결된 import collector 가 이미 존재하지 않습니다.")

    elif ctype == "pipeline":
        from backend.models.pipeline import PipelineStep
        pid = catalog.pipeline_id
        if pid:
            sink_steps = db.query(PipelineStep).filter_by(
                pipeline_id=pid
            ).filter(PipelineStep.module_type.in_([
                "internal_tsdb_sink", "internal_rdbms_sink", "internal_file_sink"
            ])).all()
            for step in sink_steps:
                cfg = step.config or {}
                mt = step.module_type
                if catalog.sink_type and catalog.sink_type != mt:
                    continue
                if mt == "internal_tsdb_sink":
                    sources.append(_summary_tsdb(db, "pipeline", pipeline_id=pid))
                elif mt == "internal_rdbms_sink":
                    src = _summary_rdbms(db, cfg.get("rdbmsId"), cfg.get("tableName"))
                    if src:
                        sources.append(src)
                elif mt == "internal_file_sink":
                    bucket = cfg.get("bucket") or "sdl-files"
                    prefix = (cfg.get("pathPrefix") or "").rstrip("/") + "/"
                    src = _summary_minio(db, bucket, prefix)
                    if src:
                        sources.append(src)

    elif ctype in ("opcua", "modbus", "mqtt", "db", "file", "api"):
        if catalog.tag_name:
            sources.append(_summary_tsdb(db, "tag",
                connector_type=ctype, connector_id=catalog.connector_id,
                tag_name=catalog.tag_name))
        else:
            sources.append(_summary_tsdb(db, "connector",
                connector_type=ctype, connector_id=catalog.connector_id))

    return {
        "catalogId": catalog.id,
        "name": catalog.name,
        "connectorType": ctype,
        "sinkType": catalog.sink_type,
        "tagName": catalog.tag_name,
        "sources": [s for s in sources if s],
        "warnings": warnings,
    }


def _delete_minio_prefix(db, bucket, prefix):
    from backend.services.minio_client import get_minio_client
    client = get_minio_client(db)
    n = 0
    for o in list(client.list_objects(bucket, prefix=prefix, recursive=True)):
        client.remove_object(bucket, o.object_name)
        n += 1
    return n


def _delete_tsdb_rows(db, where_sql, params):
    from sqlalchemy import text as _sql
    res = db.execute(_sql(f"DELETE FROM time_series_data WHERE {where_sql}"),
                     params)
    db.commit()
    return res.rowcount or 0


def _truncate_rdbms_table(db, rdbms_id, table_name):
    from backend.models.storage import RdbmsConfig
    from backend.routes.storage_rdbms import _pg_connect
    rdbms = db.query(RdbmsConfig).get(rdbms_id)
    if not rdbms:
        return 0
    conn = _pg_connect(rdbms)
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        n = int(cur.fetchone()[0] or 0)
        cur.execute(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY')
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return n


def _delete_data_for_catalog(db, catalog):
    # safety: pipeline running 차단
    if catalog.connector_type == "pipeline":
        from backend.models.pipeline import Pipeline
        p = db.query(Pipeline).get(catalog.pipeline_id)
        if p and p.status == "running":
            raise RuntimeError(
                "파이프라인이 실행 중입니다. 정지 후 다시 시도하세요."
            )

    deleted = []
    summary = _get_data_summary(db, catalog)
    for src in summary["sources"]:
        if not src.get("deletable"):
            continue
        kind = src.get("kind")
        try:
            if kind == "minio":
                n = _delete_minio_prefix(db, src["bucket"], src["prefix"])
                deleted.append({"kind": "minio",
                                "location": src["location"], "count": n})
            elif kind == "tsdb":
                n = _delete_tsdb_rows(db, src["filter"], src["filterParams"])
                deleted.append({"kind": "tsdb",
                                "location": "time_series_data", "count": n})
            elif kind == "rdbms":
                n = _truncate_rdbms_table(db,
                                          src.get("rdbmsId"),
                                          src.get("tableName"))
                deleted.append({"kind": "rdbms",
                                "location": src["location"], "count": n})
        except Exception as e:
            deleted.append({"kind": kind, "location": src.get("location"),
                            "error": str(e)})
    return deleted


@catalog_bp.route("/<int:cid>/data-summary", methods=["GET"])
def get_catalog_data_summary(cid):
    """카탈로그가 가리키는 실제 데이터 위치/건수/크기 요약."""
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(_get_data_summary(db, c))
    finally:
        db.close()


@catalog_bp.route("/<int:cid>", methods=["DELETE"])
def delete_catalog(cid):
    """카탈로그 삭제. ?delete_data=true 면 가리키는 실제 데이터도 함께 삭제."""
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        delete_data = (request.args.get("delete_data") or "").lower() in ("1", "true", "yes")

        result = {"deleted": cid, "deletedData": False, "details": [], "cascadeDeleted": 0}

        if delete_data:
            try:
                details = _delete_data_for_catalog(db, c)
                result["deletedData"] = True
                result["details"] = details
            except RuntimeError as e:
                return _err(str(e), "WRONG_STATE", 400)

        # connector-level 카탈로그 (tag_name="") 삭제 시 같은 connector 의 tag-level 카탈로그도 삭제
        if not c.tag_name:
            related = db.query(DataCatalog).filter_by(
                connector_type=c.connector_type,
                connector_id=c.connector_id,
            ).filter(DataCatalog.id != c.id).all()
            for r in related:
                db.delete(r)
                result["cascadeDeleted"] += 1

        db.delete(c)
        db.commit()
        return _ok(result)
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
        # 파이프라인 TSDB sink 는 measurement 단위 묶음 → 여러 tag_name 이 섞여 있으므로
        # 프론트에서 tag 컬럼을 노출해야 한다.
        "showTagColumn": True,
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


def _query_pipeline_rdbms(db, c, page, size, date_from, date_to, filters=None, where_clause=""):
    """RDBMS 싱크 — 외부 DB에서 직접 조회"""
    from backend.models.storage import RdbmsConfig
    import logging
    _log = logging.getLogger(__name__)

    rdbms_id = 0
    table_name = ""

    # Import 카탈로그 → access_url 또는 그룹 카탈로그에서 RDBMS 정보 추출
    if c.connector_type == "import":
        # 자신의 access_url이 rdbms:// 형식이면 직접 추출
        access = c.access_url or ""
        if not access.startswith("rdbms://") and c.connector_id:
            # 태그별 카탈로그 → 그룹 카탈로그(tag_name="")에서 access_url 가져오기
            group_cat = db.query(DataCatalog).filter_by(
                connector_type="import", connector_id=c.connector_id, tag_name=""
            ).first()
            if group_cat and group_cat.access_url:
                access = group_cat.access_url
        if access.startswith("rdbms://"):
            parts = access.replace("rdbms://", "").split("/", 1)
            try:
                rdbms_id = int(parts[0])
            except (ValueError, IndexError):
                pass
            table_name = parts[1] if len(parts) > 1 else ""
    elif c.connector_type != "import":
        # Pipeline 카탈로그 → 싱크 설정에서 추출
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
                table_name, page, size, date_from, date_to,
                where_clause=where_clause)
        else:
            columns, items, total = _rdbms_read_pg(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                rdbms.schema_name or "public",
                table_name, page, size, date_from, date_to,
                filters=filters, where_clause=where_clause)
    except _OffsetTooLargeError as e:
        return _err(str(e), "OFFSET_TOO_LARGE", 400)
    except Exception as e:
        # PostgreSQL statement_timeout → SQLSTATE 57014 (psycopg2 QueryCanceledError)
        msg = str(e)
        if "57014" in msg or "canceling statement" in msg.lower() or "statement timeout" in msg.lower():
            return _err(
                "조회가 30초 안에 완료되지 못했습니다. 기간/조건 필터(WHERE)로 범위를 좁혀주세요.",
                "QUERY_TIMEOUT", 504,
            )
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
                       table_name, page, size, date_from, date_to, where_clause=""):
    import pymysql
    conn = pymysql.connect(
        host=host, port=port, database=database,
        user=username, password=password,
        charset="utf8mb4", connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        cur = conn.cursor()
        # MySQL/MariaDB 에서도 동일한 SELECT timeout — 60s 프록시 타임아웃 전에 컷오프
        try:
            cur.execute(f"SET SESSION MAX_EXECUTION_TIME = {_RDBMS_STATEMENT_TIMEOUT_MS}")
        except Exception:
            pass  # MariaDB 5.x 등 미지원 버전은 무시

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
        if where_clause:
            where = (where + " AND ") if where else " WHERE "
            where += f"({where_clause})"

        # 총 건수
        cur.execute(f"SELECT COUNT(*) as cnt FROM `{table_name}`{where}", params)
        total = cur.fetchone()["cnt"]

        # OFFSET 페이지네이션 가드 (PG 와 동일 패턴 — 마지막 페이지 ASC 플립 + 100K cap)
        forward_offset = (page - 1) * size
        forward_count = max(0, min(size, total - forward_offset))
        if forward_count <= 0:
            return [], [], total

        ascending = forward_offset > total / 2
        if ascending:
            effective_offset = total - forward_offset - forward_count
            order = "ASC"
        else:
            effective_offset = forward_offset
            order = "DESC"

        if effective_offset > _PAGINATION_OFFSET_CAP:
            raise _OffsetTooLargeError(
                f"이 카탈로그는 {total:,} 건이라 {page} 페이지는 OFFSET {effective_offset:,} 가 필요합니다. "
                f"기간/조건 필터(WHERE)로 범위를 좁혀주세요."
            )

        cur.execute(
            f"SELECT * FROM `{table_name}`{where} ORDER BY id {order} LIMIT %s OFFSET %s",
            params + [forward_count, effective_offset],
        )
        rows = list(cur.fetchall())
        if ascending:
            rows.reverse()

        columns = list(rows[0].keys()) if rows else []
        items = []
        for r in rows:
            items.append({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()})

        return columns, items, total
    finally:
        conn.close()


_PAGINATION_OFFSET_CAP = 100_000  # 그 이상 깊이는 OFFSET 비용이 비현실적 — 필터 권장
_RDBMS_STATEMENT_TIMEOUT_MS = 30_000


class _OffsetTooLargeError(Exception):
    """OFFSET cap 초과 — _query_pipeline_rdbms 에서 친화적 에러로 변환."""


def _rdbms_read_pg(host, port, database, username, password,
                    schema, table_name, page, size, date_from, date_to, filters=None,
                    where_clause=""):
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(
        host=host, port=port, dbname=database or "postgres",
        user=username, password=password,
        connect_timeout=10,
    )
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # OFFSET 폭주 시 nginx 60s 프록시 타임아웃 전에 명확한 에러 — 502 방지
        cur.execute(f"SET statement_timeout = {_RDBMS_STATEMENT_TIMEOUT_MS}")
        full_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

        conditions = []
        params = []

        # 날짜 필터 — 날짜 컬럼 자동 감지
        if date_from or date_to:
            date_col = None
            try:
                cur.execute(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    AND column_name IN ('collected_at', 'imported_at', 'created_at', 'timestamp')
                    ORDER BY CASE column_name
                        WHEN 'collected_at' THEN 1 WHEN 'imported_at' THEN 2
                        WHEN 'timestamp' THEN 3 WHEN 'created_at' THEN 4
                    END LIMIT 1
                """, [schema or "public", table_name])
                row = cur.fetchone()
                if row:
                    date_col = row["column_name"]
            except Exception:
                pass

            if date_col:
                if date_from:
                    conditions.append(f'"{date_col}" >= %s')
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
                    conditions.append(f'"{date_col}" < %s')
                    params.append(dt_to)

        # 컬럼 필터 (RDBMS 고급 필터)
        _ALLOWED_OPS = {"=", "!=", ">", "<", ">=", "<=", "LIKE", "IN"}
        if filters and isinstance(filters, list):
            # 테이블 컬럼 목록 조회 (SQL injection 방지)
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, [schema or "public", table_name])
            valid_cols = {r["column_name"] for r in cur.fetchall()}

            for f in filters:
                col = f.get("col", "")
                op = f.get("op", "=")
                val = f.get("val", "")
                if col not in valid_cols or op not in _ALLOWED_OPS or not val:
                    continue
                if op == "IN":
                    # IN (val1, val2, ...) 처리
                    in_vals = [v.strip() for v in val.split(",") if v.strip()]
                    if in_vals:
                        placeholders = ", ".join(["%s"] * len(in_vals))
                        conditions.append(f'"{col}"::text IN ({placeholders})')
                        params.extend(in_vals)
                elif op == "LIKE":
                    conditions.append(f'"{col}"::text LIKE %s')
                    params.append(f"%{val}%")
                else:
                    conditions.append(f'"{col}"::text {op} %s')
                    params.append(val)

        if where_clause:
            conditions.append(f"({where_clause})")
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        # 총 건수
        cur.execute(f"SELECT COUNT(*) as cnt FROM {full_table}{where}", params)
        total = cur.fetchone()["cnt"]

        # ── OFFSET 페이지네이션 가드 ──
        # 1) 마지막 페이지 boundary: forward offset > total/2 이면 ASC 로 뒤집어 가져온 후 reverse
        #    (ORDER BY id DESC 결과의 N번째 ≡ ORDER BY id ASC 결과의 (total - N + 1)번째)
        # 2) 양방향 모두 100K 를 넘으면 거부 — 그 이상은 OFFSET 으로 못 풂. WHERE 필터 사용 안내.
        forward_offset = (page - 1) * size
        forward_count = max(0, min(size, total - forward_offset))
        if forward_count <= 0:
            return [], [], total

        ascending = forward_offset > total / 2
        if ascending:
            backward_offset = total - forward_offset - forward_count
            effective_offset = backward_offset
            order = "ASC"
        else:
            effective_offset = forward_offset
            order = "DESC"

        if effective_offset > _PAGINATION_OFFSET_CAP:
            raise _OffsetTooLargeError(
                f"이 카탈로그는 {total:,} 건이라 {page} 페이지는 OFFSET {effective_offset:,} 가 필요합니다. "
                f"기간/조건 필터(WHERE)로 범위를 좁혀주세요."
            )

        cur.execute(
            f"SELECT * FROM {full_table}{where} ORDER BY id {order} LIMIT %s OFFSET %s",
            params + [forward_count, effective_offset],
        )
        rows = cur.fetchall()
        if ascending:
            rows = list(reversed(rows))

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
        size = min(request.args.get("size", get_default_page_size(), type=int), 1000)
        date_from = request.args.get("from", "")
        date_to = request.args.get("to", "")
        quality_min = request.args.get("quality_min", 0, type=int)
        filters_raw = request.args.get("filters", "")
        rdbms_filters = None
        if filters_raw:
            try:
                rdbms_filters = json.loads(filters_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        where_clause = (request.args.get("where", "") or "").strip()
        if where_clause:
            ok, msg = _validate_where_clause(where_clause)
            if not ok:
                return _err(msg, "INVALID_WHERE")

        # 싱크 타입별 라우팅 (pipeline, import 공통)
        if c.sink_type and c.connector_type in ("pipeline", "import"):
            if c.sink_type == "internal_tsdb_sink":
                return _query_pipeline_tsdb(db, c, page, size, date_from, date_to, quality_min)
            elif c.sink_type == "internal_rdbms_sink":
                return _query_pipeline_rdbms(db, c, page, size, date_from, date_to, rdbms_filters,
                                             where_clause=where_clause)
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
# CAT-010p: GET /api/catalog/<id>/data/export/preview — 사전 견적
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/data/export/preview", methods=["GET"])
def preview_catalog_export(cid):
    """다운로드 전 견적 (행수, 예상 바이트, 예상 시간) 반환.

    실제 export 와 동일한 필터를 받아 COUNT(*) + 샘플 100건으로 평균 row 크기를
    계산. 외부 RDBMS 의 경우 동일 쿼리를 한 번 실행하므로 비용 ↑ 가능.
    """
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)
        if c.connector_type == "file":
            return _err("파일 카탈로그는 견적 대상이 아닙니다.", "USE_FILES_API")

        date_from = request.args.get("from", "")
        date_to = request.args.get("to", "")
        where_clause = (request.args.get("where", "") or "").strip()
        if where_clause:
            ok, msg = _validate_where_clause(where_clause)
            if not ok:
                return _err(msg, "INVALID_WHERE")

        # 파이프라인 RDBMS sink — 외부 DB 카운트 + 샘플
        if c.connector_type == "pipeline" and c.sink_type == "internal_rdbms_sink":
            return _preview_rdbms(db, c, date_from, date_to, where_clause)

        # 파이프라인 TSDB sink — TimeSeriesData 카운트
        if c.connector_type == "pipeline" and c.sink_type == "internal_tsdb_sink":
            q = _build_pipeline_tsdb_query(db, c, date_from, date_to)
            sample = q.limit(100).all()
            # COUNT(*) + ORDER BY 는 PostgreSQL 에서 GROUP BY 오류 → ORDER BY 제거 후 카운트
            row_count = q.order_by(None).with_entities(func.count(TimeSeriesData.id)).scalar() or 0
            cols = ["timestamp", "measurement", "tag_name", "value", "value_str",
                    "data_type", "unit", "quality"]
            sample_dicts = [_pipe_tsdb_row_to_dict(r) for r in sample]
            avg = _avg_row_bytes(sample_dicts, cols)
            est_bytes = row_count * avg
            return _ok({
                "estimatedRows": row_count,
                "estimatedBytes": est_bytes,
                "avgRowBytes": avg,
                "estimatedSeconds": _estimate_seconds(row_count, avg),
                "recommendation": "stream" if est_bytes < 1024**3 else "async-future",
            })

        if c.connector_type == "recipe":
            return _ok({
                "estimatedRows": 0, "estimatedBytes": 0,
                "avgRowBytes": 0, "estimatedSeconds": 1,
                "recommendation": "stream",
                "note": "recipe data — 실제 다운로드 시 즉시 완료",
            })

        # 기본: TimeSeriesData
        q, columns, is_connector_level = _build_timeseries_query(db, c, date_from, date_to)
        sample = q.limit(100).all()
        row_count = q.order_by(None).with_entities(func.count(TimeSeriesData.id)).scalar() or 0
        sample_dicts = [_ts_row_to_dict(r, is_connector_level) for r in sample]
        avg = _avg_row_bytes(sample_dicts, columns)
        est_bytes = row_count * avg
        return _ok({
            "estimatedRows": row_count,
            "estimatedBytes": est_bytes,
            "avgRowBytes": avg,
            "estimatedSeconds": _estimate_seconds(row_count, avg),
            "recommendation": "stream" if est_bytes < 1024**3 else "async-future",
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# CAT-010a: POST /api/catalog/<id>/data/export/async — 비동기 export 큐 등록
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/data/export/async", methods=["POST"])
def queue_catalog_export_async(cid):
    """카탈로그 데이터를 백그라운드에서 추출 → MinIO 적재 → 다운로드 가능 상태로 전환.

    body: {format: "csv"|"json", dateFrom?, dateTo?, name?, description?}
    응답: 생성된 DatasetRequest 정보 (request_id 포함)
    """
    from backend.models.dataset import DatasetRequest
    from backend.services import catalog_export_worker

    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)
        if c.connector_type == "file":
            return _err("파일 카탈로그는 비동기 export 대상이 아닙니다.", "USE_FILES_API")
        if c.connector_type == "recipe":
            return _err("레시피 카탈로그는 즉시 다운로드를 사용해주세요.", "USE_STREAM")

        body = request.get_json(silent=True) or {}
        fmt = (body.get("format") or "csv").lower()
        if fmt not in ("csv", "json"):
            return _err("format은 csv 또는 json만 지원합니다.", "VALIDATION")

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

        where_clause = (body.get("where") or "").strip()
        if where_clause:
            if c.sink_type != "internal_rdbms_sink":
                return _err("WHERE 절 입력은 RDBMS 싱크 카탈로그에서만 사용할 수 있습니다.", "VALIDATION")
            ok, msg = _validate_where_clause(where_clause)
            if not ok:
                return _err(msg, "INVALID_WHERE")

        # request_id 생성: ds-YYYYMMDD-NNN
        today_str = datetime.utcnow().strftime("%Y%m%d")
        prefix = f"ds-{today_str}-"
        last = (
            db.query(DatasetRequest)
            .filter(DatasetRequest.request_id.like(f"{prefix}%"))
            .order_by(DatasetRequest.id.desc())
            .first()
        )
        try:
            seq = int(last.request_id.split("-")[-1]) + 1 if last else 1
        except (ValueError, IndexError):
            seq = 1
        request_id = f"{prefix}{seq:03d}"

        default_name = (
            body.get("name")
            or f"{c.name or f'catalog_{c.id}'} ({datetime.utcnow().strftime('%Y-%m-%d %H:%M')})"
        )

        ds = DatasetRequest(
            request_id=request_id,
            name=default_name[:200],
            description=body.get("description", ""),
            requested_by=session.get("username", "anonymous"),
            catalog_id=cid,
            where_clause=where_clause,
            date_from=date_from,
            date_to=date_to,
            format=fmt,
            include_metadata=False,
            compression="gzip",   # 항상 gzip — 객체 이름은 .csv.gz / .json.gz
            status="queued",
            progress=0,
            storage_bucket=catalog_export_worker.EXPORT_BUCKET,
        )
        db.add(ds)
        db.commit()
        db.refresh(ds)

        catalog_export_worker.enqueue(request_id)
        return _ok(ds.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


def _preview_rdbms(db, c, date_from, date_to, where_clause=""):
    """외부 RDBMS sink 견적 — count + sample."""
    from backend.models.storage import RdbmsConfig
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
            cols, items, total = _rdbms_read_mysql(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                table_name, 1, 100, date_from, date_to, where_clause)
        else:
            cols, items, total = _rdbms_read_pg(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                rdbms.schema_name or "public",
                table_name, 1, 100, date_from, date_to, None, where_clause)
    except Exception as e:
        return _err(f"외부 DB 견적 실패: {e}", "RDBMS_ERROR")

    avg = _avg_row_bytes(items, cols)
    est_bytes = total * avg
    return _ok({
        "estimatedRows": total,
        "estimatedBytes": est_bytes,
        "avgRowBytes": avg,
        "estimatedSeconds": _estimate_seconds(total, avg),
        "recommendation": "stream" if est_bytes < 1024**3 else "async-future",
    })


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

        # 기본: TimeSeriesData 스트리밍 export
        try:
            limit = int(request.args.get("limit", "0") or 0)
        except ValueError:
            limit = 0
        return _stream_timeseries_export(db, c, date_from, date_to, fmt, limit)
    finally:
        db.close()


def _build_timeseries_query(db, c, date_from, date_to):
    """TimeSeriesData 카탈로그용 쿼리 + (header_columns, row_serializer) 반환."""
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
    q = q.order_by(TimeSeriesData.timestamp.asc())
    if is_connector_level:
        columns = ["timestamp", "tag_name", "value", "value_str",
                   "data_type", "unit", "quality", "measurement"]
    else:
        columns = ["timestamp", "value", "value_str",
                   "data_type", "unit", "quality", "measurement"]
    return q, columns, is_connector_level


def _ts_row_to_dict(r, is_connector_level):
    d = {
        "timestamp": r.timestamp.isoformat() if r.timestamp else "",
        "value": r.value if r.value is not None else "",
        "value_str": r.value_str or "",
        "data_type": r.data_type or "",
        "unit": r.unit or "",
        "quality": r.quality if r.quality is not None else "",
        "measurement": r.measurement or "",
    }
    if is_connector_level:
        d["tag_name"] = r.tag_name or ""
    return d


def _stream_timeseries_export(db, c, date_from, date_to, fmt, limit):
    q, columns, is_connector_level = _build_timeseries_query(db, c, date_from, date_to)
    if limit and limit > 0:
        q = q.limit(limit)
    elif _EXPORT_SAFETY_CAP > 0:
        q = q.limit(_EXPORT_SAFETY_CAP)

    fname_prefix = c.tag_name or f"{c.connector_type}_{c.connector_id}"

    if fmt == "json":
        def _json_gen():
            for r in q.yield_per(_EXPORT_CHUNK_ROWS):
                yield _ts_row_to_dict(r, is_connector_level)
        return _stream_json_response(_json_gen(), fname_prefix)

    def _csv_gen():
        # BOM + header
        yield _utf8_bom() + ",".join(columns) + "\n"
        batch = []
        for r in q.yield_per(_EXPORT_CHUNK_ROWS):
            batch.append(_ts_row_to_dict(r, is_connector_level))
            if len(batch) >= _EXPORT_CHUNK_ROWS:
                yield _csv_chunk_writer(batch, columns)
                batch = []
        if batch:
            yield _csv_chunk_writer(batch, columns)
    return _stream_csv_response(_csv_gen(), fname_prefix)


# ──────────────────────────────────────────────
# 파이프라인 싱크 내보내기 헬퍼
# ──────────────────────────────────────────────

def _build_pipeline_tsdb_query(db, c, date_from, date_to):
    q = db.query(TimeSeriesData).filter(TimeSeriesData.pipeline_id == c.pipeline_id)
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
    return q.order_by(TimeSeriesData.timestamp.asc())


def _pipe_tsdb_row_to_dict(r):
    return {
        "timestamp": r.timestamp.isoformat() if r.timestamp else "",
        "measurement": r.measurement or "",
        "tag_name": r.tag_name or "",
        "value": r.value if r.value is not None else "",
        "value_str": r.value_str or "",
        "data_type": r.data_type or "",
        "unit": r.unit or "",
        "quality": r.quality if r.quality is not None else "",
    }


def _export_pipeline_tsdb(db, c, date_from, date_to, fmt):
    """TSDB 싱크 데이터 CSV/JSON 내보내기 (스트리밍)."""
    try:
        limit = int(request.args.get("limit", "0") or 0)
    except ValueError:
        limit = 0

    q = _build_pipeline_tsdb_query(db, c, date_from, date_to)
    if limit and limit > 0:
        q = q.limit(limit)
    elif _EXPORT_SAFETY_CAP > 0:
        q = q.limit(_EXPORT_SAFETY_CAP)

    columns = ["timestamp", "measurement", "tag_name", "value", "value_str",
               "data_type", "unit", "quality"]
    fname_prefix = f"pipeline_{c.pipeline_id}_{c.tag_name or 'all'}"

    if fmt == "json":
        def _json_gen():
            for r in q.yield_per(_EXPORT_CHUNK_ROWS):
                yield _pipe_tsdb_row_to_dict(r)
        return _stream_json_response(_json_gen(), fname_prefix)

    def _csv_gen():
        yield _utf8_bom() + ",".join(columns) + "\n"
        batch = []
        for r in q.yield_per(_EXPORT_CHUNK_ROWS):
            batch.append(_pipe_tsdb_row_to_dict(r))
            if len(batch) >= _EXPORT_CHUNK_ROWS:
                yield _csv_chunk_writer(batch, columns)
                batch = []
        if batch:
            yield _csv_chunk_writer(batch, columns)
    return _stream_csv_response(_csv_gen(), fname_prefix)


def _rdbms_stream_pg(host, port, database, username, password, schema,
                      table_name, date_from, date_to, limit=0, where_clause=""):
    """PostgreSQL named server-side cursor 로 row 스트리밍.

    yield (columns: list[str], None) 처음 1회 → 이후 yield (None, row_dict) 반복.
    where_clause: 검증된 raw WHERE 본문(앞 'WHERE' 제외). 날짜 조건과 AND 로 결합.
    """
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(
        host=host, port=port, dbname=database or "postgres",
        user=username, password=password,
        connect_timeout=10,
    )
    try:
        # 날짜 컬럼 자동 감지
        meta_cur = conn.cursor()
        date_col = None
        try:
            meta_cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                AND column_name IN ('collected_at','imported_at','created_at','timestamp')
                ORDER BY CASE column_name
                    WHEN 'collected_at' THEN 1 WHEN 'imported_at' THEN 2
                    WHEN 'timestamp' THEN 3 WHEN 'created_at' THEN 4
                END LIMIT 1
            """, (schema or "public", table_name))
            row = meta_cur.fetchone()
            if row:
                date_col = row[0]
        finally:
            meta_cur.close()

        full_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        conditions = []
        params = []
        if date_col and date_from:
            conditions.append(f'"{date_col}" >= %s')
            params.append(date_from)
        if date_col and date_to:
            try:
                dt = datetime.fromisoformat(date_to)
                if dt.hour == 0 and dt.minute == 0:
                    dt += timedelta(days=1)
                conditions.append(f'"{date_col}" < %s')
                params.append(dt.isoformat())
            except ValueError:
                pass
        if where_clause:
            conditions.append(f"({where_clause})")
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f'SELECT * FROM {full_table}{where}'
        if date_col:
            sql += f' ORDER BY "{date_col}" ASC'
        if limit and limit > 0:
            sql += f' LIMIT {int(limit)}'

        # named cursor → server-side, 메모리 안 적재
        ss_cur = conn.cursor(name="sdl_export_ss",
                              cursor_factory=psycopg2.extras.RealDictCursor)
        ss_cur.itersize = _EXPORT_CHUNK_ROWS
        try:
            ss_cur.execute(sql, params)
            columns_emitted = False
            for r in ss_cur:
                if not columns_emitted:
                    yield (list(r.keys()), None)
                    columns_emitted = True
                yield (None, dict(r))
            if not columns_emitted:
                # 빈 결과 — 컬럼은 information_schema 에서 별도 조회
                cols_cur = conn.cursor()
                try:
                    cols_cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (schema or "public", table_name))
                    cols = [r[0] for r in cols_cur.fetchall()]
                finally:
                    cols_cur.close()
                yield (cols, None)
        finally:
            ss_cur.close()
    finally:
        conn.close()


def _rdbms_stream_mysql(host, port, database, username, password,
                         table_name, date_from, date_to, limit=0, where_clause=""):
    """MySQL/MariaDB SSCursor 로 row 스트리밍."""
    import pymysql
    import pymysql.cursors
    conn = pymysql.connect(
        host=host, port=port, database=database,
        user=username, password=password,
        charset="utf8mb4", connect_timeout=10,
        cursorclass=pymysql.cursors.SSDictCursor,  # server-side
    )
    try:
        # 날짜 필터 — collected_at 가 있을 때만
        where = ""
        params = []
        if date_from or date_to:
            conds = []
            if date_from:
                conds.append("collected_at >= %s")
                params.append(date_from)
            if date_to:
                try:
                    dt = datetime.fromisoformat(date_to)
                    if dt.hour == 0 and dt.minute == 0:
                        dt += timedelta(days=1)
                    conds.append("collected_at < %s")
                    params.append(dt.isoformat())
                except ValueError:
                    pass
            if conds:
                where = " WHERE " + " AND ".join(conds)
        if where_clause:
            where = (where + " AND ") if where else " WHERE "
            where += f"({where_clause})"
        sql = f"SELECT * FROM `{table_name}`{where}"
        if limit and limit > 0:
            sql += f" LIMIT {int(limit)}"

        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            columns_emitted = False
            while True:
                rows = cur.fetchmany(_EXPORT_CHUNK_ROWS)
                if not rows:
                    break
                if not columns_emitted:
                    yield (list(rows[0].keys()), None)
                    columns_emitted = True
                for r in rows:
                    yield (None, dict(r))
            if not columns_emitted:
                yield ([], None)
        finally:
            cur.close()
    finally:
        conn.close()


def _export_pipeline_rdbms(db, c, date_from, date_to, fmt):
    """RDBMS 싱크 데이터 CSV/JSON 내보내기 (server-side cursor 스트리밍)."""
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

    try:
        limit = int(request.args.get("limit", "0") or 0)
    except ValueError:
        limit = 0
    if not limit and _EXPORT_SAFETY_CAP > 0:
        limit = _EXPORT_SAFETY_CAP

    where_clause = (request.args.get("where", "") or "").strip()
    if where_clause:
        ok, msg = _validate_where_clause(where_clause)
        if not ok:
            return _err(msg, "INVALID_WHERE")

    db_type = (rdbms.db_type or "").lower()
    fname_prefix = f"pipeline_{c.pipeline_id}_{table_name}"

    def _row_iter():
        if "mysql" in db_type or "maria" in db_type:
            yield from _rdbms_stream_mysql(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                table_name, date_from, date_to, limit, where_clause,
            )
        else:
            yield from _rdbms_stream_pg(
                rdbms.host, rdbms.port, rdbms.database_name,
                rdbms.username or "", rdbms.password or "",
                rdbms.schema_name or "public",
                table_name, date_from, date_to, limit, where_clause,
            )

    if fmt == "json":
        def _json_gen():
            try:
                cols = []
                for cols_or_none, row in _row_iter():
                    if cols_or_none is not None:
                        cols = cols_or_none
                        continue
                    yield row
            except Exception as e:
                _log.error("RDBMS 싱크 JSON 스트리밍 실패: %s", e)
        return _stream_json_response(_json_gen(), fname_prefix)

    def _csv_gen():
        try:
            cols = []
            header_written = False
            batch = []
            for cols_or_none, row in _row_iter():
                if cols_or_none is not None:
                    cols = cols_or_none
                    if not header_written:
                        yield _utf8_bom() + ",".join([str(x) for x in cols]) + "\n"
                        header_written = True
                    continue
                batch.append(row)
                if len(batch) >= _EXPORT_CHUNK_ROWS:
                    yield _csv_chunk_writer(batch, cols)
                    batch = []
            if not header_written and cols:
                yield _utf8_bom() + ",".join([str(x) for x in cols]) + "\n"
            if batch:
                yield _csv_chunk_writer(batch, cols)
        except Exception as e:
            _log.error("RDBMS 싱크 CSV 스트리밍 실패: %s", e)

    return _stream_csv_response(_csv_gen(), fname_prefix)


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
    """파일 브라우저 — 디렉토리 구조 탐색 지원

    Query params:
        path: 탐색할 하위 경로 (예: "2026-04-01/")
        search: 파일명 검색 (recursive)
        date_from/date_to: 수정일 필터
        page/size: 페이징
    """
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", get_default_page_size(), type=int)
        search = request.args.get("search", "").lower()
        browse_path = request.args.get("path", "")  # 하위 디렉토리 탐색
        date_from = request.args.get("date_from", "")
        date_to = request.args.get("date_to", "")

        from minio.error import S3Error
        from backend.config import MINIO_BUCKETS
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)

        # base prefix 결정
        if c.connector_type == "pipeline" and c.sink_type == "internal_file_sink":
            sink_cfg = _get_pipeline_sink_config(db, c.pipeline_id, "internal_file_sink")
            bucket = sink_cfg.get("bucket", "sdl-files")
            base_prefix = sink_cfg.get("pathPrefix", f"pipeline/{c.pipeline_id}/")
        elif c.connector_type == "import":
            bucket = MINIO_BUCKETS[0] if MINIO_BUCKETS else "sdl-files"
            base_prefix = f"import/{c.connector_id}/"
        elif c.connector_type == "file":
            bucket = MINIO_BUCKETS[0] if MINIO_BUCKETS else "sdl-files"
            # FileCollector의 targetPathPrefix 사용
            from backend.models.collector import FileCollector as FC
            fc = db.query(FC).get(c.connector_id)
            if fc and fc.target_path_prefix:
                base_prefix = fc.target_path_prefix.replace("{collector_id}", str(c.connector_id)).replace("{date}/", "")
                if not base_prefix.endswith("/"):
                    base_prefix += "/"
                bucket = fc.target_bucket or bucket
            else:
                base_prefix = f"raw/{c.connector_id}/"
        else:
            bucket = MINIO_BUCKETS[0] if MINIO_BUCKETS else "sdl-files"
            base_prefix = f"raw/{c.connector_id}/"

        if base_prefix and not base_prefix.endswith("/"):
            base_prefix += "/"

        # 탐색 경로 적용
        current_prefix = base_prefix + browse_path
        if current_prefix and not current_prefix.endswith("/"):
            current_prefix += "/"

        # 검색 모드: recursive로 전체 검색
        is_search = bool(search)
        use_recursive = is_search

        # 수정일 필터 파싱
        dt_from = dt_to = None
        if date_from:
            try: dt_from = datetime.fromisoformat(date_from)
            except ValueError: pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                if dt_to.hour == 0 and dt_to.minute == 0:
                    dt_to += timedelta(days=1)
            except ValueError: pass

        dirs = []       # 하위 디렉토리 목록
        files = []      # 파일 목록
        seen_dirs = set()

        try:
            for obj in client.list_objects(bucket, prefix=current_prefix, recursive=use_recursive):
                obj_name = obj.object_name

                if not use_recursive and obj.is_dir:
                    # 디렉토리 항목
                    dir_name = obj_name[len(current_prefix):].rstrip("/")
                    if dir_name and dir_name not in seen_dirs:
                        seen_dirs.add(dir_name)
                        dirs.append({
                            "name": dir_name,
                            "type": "directory",
                            "path": browse_path + dir_name + "/",
                        })
                    continue

                filename = os.path.basename(obj_name)
                if not filename:
                    continue
                if search and search not in filename.lower() and search not in obj_name.lower():
                    continue
                if obj.last_modified:
                    mod_naive = obj.last_modified.replace(tzinfo=None)
                    if dt_from and mod_naive < dt_from:
                        continue
                    if dt_to and mod_naive >= dt_to:
                        continue

                ext = os.path.splitext(filename)[1].lower()
                rel_path = obj_name[len(base_prefix):] if obj_name.startswith(base_prefix) else obj_name

                files.append({
                    "name": filename,
                    "objectName": obj_name,
                    "bucket": bucket,
                    "type": _file_type(ext),
                    "extension": ext,
                    "size": obj.size,
                    "sizeDisplay": _fmt_bytes(obj.size),
                    "path": rel_path,
                    "modifiedAt": obj.last_modified.isoformat() if obj.last_modified else None,
                })
        except S3Error:
            pass

        # 디렉토리 정렬 (이름순) + 파일 정렬 (수정일 역순)
        dirs.sort(key=lambda x: x["name"])
        files.sort(key=lambda x: x.get("modifiedAt") or "", reverse=True)

        total = len(files)
        start = (page - 1) * size
        paged_files = files[start:start + size]

        # 빵크럼 경로 생성
        breadcrumb = [{"name": "root", "path": ""}]
        if browse_path:
            parts = browse_path.strip("/").split("/")
            acc = ""
            for p in parts:
                acc += p + "/"
                breadcrumb.append({"name": p, "path": acc})

        return _ok({
            "directories": dirs,
            "items": paged_files,
            "total": total,
            "page": page,
            "size": size,
            "currentPath": browse_path,
            "breadcrumb": breadcrumb,
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


# ──────────────────────────────────────────────
# CAT-013: POST /api/catalog/<id>/files/download-zip — 선택 파일 ZIP 다운로드
# ──────────────────────────────────────────────
@catalog_bp.route("/<int:cid>/files/download-zip", methods=["POST"])
def download_catalog_files_zip(cid):
    db = _db()
    try:
        c = db.query(DataCatalog).get(cid)
        if not c:
            return _err("카탈로그를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(silent=True) or {}
        files = body.get("files", [])  # [{"objectName": "...", "bucket": "..."}]
        if not files:
            return _err("다운로드할 파일을 선택하세요.", "VALIDATION")

        import zipfile
        from backend.services.minio_client import get_minio_client
        from backend.config import MINIO_BUCKETS
        client = get_minio_client(db)
        default_bucket = MINIO_BUCKETS[0] if MINIO_BUCKETS else "sdl-files"

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in files:
                obj_name = f.get("objectName", "")
                bucket = f.get("bucket", "") or default_bucket
                if not obj_name:
                    continue
                try:
                    resp = client.get_object(bucket, obj_name)
                    data = resp.read()
                    resp.close()
                    resp.release_conn()
                    zf.writestr(os.path.basename(obj_name), data)
                except Exception as e:
                    logger.warning(f"ZIP: skip {obj_name}: {e}")

        zip_buffer.seek(0)
        catalog_name = c.name.replace("/", "_").replace(" ", "_")[:30]
        return send_file(
            zip_buffer,
            mimetype="application/zip",
            as_attachment=True,
            download_name=f"{catalog_name}_{len(files)}files.zip",
        )
    except Exception as e:
        return _err(f"ZIP 생성 실패: {e}", "SERVER_ERROR", 500)
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
        size = request.args.get("size", get_default_page_size(), type=int)
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

        # MinIO 에서 스트리밍 다운로드 (수 GB 파일도 메모리에 적재하지 않음)
        from minio.error import S3Error
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)

        bucket = ds.storage_bucket or "sdl-files"
        try:
            stat = client.stat_object(bucket, ds.file_name)
        except S3Error:
            return _err(f"파일을 찾을 수 없습니다: {ds.file_name}", "NOT_FOUND", 404)

        download_name = os.path.basename(ds.file_name)
        download_lc = download_name.lower()
        if download_lc.endswith(".csv.gz") or download_lc.endswith(".json.gz") or download_lc.endswith(".gz"):
            content_type = "application/gzip"
        elif download_lc.endswith(".csv"):
            content_type = "text/csv"
        elif download_lc.endswith(".json"):
            content_type = "application/json"
        else:
            content_type = "application/octet-stream"

        def _stream():
            resp = client.get_object(bucket, ds.file_name)
            try:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk
            finally:
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass

        headers = {
            "Content-Disposition": f'attachment; filename="{download_name}"',
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
        }
        if stat and getattr(stat, "size", None):
            headers["Content-Length"] = str(stat.size)
        return Response(
            stream_with_context(_stream()),
            mimetype=content_type,
            headers=headers,
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
