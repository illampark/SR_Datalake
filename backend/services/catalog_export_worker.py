"""카탈로그 비동기 Export 워커 — 단일 카탈로그의 대용량 데이터를 MinIO 로 gzip 적재.

요청 생성 → 큐 적재 → 백그라운드 스레드가 한 건씩 실행 →
스트리밍 row generator → gzip → os.pipe → MinIO multipart put_object → ready 상태로 전환.

Tier 1 (HTTP streaming) 의 row generator 헬퍼 (`_build_timeseries_query`,
`_build_pipeline_tsdb_query`, `_rdbms_stream_pg`, `_rdbms_stream_mysql`,
`_ts_row_to_dict`, `_pipe_tsdb_row_to_dict`, `_csv_chunk_writer`) 를 그대로 재사용한다.
"""

import gzip
import io
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

EXPORT_BUCKET = "sdl-exports"
_PART_SIZE = 64 * 1024 * 1024   # MinIO multipart 최소 5MB, 64MB 가 안정 + 최대 640GB 객체
_PROGRESS_COMMIT_EVERY_ROWS = 50_000
_PROGRESS_COMMIT_EVERY_SEC = 5.0
_RETENTION_DAYS = 7

_queue = []
_lock = threading.Lock()
_worker_running = False


# ──────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────
def enqueue(request_id):
    with _lock:
        _queue.append(request_id)
    _ensure_worker()


def _ensure_worker():
    global _worker_running
    with _lock:
        if _worker_running:
            return
        _worker_running = True
    t = threading.Thread(target=_worker_loop, daemon=True, name="catalog-export-worker")
    t.start()


def _worker_loop():
    global _worker_running
    while True:
        with _lock:
            if not _queue:
                _worker_running = False
                return
            request_id = _queue.pop(0)
        try:
            _execute(request_id)
        except Exception as e:
            logger.error("catalog-export 실행 실패 [%s]: %s", request_id, e, exc_info=True)
            _mark_failed(request_id, str(e))


# ──────────────────────────────────────────────
# 실행 본체
# ──────────────────────────────────────────────
def _execute(request_id):
    from backend.database import SessionLocal
    from backend.models.dataset import DatasetRequest
    from backend.models.catalog import DataCatalog

    db = SessionLocal()
    try:
        req = db.query(DatasetRequest).filter_by(request_id=request_id).first()
        if not req:
            logger.warning("DatasetRequest 없음 [%s]", request_id)
            return
        if not req.catalog_id:
            _set_failed(db, req, "catalog_id가 설정되지 않은 요청입니다.")
            return

        catalog = db.query(DataCatalog).get(req.catalog_id)
        if not catalog:
            _set_failed(db, req, "대상 카탈로그를 찾을 수 없습니다.")
            return

        req.status = "processing"
        req.started_at = datetime.utcnow()
        req.progress = 1
        db.commit()

        # 견적 — 진행률 계산용 (실패해도 0으로 시작)
        estimated_rows = _estimate_rows(db, catalog, req)

        # row generator 선택
        row_iter, columns_provider = _select_row_iterator(db, catalog, req)
        if row_iter is None:
            _set_failed(db, req, "이 카탈로그 유형의 비동기 export 를 지원하지 않습니다.")
            return

        # MinIO 준비
        from backend.services.minio_client import get_minio_client
        client = get_minio_client(db)
        ensure_export_bucket(client)

        ext = "csv.gz" if req.format == "csv" else "json.gz"
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_name = _sanitize(req.name)[:60] or f"catalog_{catalog.id}"
        object_name = f"catalog_{catalog.id}/{ts}_{safe_name}_{request_id}.{ext}"

        # 진행 상황 공유 변수 (producer 가 업데이트, 별도 progress committer 가 DB 반영)
        state = {
            "rows_written": 0,
            "bytes_written_uncompressed": 0,
            "last_commit_rows": 0,
            "last_commit_at": time.time(),
            "stop": False,
        }

        # ── 1. 데이터 → CSV/JSON 라인 → gzip → pipe → MinIO ──
        r_fd, w_fd = os.pipe()
        producer_err = {"err": None}

        def _producer():
            wf = os.fdopen(w_fd, "wb")
            gz = gzip.GzipFile(fileobj=wf, mode="wb", compresslevel=6)
            try:
                # Header / opening
                if req.format == "csv":
                    cols = columns_provider() or []
                    header_line = _utf8_bom() + ",".join([str(c) for c in cols]) + "\n"
                    gz.write(header_line.encode("utf-8"))
                else:
                    gz.write(b"[")

                first_json = True
                for kind, payload in row_iter():
                    if kind == "columns":
                        # CSV 헤더가 row 도래 전에 알려진 경우 사용. 이미 비어있는 columns_provider 였다면
                        # 여기서 동적으로 헤더를 적는다. (현재 generator 들은 columns_provider 로 충분)
                        continue
                    # payload: dict
                    if req.format == "csv":
                        cols = columns_provider() or list(payload.keys())
                        line = _csv_line(payload, cols)
                        b = line.encode("utf-8")
                        gz.write(b)
                        state["bytes_written_uncompressed"] += len(b)
                    else:
                        if not first_json:
                            gz.write(b",")
                        gz.write(json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"))
                        first_json = False
                    state["rows_written"] += 1

                if req.format == "json":
                    gz.write(b"]")
                gz.close()
            except Exception as e:
                producer_err["err"] = e
                logger.error("catalog-export producer 실패 [%s]: %s", request_id, e, exc_info=True)
            finally:
                try:
                    gz.close()
                except Exception:
                    pass
                try:
                    wf.close()
                except Exception:
                    pass

        prod_thread = threading.Thread(target=_producer, daemon=True, name=f"export-producer-{request_id}")
        prod_thread.start()

        # 진행률 커밋용 별도 스레드 (DB 세션 분리)
        def _progress_committer():
            from backend.database import SessionLocal as _S
            db2 = _S()
            try:
                while not state["stop"]:
                    time.sleep(2.0)
                    if state["stop"]:
                        break
                    rows = state["rows_written"]
                    if rows == state["last_commit_rows"]:
                        continue
                    state["last_commit_rows"] = rows
                    state["last_commit_at"] = time.time()
                    try:
                        r2 = db2.query(DatasetRequest).filter_by(request_id=request_id).first()
                        if not r2:
                            return
                        r2.total_rows = rows
                        if estimated_rows > 0:
                            # 80% 까지 row 진행률 + 마지막 20% 는 업로드 마감
                            r2.progress = min(80, int((rows / estimated_rows) * 80))
                        else:
                            r2.progress = min(80, int(rows / 100_000))
                        db2.commit()
                    except Exception:
                        db2.rollback()
            finally:
                db2.close()

        prog_thread = threading.Thread(target=_progress_committer, daemon=True, name=f"export-progress-{request_id}")
        prog_thread.start()

        rf = os.fdopen(r_fd, "rb")
        upload_err = None
        try:
            client.put_object(
                EXPORT_BUCKET,
                object_name,
                rf,
                length=-1,
                part_size=_PART_SIZE,
                content_type=("application/gzip" if req.format == "csv" else "application/gzip"),
            )
        except Exception as e:
            upload_err = e
            logger.error("catalog-export MinIO 업로드 실패 [%s]: %s", request_id, e, exc_info=True)
        finally:
            try:
                rf.close()
            except Exception:
                pass
            state["stop"] = True
            prod_thread.join(timeout=60)
            prog_thread.join(timeout=10)

        if producer_err["err"] is not None:
            _set_failed(db, req, f"데이터 스트리밍 실패: {producer_err['err']}")
            _cleanup_object(client, object_name)
            return
        if upload_err is not None:
            _set_failed(db, req, f"MinIO 업로드 실패: {upload_err}")
            _cleanup_object(client, object_name)
            return

        # ── 2. 메타데이터 갱신 ──
        try:
            stat = client.stat_object(EXPORT_BUCKET, object_name)
            file_size = stat.size or 0
        except Exception:
            file_size = 0

        req.status = "ready"
        req.progress = 100
        req.total_rows = state["rows_written"]
        req.file_size_bytes = file_size
        req.file_name = object_name
        req.storage_bucket = EXPORT_BUCKET
        req.completed_at = datetime.utcnow()
        req.expires_at = datetime.utcnow() + timedelta(days=_RETENTION_DAYS)
        req.error_message = ""
        db.commit()
        logger.info(
            "catalog-export ready [%s] rows=%d size=%d bytes object=%s",
            request_id, state["rows_written"], file_size, object_name,
        )
    finally:
        db.close()


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def ensure_export_bucket(client):
    """sdl-exports 버킷이 없으면 생성."""
    try:
        if not client.bucket_exists(EXPORT_BUCKET):
            client.make_bucket(EXPORT_BUCKET)
    except Exception as e:
        logger.warning("sdl-exports 버킷 확인/생성 실패: %s", e)


def cleanup_expired_exports():
    """expires_at 이 지난 ready 상태의 export 파일을 MinIO 에서 제거하고 expired 로 표기.

    retention 스케줄러가 호출하기 위한 진입점. 호출 시점의 실패는 다음 주기로 미룬다.
    """
    from backend.database import SessionLocal
    from backend.models.dataset import DatasetRequest
    from backend.services.minio_client import get_minio_client

    db = SessionLocal()
    try:
        now = datetime.utcnow()
        rows = (
            db.query(DatasetRequest)
            .filter(DatasetRequest.status == "ready")
            .filter(DatasetRequest.expires_at != None)  # noqa: E711
            .filter(DatasetRequest.expires_at < now)
            .filter(DatasetRequest.storage_bucket == EXPORT_BUCKET)
            .all()
        )
        if not rows:
            return 0
        try:
            client = get_minio_client(db)
        except Exception as e:
            logger.warning("retention: MinIO 클라이언트 가져오기 실패: %s", e)
            return 0
        removed = 0
        for r in rows:
            try:
                if r.file_name:
                    client.remove_object(EXPORT_BUCKET, r.file_name)
            except Exception as e:
                logger.warning("retention: 객체 삭제 실패 %s: %s", r.file_name, e)
            r.status = "expired"
            removed += 1
        db.commit()
        if removed:
            logger.info("catalog-export retention: %d개 만료 처리", removed)
        return removed
    finally:
        db.close()


def _select_row_iterator(db, catalog, req):
    """카탈로그 유형별 (row_generator, columns_provider) 반환.

    row_generator: 호출 시 yield (kind, payload) — kind 는 'row' 또는 'columns'
                   현재 모든 generator 는 row 만 yield, header 는 columns_provider 로 전달.
    columns_provider: 호출하면 list[str] 반환 (CSV 헤더용). 사전 알려지지 않으면 None.
    """
    from backend.routes import catalog as catalog_routes

    date_from = req.date_from.isoformat() if req.date_from else ""
    date_to = req.date_to.isoformat() if req.date_to else ""

    # 1) 파이프라인 RDBMS 싱크
    if catalog.connector_type == "pipeline" and catalog.sink_type == "internal_rdbms_sink":
        from backend.models.storage import RdbmsConfig
        cfg = catalog_routes._get_pipeline_sink_config(db, catalog.pipeline_id, "internal_rdbms_sink")
        rdbms_id = cfg.get("rdbmsId", 0)
        table_name = cfg.get("tableName", "")
        if not rdbms_id or not table_name:
            return None, None
        rdbms = db.query(RdbmsConfig).get(rdbms_id)
        if not rdbms:
            return None, None
        db_type = (rdbms.db_type or "").lower()

        cols_holder = {"cols": None}

        def _gen():
            if "mysql" in db_type or "maria" in db_type:
                stream = catalog_routes._rdbms_stream_mysql(
                    rdbms.host, rdbms.port, rdbms.database_name,
                    rdbms.username or "", rdbms.password or "",
                    table_name, date_from, date_to, 0,
                )
            else:
                stream = catalog_routes._rdbms_stream_pg(
                    rdbms.host, rdbms.port, rdbms.database_name,
                    rdbms.username or "", rdbms.password or "",
                    rdbms.schema_name or "public",
                    table_name, date_from, date_to, 0,
                )
            for cols_or_none, row in stream:
                if cols_or_none is not None:
                    cols_holder["cols"] = cols_or_none
                    yield ("columns", cols_or_none)
                    continue
                yield ("row", row)

        def _cols():
            return cols_holder["cols"]

        return _gen, _cols

    # 2) 파이프라인 TSDB 싱크
    if catalog.connector_type == "pipeline" and catalog.sink_type == "internal_tsdb_sink":
        cols = ["timestamp", "measurement", "tag_name", "value", "value_str",
                "data_type", "unit", "quality"]
        q = catalog_routes._build_pipeline_tsdb_query(db, catalog, date_from, date_to)

        def _gen():
            for r in q.yield_per(10_000):
                yield ("row", catalog_routes._pipe_tsdb_row_to_dict(r))

        return _gen, (lambda: cols)

    # 3) 기본: TimeSeriesData (커넥터/태그 단위)
    if catalog.connector_type != "file" and catalog.connector_type != "recipe":
        q, cols, is_connector_level = catalog_routes._build_timeseries_query(
            db, catalog, date_from, date_to
        )

        def _gen():
            for r in q.yield_per(10_000):
                yield ("row", catalog_routes._ts_row_to_dict(r, is_connector_level))

        return _gen, (lambda: cols)

    return None, None


def _estimate_rows(db, catalog, req):
    """진행률 계산용 추정 행수. 실패하면 0 반환.

    카운트 쿼리는 별도 세션에서 실행한다 — PostgreSQL 에서 ORDER BY 가 붙은 query 의
    카운트가 실패하면 호출 세션이 InFailedSqlTransaction 으로 오염되기 때문.
    """
    from backend.database import SessionLocal
    db2 = SessionLocal()
    try:
        from sqlalchemy import func
        from backend.models.storage import TimeSeriesData
        from backend.routes import catalog as catalog_routes

        date_from = req.date_from.isoformat() if req.date_from else ""
        date_to = req.date_to.isoformat() if req.date_to else ""

        if catalog.connector_type == "pipeline" and catalog.sink_type == "internal_tsdb_sink":
            q = catalog_routes._build_pipeline_tsdb_query(db2, catalog, date_from, date_to)
            return q.order_by(None).with_entities(func.count(TimeSeriesData.id)).scalar() or 0

        if catalog.connector_type == "pipeline" and catalog.sink_type == "internal_rdbms_sink":
            # 외부 DB 카운트는 비용 ↑ — preview 가 미리 했더라도 worker 시점에 다시 부르긴 무거움.
            # 0 반환 시 producer 가 rows/100k 기반 fallback 진행률 사용
            return 0

        if catalog.connector_type not in ("file", "recipe"):
            q, _, _ = catalog_routes._build_timeseries_query(db2, catalog, date_from, date_to)
            return q.order_by(None).with_entities(func.count(TimeSeriesData.id)).scalar() or 0
    except Exception as e:
        logger.warning("estimate_rows 실패 [%s]: %s", req.request_id, e)
        try:
            db2.rollback()
        except Exception:
            pass
    finally:
        db2.close()
    return 0


def _csv_line(row_dict, columns):
    """단일 row 를 CSV 라인 1개로 직렬화 (개행 포함)."""
    from backend.routes.catalog import _csv_chunk_writer
    return _csv_chunk_writer([row_dict], columns)


def _utf8_bom():
    return "﻿"


def _sanitize(name):
    bad = '/\\?%*:|"<>'
    out = "".join("_" if c in bad else c for c in (name or ""))
    return out.replace(" ", "_").strip("._")


def _set_failed(db, req, msg):
    req.status = "failed"
    req.error_message = msg[:1000]
    req.completed_at = datetime.utcnow()
    db.commit()
    logger.warning("catalog-export 실패 [%s]: %s", req.request_id, msg)


def _mark_failed(request_id, error_msg):
    from backend.database import SessionLocal
    from backend.models.dataset import DatasetRequest
    db = SessionLocal()
    try:
        req = db.query(DatasetRequest).filter_by(request_id=request_id).first()
        if req:
            req.status = "failed"
            req.error_message = error_msg[:1000]
            req.completed_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()


def _cleanup_object(client, object_name):
    try:
        client.remove_object(EXPORT_BUCKET, object_name)
    except Exception:
        pass


def resume_pending_on_startup():
    """프로세스 재시작 시 queued / processing 상태인 요청을 다시 큐에 적재.

    processing 인 요청은 worker 가 끊긴 것으로 가정하고 queued 로 되돌린 뒤 재실행한다.
    """
    from backend.database import SessionLocal
    from backend.models.dataset import DatasetRequest
    db = SessionLocal()
    try:
        rows = (
            db.query(DatasetRequest)
            .filter(DatasetRequest.catalog_id != None)  # noqa: E711
            .filter(DatasetRequest.status.in_(["queued", "processing"]))
            .all()
        )
        for r in rows:
            r.status = "queued"
            r.progress = 0
            r.error_message = ""
        db.commit()
        for r in rows:
            enqueue(r.request_id)
        if rows:
            logger.info("catalog-export resume: %d 건을 다시 큐에 적재", len(rows))
    except Exception as e:
        logger.warning("resume_pending_on_startup 실패: %s", e)
    finally:
        db.close()
