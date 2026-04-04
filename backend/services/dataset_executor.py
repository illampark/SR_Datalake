"""데이터셋 추출 실행기 — 백그라운드 스레드에서 대량 데이터 쿼리 → 파일 생성 → MinIO 업로드"""

import csv
import gzip
import io
import json
import logging
import random
import threading
import time
from datetime import datetime, timedelta

from sqlalchemy import func

logger = logging.getLogger(__name__)

# 실행 큐
_queue = []
_lock = threading.Lock()
_worker_running = False


def enqueue(request_id):
    """추출 요청을 큐에 추가"""
    with _lock:
        _queue.append(request_id)
    _ensure_worker()


def _ensure_worker():
    """워커 스레드가 없으면 시작"""
    global _worker_running
    if _worker_running:
        return
    _worker_running = True
    t = threading.Thread(target=_worker_loop, daemon=True)
    t.start()


def _worker_loop():
    """큐에서 요청을 꺼내 순차 실행"""
    global _worker_running
    while True:
        request_id = None
        with _lock:
            if _queue:
                request_id = _queue.pop(0)
        if request_id is None:
            _worker_running = False
            return
        try:
            _execute(request_id)
        except Exception as e:
            logger.error("Dataset executor error [%s]: %s", request_id, e, exc_info=True)
            _mark_failed(request_id, str(e))


def _execute(request_id):
    """단일 데이터셋 추출 실행"""
    from backend.database import SessionLocal
    from backend.models.dataset import DatasetRequest
    from backend.models.storage import TimeSeriesData

    db = SessionLocal()
    try:
        req = db.query(DatasetRequest).filter_by(request_id=request_id).first()
        if not req:
            return

        req.status = "processing"
        req.started_at = datetime.utcnow()
        req.progress = 0
        db.commit()

        # ── 1. 쿼리 구성 ──
        q = db.query(TimeSeriesData)

        # 태그 필터
        tags = req.tags or []
        if tags:
            q = q.filter(TimeSeriesData.tag_name.in_(tags))

        # 커넥터 타입 필터
        c_types = req.connector_types or []
        if c_types:
            q = q.filter(TimeSeriesData.connector_type.in_(c_types))

        # 커넥터 ID 필터
        c_ids = req.connector_ids or []
        if c_ids:
            q = q.filter(TimeSeriesData.connector_id.in_(c_ids))

        # 기간 필터
        if req.date_from:
            q = q.filter(TimeSeriesData.timestamp >= req.date_from)
        if req.date_to:
            dt_to = req.date_to
            if dt_to.hour == 0 and dt_to.minute == 0:
                dt_to += timedelta(days=1)
            q = q.filter(TimeSeriesData.timestamp < dt_to)

        # 품질 필터
        if req.quality_min and req.quality_min > 0:
            q = q.filter(TimeSeriesData.quality >= req.quality_min)

        q = q.order_by(TimeSeriesData.timestamp.asc())

        # ── 2. 총 행 수 파악 ──
        total_count = q.count()
        req.progress = 5
        db.commit()

        if total_count == 0:
            req.status = "ready"
            req.progress = 100
            req.total_rows = 0
            req.completed_at = datetime.utcnow()
            req.profile = {"message": "No data found for the given criteria"}
            db.commit()
            return

        # ── 3. 스트리밍 추출 (청크 단위) ──
        CHUNK_SIZE = 10000
        output_buffer = io.StringIO() if req.format != "json" else None
        json_rows = [] if req.format == "json" else None
        csv_writer = None
        if req.format == "csv":
            csv_writer = csv.writer(output_buffer)
            csv_writer.writerow([
                "timestamp", "tag_name", "value", "value_str",
                "data_type", "unit", "quality",
                "connector_type", "connector_id", "measurement",
            ])

        # 프로파일 수집용
        profile_data = {}
        written_rows = 0
        offset = 0

        # 다운샘플링 설정
        interval_seconds = _parse_interval(req.sampling_interval) if req.sampling_method == "downsample" else 0
        last_timestamps = {}  # tag_name별 마지막 기록 시각

        while offset < total_count:
            chunk = q.offset(offset).limit(CHUNK_SIZE).all()
            if not chunk:
                break

            for row in chunk:
                # 다운샘플링: 간격 이내 데이터 건너뛰기
                if req.sampling_method == "downsample" and interval_seconds > 0:
                    tag_key = row.tag_name
                    if tag_key in last_timestamps:
                        delta = (row.timestamp - last_timestamps[tag_key]).total_seconds()
                        if delta < interval_seconds:
                            continue
                    last_timestamps[tag_key] = row.timestamp

                # 랜덤 샘플링
                if req.sampling_method == "random":
                    ratio = req.sampling_ratio or 1.0
                    if random.random() > ratio:
                        continue

                # 행 기록
                if req.format == "csv":
                    csv_writer.writerow([
                        row.timestamp.isoformat() if row.timestamp else "",
                        row.tag_name,
                        row.value if row.value is not None else "",
                        row.value_str or "",
                        row.data_type or "",
                        row.unit or "",
                        row.quality if row.quality is not None else "",
                        row.connector_type or "",
                        row.connector_id or "",
                        row.measurement or "",
                    ])
                elif req.format == "json":
                    json_rows.append(row.to_dict())

                # 프로파일 수집
                _update_profile(profile_data, row)
                written_rows += 1

            offset += CHUNK_SIZE
            req.progress = min(5 + int((offset / total_count) * 85), 90)
            db.commit()

        # ── 4. 파일 생성 ──
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_name = req.name.replace(" ", "_").replace("/", "_")[:50]

        if req.format == "csv":
            file_content = output_buffer.getvalue().encode("utf-8-sig")
            ext = "csv"
        elif req.format == "json":
            file_content = json.dumps(json_rows, ensure_ascii=False, default=str).encode("utf-8")
            ext = "json"
        else:
            file_content = output_buffer.getvalue().encode("utf-8-sig") if output_buffer else b""
            ext = "csv"

        # 압축
        if req.compression == "gzip":
            file_content = gzip.compress(file_content)
            ext += ".gz"

        file_name = f"dataset/{ts}_{safe_name}.{ext}"
        file_size = len(file_content)

        # ── 5. MinIO 업로드 ──
        req.progress = 92
        db.commit()

        try:
            from backend.services.minio_client import get_minio_client
            client = get_minio_client(db)

            bucket = "sdl-files"
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)

            client.put_object(
                bucket, file_name,
                io.BytesIO(file_content), length=file_size,
                content_type=_content_type(ext),
            )
            uploaded_to_minio = True
        except Exception as e:
            logger.warning("MinIO upload failed, saving locally: %s", e)
            uploaded_to_minio = False
            # MinIO 실패 시 로컬 저장
            import os
            local_dir = os.path.join(os.path.dirname(__file__), "..", "..", "dataset_exports")
            os.makedirs(local_dir, exist_ok=True)
            local_path = os.path.join(local_dir, os.path.basename(file_name))
            with open(local_path, "wb") as f:
                f.write(file_content)
            file_name = f"local://{local_path}"

        # ── 6. 프로파일 생성 ──
        profile_summary = _finalize_profile(profile_data, written_rows)

        # ── 7. 메타데이터 포함 ──
        if req.include_metadata:
            from backend.models.metadata import TagMetadata
            tag_meta = {}
            meta_rows = db.query(TagMetadata).filter(
                TagMetadata.tag_name.in_(tags) if tags else True
            ).all()
            for m in meta_rows:
                tag_meta[m.tag_name] = {
                    "unit": m.unit,
                    "dataType": m.data_type,
                    "connectorType": m.connector_type,
                    "connectorName": m.connector_name,
                    "qualityScore": m.quality_score,
                    "sampleCount": m.sample_count,
                }
            profile_summary["tagMetadata"] = tag_meta

        # ── 8. 완료 ──
        req.status = "ready"
        req.progress = 100
        req.total_rows = written_rows
        req.file_size_bytes = file_size
        req.file_name = file_name
        req.storage_bucket = "sdl-files" if uploaded_to_minio else "local"
        req.profile = profile_summary
        req.completed_at = datetime.utcnow()
        req.expires_at = datetime.utcnow() + timedelta(days=7)
        db.commit()

        logger.info("Dataset [%s] ready: %d rows, %s", request_id, written_rows, _fmt_bytes(file_size))

    except Exception as e:
        logger.error("Dataset execution failed [%s]: %s", request_id, e, exc_info=True)
        try:
            req.status = "failed"
            req.error_message = str(e)[:1000]
            req.completed_at = datetime.utcnow()
            db.commit()
        except Exception:
            pass
        raise
    finally:
        db.close()


def _mark_failed(request_id, error_msg):
    """에러 발생 시 상태 업데이트"""
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


def _parse_interval(interval_str):
    """간격 문자열 → 초 변환 (1s, 10s, 1min, 5min, 1h 등)"""
    if not interval_str:
        return 0
    s = interval_str.strip().lower()
    try:
        if s.endswith("h"):
            return int(s[:-1]) * 3600
        elif s.endswith("min"):
            return int(s[:-3]) * 60
        elif s.endswith("m") and not s.endswith("min"):
            return int(s[:-1]) * 60
        elif s.endswith("s"):
            return int(s[:-1])
        else:
            return int(s)
    except (ValueError, IndexError):
        return 0


def _update_profile(profile_data, row):
    """행 단위로 프로파일 데이터 누적"""
    tag = row.tag_name
    if tag not in profile_data:
        profile_data[tag] = {
            "count": 0, "null_count": 0,
            "min": None, "max": None, "sum": 0.0,
            "unit": row.unit or "", "data_type": row.data_type or "",
        }
    p = profile_data[tag]
    p["count"] += 1
    if row.value is None:
        p["null_count"] += 1
    else:
        v = row.value
        if p["min"] is None or v < p["min"]:
            p["min"] = v
        if p["max"] is None or v > p["max"]:
            p["max"] = v
        p["sum"] += v


def _finalize_profile(profile_data, total_rows):
    """프로파일 데이터 → 요약 결과"""
    columns = {}
    for tag, p in profile_data.items():
        numeric_count = p["count"] - p["null_count"]
        columns[tag] = {
            "count": p["count"],
            "unit": p["unit"],
            "dataType": p["data_type"],
            "min": round(p["min"], 6) if p["min"] is not None else None,
            "max": round(p["max"], 6) if p["max"] is not None else None,
            "mean": round(p["sum"] / numeric_count, 6) if numeric_count > 0 else None,
            "nullRatio": round(p["null_count"] / p["count"], 4) if p["count"] > 0 else 0,
        }
    return {
        "totalRows": total_rows,
        "tagCount": len(profile_data),
        "columns": columns,
    }


def _content_type(ext):
    """확장자 → Content-Type"""
    if "csv" in ext:
        return "text/csv"
    elif "json" in ext:
        return "application/json"
    elif "gz" in ext:
        return "application/gzip"
    return "application/octet-stream"


def _fmt_bytes(b):
    if not b:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"
