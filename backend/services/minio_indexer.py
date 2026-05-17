"""MinIO Object Indexer — Bucket Notification(webhook) 이벤트를 minio_object 에 반영.

설계: claudedocs/minio-event-index-design.md
- handle_events(): webhook 페이로드(S3 이벤트 JSON)를 파싱해 upsert/delete.
- backfill() / reconcile() 는 후속 Phase 에서 추가.

기존 '주기적 전체 LIST + 파일 캐시' 를 대체 — MinIO 가 변경분만 push 하고
minio_object 테이블이 실시간 인덱스가 된다.
"""
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

from backend.database import SessionLocal
from backend.models.minio_object import MinioObject
from backend.services.file_indexer import _classify

logger = logging.getLogger(__name__)


def _parse_key(object_name):
    """객체 키 → (name, parent_path, ftype, extension). parent_path 는 끝 '/' 없음."""
    object_name = (object_name or "").lstrip("/")
    name = os.path.basename(object_name)
    parent = os.path.dirname(object_name)
    ftype, ext = _classify(name)
    return name, parent, ftype, ext


def _parse_event_time(s):
    """S3 eventTime(ISO8601) → naive UTC datetime. 실패 시 None."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def handle_events(payload):
    """MinIO webhook 페이로드(S3 이벤트 JSON)를 minio_object 에 반영.

    s3:ObjectCreated:* → upsert, s3:ObjectRemoved:* → delete.
    같은 객체에 더 최신(또는 동일) sequencer 가 이미 반영됐으면 생성 이벤트는
    skip — 이벤트 순서 역전 방어. 불확실분은 reconcile 이 정정.

    반환: {"applied", "deleted", "skipped"}.
    """
    records = (payload or {}).get("Records") or []
    applied = deleted = skipped = 0
    if not records:
        return {"applied": 0, "deleted": 0, "skipped": 0}
    db = SessionLocal()
    try:
        for rec in records:
            try:
                ev = rec.get("eventName", "") or ""
                s3 = rec.get("s3", {}) or {}
                bucket = (s3.get("bucket") or {}).get("name") or ""
                obj = s3.get("object") or {}
                key = unquote_plus(obj.get("key", "") or "")
                if not bucket or not key:
                    skipped += 1
                    continue

                if ev.startswith("s3:ObjectRemoved"):
                    deleted += (db.query(MinioObject)
                                  .filter_by(bucket=bucket, object_name=key).delete())
                    continue
                if not ev.startswith("s3:ObjectCreated"):
                    skipped += 1
                    continue

                name, parent, ftype, ext = _parse_key(key)
                if not name:                      # 디렉토리 마커(키가 '/' 로 끝남) — 무시
                    skipped += 1
                    continue
                seq = (obj.get("sequencer") or "")[:40]
                row = (db.query(MinioObject)
                         .filter_by(bucket=bucket, object_name=key).one_or_none())
                if row is not None and seq and row.event_seq and seq <= row.event_seq:
                    skipped += 1                  # 이미 더 최신 이벤트 반영됨
                    continue
                vals = dict(
                    name=name, parent_path=parent, ftype=ftype, extension=ext,
                    size=int(obj.get("size") or 0),
                    etag=(obj.get("eTag") or "")[:64],
                    content_type=(obj.get("contentType") or "")[:160],
                    last_modified=_parse_event_time(rec.get("eventTime")),
                    event_seq=seq, indexed_at=datetime.utcnow(),
                )
                if row is None:
                    db.add(MinioObject(bucket=bucket, object_name=key, **vals))
                else:
                    for k, v in vals.items():
                        setattr(row, k, v)
                applied += 1
            except Exception as e:
                logger.warning("minio event 처리 실패: %s", e)
                skipped += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return {"applied": applied, "deleted": deleted, "skipped": skipped}


def backfill(buckets=None, batch_size=1000):
    """기존 MinIO 객체를 전체 LIST 해 minio_object 에 1회 적재 (Phase 4).

    웹훅(Phase 3)이 켜진 뒤 실행 — 백필 도중 도착한 이벤트와 겹쳐도 안전하다:
    handle_events 는 SELECT-후-upsert 라 더 신선한 이벤트 데이터가 우선되고,
    backfill 은 ON CONFLICT DO NOTHING 으로 이미 인덱싱된 객체를 건드리지 않는다.

    느린 작업(수만~수십만 객체 LIST) — HTTP 요청이 아닌 운영 명령/백그라운드로 실행.
    반환: {bucket: 적재 건수}.
    """
    from backend.config import MINIO_BUCKETS
    from backend.services.minio_client import get_minio_client
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    buckets = buckets or MINIO_BUCKETS
    result = {}
    db = SessionLocal()
    try:
        client = get_minio_client(db)
        now = datetime.utcnow()
        for bucket in buckets:
            n = 0
            batch = []

            def _flush():
                nonlocal n
                if not batch:
                    return
                db.execute(pg_insert(MinioObject).values(batch)
                           .on_conflict_do_nothing(index_elements=["bucket", "object_name"]))
                db.commit()
                n += len(batch)
                batch.clear()

            try:
                for obj in client.list_objects(bucket, recursive=True):
                    key = obj.object_name or ""
                    if key.endswith("/"):
                        continue
                    name, parent, ftype, ext = _parse_key(key)
                    if not name:
                        continue
                    lm = obj.last_modified
                    if lm is not None and getattr(lm, "tzinfo", None) is not None:
                        lm = lm.astimezone(timezone.utc).replace(tzinfo=None)
                    batch.append(dict(
                        bucket=bucket, object_name=key, name=name,
                        parent_path=parent, ftype=ftype, extension=ext,
                        size=int(obj.size or 0),
                        etag=(obj.etag or "").strip('"')[:64],
                        content_type="", last_modified=lm,
                        event_seq="", indexed_at=now,
                    ))
                    if len(batch) >= batch_size:
                        _flush()
                _flush()
            except Exception as e:
                db.rollback()
                logger.warning("backfill bucket=%s 실패: %s", bucket, e)
            result[bucket] = n
            logger.info("backfill bucket=%s: %d 객체 적재", bucket, n)
    finally:
        db.close()
    return result
