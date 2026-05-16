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
