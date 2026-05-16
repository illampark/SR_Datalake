"""File Indexer — local_path import collector 의 'import 대기함(inbox)' 인덱스.

MinIO 정본화 이후 file_index 의 역할:
- local_path 는 정본 저장소가 아니라 import 대기함(inbox) — import 가 끝나면
  소스는 post_import_action(archive/delete)로 정리되어 사라진다.
- 따라서 file_index 는 '아직 import 되지 않은 대기 파일' 의 인덱스이며,
  데이터레이크 스토리지 합계(/status, /stats)에는 절대 반영하지 않는다.
- archive_subdir(.imported 등)는 스캔에서 제외 — 이미 import 완료된 파일.

목적:
- /local/browse 응답을 NFS walk 비용 없이 SQL 인덱스 hit 으로 응답 (ms 단위)
- 첫 스캔 + 변경분 스캔 모두 backend 백그라운드에서 처리

multi-worker 안전성:
- file_index_state.is_running 플래그 + DB advisory lock 으로 collector 별 동시 1개 보장
- 모든 worker 가 start_scheduler() 를 호출해도, 같은 collector 를 중복 스캔하지 않음

스케줄링: 모든 등록된 local_path collector 를 INTERVAL_MIN(기본 5분) 마다 순회.
"""
from __future__ import annotations

import logging
import mimetypes
import os
import threading
import time
from datetime import datetime
from typing import Iterable, List

from sqlalchemy import text as _sql_text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from backend.database import SessionLocal, engine
from backend.models.collector import ImportCollector
from backend.models.file_index import FileIndex, FileIndexState

logger = logging.getLogger(__name__)

INTERVAL_MIN = int(os.environ.get("FILE_INDEXER_INTERVAL_MIN", "5"))
BATCH_SIZE = int(os.environ.get("FILE_INDEXER_BATCH_SIZE", "200"))
SCAN_TIMEOUT_SEC = int(os.environ.get("FILE_INDEXER_TIMEOUT_SEC", "1800"))  # 30분
DISABLED = os.environ.get("FILE_INDEXER_DISABLED", "0") == "1"
# import/{cid}/ 객체 목록 캐시를 TTL(storage_file._MINIO_CACHE_TTL_SEC, 기본 30분)
# 만료 전 미리 갱신하는 기준 나이. 이 값보다 오래된 캐시는 워머가 재나열한다.
# 반드시 캐시 TTL 보다 작아야 cold 캐시 윈도우가 생기지 않는다.
IMPORT_WARM_REFRESH_AGE = int(os.environ.get("IMPORT_WARM_REFRESH_AGE_SEC", "1080"))  # 18분
# Leader election advisory-lock key (전역). 한 워커만 lock 을 잡아 스케줄 실행.
LEADER_LOCK_KEY = 0x46494C45494E4458  # 'FILEINDX'

_scheduler_thread = None
_stop_event = None
_leader_conn = None
# 각 collector 의 진행 중 스캔을 호스트 in-process 에서 표시 (선택적 빠른 abort 신호)
_running_scans = {}


# ── 파일 분류 (storage_file._classify 와 호환) ──

def _classify(name: str) -> tuple[str, str]:
    ext = os.path.splitext(name)[1].lower()
    mime, _ = mimetypes.guess_type(name)
    if ext in {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".svg", ".webp", ".ico"}:
        return "image", ext
    if ext in {".pdf"}:
        return "pdf", ext
    if ext in {".csv"}:
        return "csv", ext
    if ext in {".json", ".jsonl", ".ndjson"}:
        return "json", ext
    if ext in {".xlsx", ".xlsm", ".xls"}:
        return "excel", ext
    if ext in {".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar"}:
        return "archive", ext
    if ext in {".log", ".txt", ".tsv", ".md", ".yaml", ".yml", ".xml", ".html", ".htm"}:
        return "text", ext
    return "other", ext


# ── 진입점 ──

def start_scheduler() -> bool:
    """스케줄러 백그라운드 스레드 시작. multi-worker 환경에서 한 워커만
    PG advisory lock 을 잡아 실행한다. (gunicorn 4 worker 모두 호출해도 안전)
    """
    global _scheduler_thread, _stop_event, _leader_conn
    if DISABLED:
        logger.info("file_indexer disabled (FILE_INDEXER_DISABLED=1)")
        return False
    if _scheduler_thread and _scheduler_thread.is_alive():
        return True

    # ── Leader election: 한 워커만 lock 을 끝까지 보유 ──
    # 연결 끊기지 않게 모듈 전역 connection 유지. 다른 worker 가 try_lock 하면 False.
    try:
        raw = engine.raw_connection()
        cur = raw.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (LEADER_LOCK_KEY,))
        got = cur.fetchone()[0]
        if not got:
            cur.close()
            raw.close()
            logger.info("file_indexer not leader (other worker holds lock)")
            return False
        _leader_conn = raw  # close 되지 않도록 보존
    except Exception as e:
        logger.warning("file_indexer leader-election failed: %s", e)
        return False

    _stop_event = threading.Event()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(_stop_event,),
        name="file-indexer-leader",
        daemon=True,
    )
    _scheduler_thread.start()
    logger.info("file_indexer scheduler started (leader, interval=%d min, batch=%d)",
                INTERVAL_MIN, BATCH_SIZE)
    return True


def stop_scheduler() -> None:
    if _stop_event:
        _stop_event.set()


def _scheduler_loop(stop_event: threading.Event) -> None:
    # 부팅 직후 즉시 1회 스캔 (다른 worker 들은 advisory lock 으로 wait → skip)
    try:
        time.sleep(15)  # 부팅 안정화 대기
        scan_all_collectors()
        # MinIO list 캐시도 미리 채움 — UI 요청은 항상 cache hit
        _warmup_minio_cache()
    except Exception as e:
        logger.warning("initial scan failed: %s", e)

    while not stop_event.wait(INTERVAL_MIN * 60):
        try:
            scan_all_collectors()
            _warmup_minio_cache()
        except Exception as e:
            logger.warning("periodic scan failed: %s", e)


def _warmup_minio_cache() -> None:
    """MinIO list 캐시를 미리 채워 사용자 첫 호출이 timeout 받지 않게.
    NFS 위 MinIO 163k 객체 list 가 5~10분 걸리므로 leader-only 백그라운드에서 처리.
    """
    try:
        from backend.routes import storage_file as sf
        t0 = time.time()
        sf._minio_bucket_summary_cached()
        d1 = time.time() - t0
        t0 = time.time()
        for bucket in ("sdl-files", "sdl-archive", "sdl-backup"):
            try:
                sf._minio_browse_cached(bucket, "")
            except Exception:
                pass
        d2 = time.time() - t0
        t0 = time.time()
        n_imp = _warmup_import_objects(sf)
        d3 = time.time() - t0
        logger.info("minio cache warmup: summary=%.1fs browse=%.1fs import_objs=%.1fs(%d개)",
                    d1, d2, d3, n_imp)
    except Exception as e:
        logger.warning("minio cache warmup failed: %s", e)


def _warmup_import_objects(sf) -> int:
    """local_path + target=file collector 들의 import/{cid}/ 객체 목록을 미리 캐시.

    이관 여부 배지(/local/minio-status)·일괄 정리(/local/cleanup-migrated)가 NFS 위
    cold 캐시 LIST(대형 collector 는 수십 초~분) 를 사용자 요청 경로에서 만나지
    않게 한다. TTL 만료 전(IMPORT_WARM_REFRESH_AGE)에 미리 재나열 → 항상 cache hit.

    leader 워커에서만 호출되므로 워커 간 중복 LIST 가 없다.
    반환: 이번에 실제로 재나열한 collector 수.
    """
    db = SessionLocal()
    try:
        from backend.models.collector import ImportCollector
        rows = (db.query(ImportCollector)
                .filter(ImportCollector.source_mode == "local_path").all())
        targets = [(r.target_bucket or "sdl-files", r.id) for r in rows
                   if (r.target_type or "file").lower() == "file"]
    except Exception as e:
        logger.warning("import_objs warmup: collector 조회 실패: %s", e)
        return 0
    finally:
        db.close()

    warmed = 0
    for bucket, cid in targets:
        cached, age = sf._cache_get(f"import_objs:{bucket}:{cid}")
        if cached is not None and age is not None and age < IMPORT_WARM_REFRESH_AGE:
            continue  # 아직 신선 — 재나열 불필요
        try:
            sf._minio_import_objects(bucket, cid, blocking=True, force=True)
            warmed += 1
        except Exception as e:
            logger.warning("import_objs warmup cid=%s 실패: %s", cid, e)
    return warmed


def scan_all_collectors() -> None:
    """모든 local_path 모드 import collector 를 순회 스캔."""
    db = SessionLocal()
    try:
        rows = (
            db.query(ImportCollector)
            .filter(ImportCollector.source_mode == "local_path")
            .filter(ImportCollector.local_path != "")
            .all()
        )
        cids = [r.id for r in rows]
    finally:
        db.close()
    for cid in cids:
        try:
            scan_collector(cid)
        except Exception as e:
            logger.warning("scan_collector(%s) failed: %s", cid, e)


# ── 단일 collector 스캔 ──

def scan_collector(collector_id: int, force: bool = False) -> dict:
    """단일 collector 의 local_path 디렉토리를 walk 하여 file_index 갱신.

    멀티 워커 안전: PG advisory lock (collector_id 기반) 으로 동시 1개 보장.
    리턴: {"scanned": N, "deleted": M, "duration_ms": K, "skipped": bool}
    """
    # advisory lock key — collector_id 만으로 충분
    is_pg = engine.dialect.name == "postgresql"
    db = SessionLocal()
    locked = False
    try:
        if is_pg:
            # try_advisory_lock — 다른 워커가 잡고 있으면 즉시 False
            got = db.execute(_sql_text(
                "SELECT pg_try_advisory_lock(1234567, :cid)"
            ), {"cid": int(collector_id)}).scalar()
            if not got:
                logger.info("collector=%s scan skipped (locked by other worker)", collector_id)
                return {"scanned": 0, "deleted": 0, "duration_ms": 0, "skipped": True}
            locked = True

        ic = db.query(ImportCollector).get(int(collector_id))
        if not ic:
            return {"error": "collector not found"}
        if ic.source_mode != "local_path" or not ic.local_path:
            return {"error": "not a local_path collector"}
        base_path = ic.local_path
        # archive 영역(.imported 등)은 인덱싱 제외 — 이미 import 완료된 파일이라
        # import 대기함(inbox) 뷰·집계에 잡히면 안 됨.
        archive_subdir = (ic.archive_subdir or ".imported")
        if not os.path.isdir(base_path):
            _update_state(db, collector_id,
                          last_error=f"local_path 가 존재하지 않습니다: {base_path}",
                          is_running=False)
            db.commit()
            return {"error": "base_path missing", "path": base_path}

        scan_started = datetime.utcnow()
        _update_state(db, collector_id,
                      last_scan_started_at=scan_started,
                      is_running=True, last_error="")
        db.commit()
        _running_scans[collector_id] = scan_started

        t0 = time.time()
        n_files = 0
        n_dirs = 0
        batch: list[dict] = []

        try:
            for root, dirs, files in os.walk(base_path):
                # archive 영역은 하위 walk 진입 자체를 차단
                if archive_subdir in dirs:
                    dirs.remove(archive_subdir)
                # 디렉토리 entry
                for d in dirs:
                    full = os.path.join(root, d)
                    rel = os.path.relpath(full, base_path).replace(os.sep, "/")
                    parent = os.path.dirname(rel).replace(os.sep, "/")
                    try:
                        st = os.stat(full)
                        mtime = datetime.utcfromtimestamp(st.st_mtime)
                    except OSError:
                        mtime = None
                    batch.append({
                        "collector_id": collector_id,
                        "rel_path": rel,
                        "parent_path": parent,
                        "name": d,
                        "is_dir": True,
                        "ftype": "directory",
                        "extension": "",
                        "size": 0,
                        "modified_at": mtime,
                        "indexed_at": scan_started,
                    })
                    n_dirs += 1
                # 파일 entry
                for f in files:
                    full = os.path.join(root, f)
                    rel = os.path.relpath(full, base_path).replace(os.sep, "/")
                    parent = os.path.dirname(rel).replace(os.sep, "/")
                    try:
                        st = os.stat(full)
                    except OSError:
                        continue
                    ftype, ext = _classify(f)
                    batch.append({
                        "collector_id": collector_id,
                        "rel_path": rel,
                        "parent_path": parent,
                        "name": f,
                        "is_dir": False,
                        "ftype": ftype,
                        "extension": ext,
                        "size": int(st.st_size or 0),
                        "modified_at": datetime.utcfromtimestamp(st.st_mtime),
                        "indexed_at": scan_started,
                    })
                    n_files += 1

                if len(batch) >= BATCH_SIZE:
                    _upsert_batch(db, batch)
                    db.commit()
                    batch.clear()

                # timeout 체크
                if (time.time() - t0) > SCAN_TIMEOUT_SEC:
                    raise TimeoutError(
                        f"scan exceeded SCAN_TIMEOUT_SEC={SCAN_TIMEOUT_SEC}s"
                    )

            if batch:
                _upsert_batch(db, batch)
                db.commit()
                batch.clear()

            # stale row 삭제 — 이번 scan 에서 보이지 않은 record
            deleted = db.execute(_sql_text(
                "DELETE FROM file_index "
                "WHERE collector_id=:cid AND indexed_at < :ts"
            ), {"cid": collector_id, "ts": scan_started}).rowcount or 0

            db.commit()
            duration_ms = int((time.time() - t0) * 1000)
            _update_state(db, collector_id,
                          last_scan_finished_at=datetime.utcnow(),
                          last_scan_files=n_files,
                          last_scan_dirs=n_dirs,
                          last_scan_duration_ms=duration_ms,
                          last_error="",
                          is_running=False)
            db.commit()
            logger.info(
                "file_indexer: collector=%s files=%d dirs=%d deleted=%d duration=%dms",
                collector_id, n_files, n_dirs, deleted, duration_ms,
            )
            return {
                "scanned": n_files, "dirs": n_dirs,
                "deleted": deleted, "duration_ms": duration_ms,
            }
        except Exception as e:
            db.rollback()
            _update_state(db, collector_id,
                          last_scan_finished_at=datetime.utcnow(),
                          last_error=str(e)[:500],
                          is_running=False)
            db.commit()
            logger.warning("scan_collector(%s) error: %s", collector_id, e)
            return {"error": str(e)}
    finally:
        _running_scans.pop(collector_id, None)
        try:
            if locked:
                db.execute(_sql_text(
                    "SELECT pg_advisory_unlock(1234567, :cid)"
                ), {"cid": int(collector_id)})
                db.commit()
        except Exception:
            pass
        db.close()


def _upsert_batch(db, batch: list[dict]) -> None:
    """PG 의 ON CONFLICT 로 batch upsert."""
    if not batch:
        return
    stmt = pg_insert(FileIndex.__table__).values(batch)
    update_cols = {
        "parent_path": stmt.excluded.parent_path,
        "name": stmt.excluded.name,
        "is_dir": stmt.excluded.is_dir,
        "ftype": stmt.excluded.ftype,
        "extension": stmt.excluded.extension,
        "size": stmt.excluded.size,
        "modified_at": stmt.excluded.modified_at,
        "indexed_at": stmt.excluded.indexed_at,
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_file_index_path",
        set_=update_cols,
    )
    db.execute(stmt)


def _update_state(db, collector_id: int, **fields) -> None:
    state = db.query(FileIndexState).get(int(collector_id))
    if not state:
        state = FileIndexState(collector_id=int(collector_id))
        db.add(state)
        db.flush()
    for k, v in fields.items():
        setattr(state, k, v)


# ── 조회 헬퍼 (route 에서 사용) ──

def get_state(collector_id: int) -> dict:
    db = SessionLocal()
    try:
        s = db.query(FileIndexState).get(int(collector_id))
        if not s:
            return {"collectorId": int(collector_id), "indexed": False}
        d = s.to_dict()
        d["indexed"] = bool(s.last_scan_finished_at)
        return d
    finally:
        db.close()
