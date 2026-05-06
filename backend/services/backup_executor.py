"""
Backup / Restore Executor.

Backup targets:
  1. PostgreSQL (sdl) -- pg_dump / pg_restore subprocess
  2. MinIO buckets (sdl-files, sdl-archive) -- tar.gz via minio SDK
  3. Config files (config.py 등) -- file tarball
"""

import io
import os
import shutil
import tarfile
import time
import logging
import subprocess
from datetime import datetime
from urllib.parse import urlparse

from minio.error import S3Error

from backend.database import SessionLocal
from backend.models.backup import BackupHistory
from backend.config import DATABASE_URL
from backend.services.minio_client import get_minio_client

logger = logging.getLogger(__name__)

BACKUP_BUCKET = "sdl-backup"
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_CONFIG_FILES = [
    "backend/config.py",
]


# ── Public API ──────────────────────────────────────


def execute_backup(targets, backup_type="manual"):
    """백업 메인 진입점. 백그라운드 스레드에서 호출."""
    db = SessionLocal()
    t0 = time.time()
    total_bytes = 0
    status = "success"
    error_msg = ""

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    storage_key = f"backups/{timestamp}/"

    # BackupHistory 레코드 생성 (running)
    history = BackupHistory(
        execution_time=datetime.utcnow(),
        backup_type=backup_type,
        operation="backup",
        targets=",".join(targets),
        storage_key=storage_key,
        status="running",
    )
    try:
        db.add(history)
        db.commit()
        db.refresh(history)
        history_id = history.id
    except Exception as e:
        logger.error("백업 이력 생성 실패: %s", e)
        db.rollback()
        db.close()
        return None
    finally:
        db.close()

    try:
        client = _get_minio_client()
        _ensure_bucket(client, BACKUP_BUCKET)

        if "postgresql" in targets:
            total_bytes += _backup_postgresql(client, storage_key)

        if "minio" in targets:
            total_bytes += _backup_minio_buckets(client, storage_key)

        if "config" in targets:
            total_bytes += _backup_config_files(client, storage_key)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("백업 실패: %s", e)

    duration = round(time.time() - t0, 1)

    # 이력 업데이트
    db = SessionLocal()
    try:
        h = db.query(BackupHistory).filter(BackupHistory.id == history_id).first()
        if h:
            h.status = status
            h.total_bytes = total_bytes
            h.duration_seconds = duration
            h.error_message = error_msg
            db.commit()
    except Exception as e:
        logger.error("백업 이력 업데이트 실패: %s", e)
        db.rollback()
    finally:
        db.close()

    logger.info("백업 완료 [%s] targets=%s bytes=%d duration=%.1fs",
                status, targets, total_bytes, duration)
    return history_id


def execute_restore(backup_id, targets):
    """복원 메인 진입점. 백그라운드 스레드에서 호출."""
    db = SessionLocal()
    t0 = time.time()
    total_bytes = 0
    status = "success"
    error_msg = ""

    # 원본 백업 조회
    try:
        src = db.query(BackupHistory).filter(
            BackupHistory.id == backup_id,
            BackupHistory.status == "success",
            BackupHistory.operation == "backup",
        ).first()
        if not src:
            logger.error("복원 대상 백업 없음: id=%s", backup_id)
            return None
        storage_key = src.storage_key
        src_targets = src.targets.split(",") if src.targets else []
    finally:
        db.close()

    # 실제 타겟 필터
    if not targets:
        targets = src_targets
    targets = [t for t in targets if t in src_targets]

    # 복원 이력 생성
    db = SessionLocal()
    history = BackupHistory(
        execution_time=datetime.utcnow(),
        backup_type="manual",
        operation="restore",
        targets=",".join(targets),
        storage_key=storage_key,
        status="running",
        restore_from_id=backup_id,
    )
    try:
        db.add(history)
        db.commit()
        db.refresh(history)
        history_id = history.id
    except Exception as e:
        logger.error("복원 이력 생성 실패: %s", e)
        db.rollback()
        db.close()
        return None
    finally:
        db.close()

    try:
        client = _get_minio_client()

        if "postgresql" in targets:
            total_bytes += _restore_postgresql(client, storage_key)

        if "minio" in targets:
            total_bytes += _restore_minio_buckets(client, storage_key)

        if "config" in targets:
            total_bytes += _restore_config_files(client, storage_key)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("복원 실패: %s", e)

    duration = round(time.time() - t0, 1)

    db = SessionLocal()
    try:
        h = db.query(BackupHistory).filter(BackupHistory.id == history_id).first()
        if h:
            h.status = status
            h.total_bytes = total_bytes
            h.duration_seconds = duration
            h.error_message = error_msg
            db.commit()
    except Exception as e:
        logger.error("복원 이력 업데이트 실패: %s", e)
        db.rollback()
    finally:
        db.close()

    logger.info("복원 완료 [%s] targets=%s duration=%.1fs", status, targets, duration)
    return history_id


def get_backup_storage_info():
    """sdl-backup 버킷의 사용량 정보 반환."""
    try:
        client = _get_minio_client()
        if not client.bucket_exists(BACKUP_BUCKET):
            return {"totalBytes": 0, "objectCount": 0}

        total = 0
        count = 0
        for obj in client.list_objects(BACKUP_BUCKET, recursive=True):
            total += obj.size or 0
            count += 1
        return {"totalBytes": total, "objectCount": count}
    except Exception as e:
        logger.error("스토리지 정보 조회 실패: %s", e)
        return {"totalBytes": 0, "objectCount": 0, "error": str(e)}


def delete_backup_objects(storage_key):
    """MinIO에서 특정 백업의 오브젝트 삭제."""
    try:
        client = _get_minio_client()
        if not client.bucket_exists(BACKUP_BUCKET):
            return 0
        objects = list(client.list_objects(BACKUP_BUCKET, prefix=storage_key, recursive=True))
        for obj in objects:
            client.remove_object(BACKUP_BUCKET, obj.object_name)
        return len(objects)
    except Exception as e:
        logger.error("백업 오브젝트 삭제 실패: %s", e)
        return 0


# ── PostgreSQL ──────────────────────────────────────


def _parse_db_url(url):
    """SQLAlchemy URL에서 접속 정보 추출."""
    # postgresql+psycopg2://user:pass@host:port/dbname
    clean = url.replace("postgresql+psycopg2://", "postgresql://")
    parsed = urlparse(clean)
    return {
        "host": parsed.hostname or "localhost",
        "port": str(parsed.port or 5432),
        "user": parsed.username or "postgres",
        "password": parsed.password or "",
        "dbname": parsed.path.lstrip("/") or "postgres",
    }


def _backup_postgresql(client, storage_key):
    """pg_dump로 PostgreSQL 백업 → MinIO 업로드."""
    pg_dump = shutil.which("pg_dump")
    if not pg_dump:
        raise RuntimeError("pg_dump 바이너리를 찾을 수 없습니다.")

    info = _parse_db_url(DATABASE_URL)
    env = os.environ.copy()
    env["PGPASSWORD"] = info["password"]

    result = subprocess.run(
        [pg_dump, "--format=custom", "--compress=6",
         "-h", info["host"], "-p", info["port"],
         "-U", info["user"], info["dbname"]],
        capture_output=True, env=env, timeout=600,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"pg_dump 실패 (rc={result.returncode}): {stderr}")

    dump_data = result.stdout
    obj_name = storage_key + "pg_sdl.dump"
    client.put_object(
        BACKUP_BUCKET, obj_name,
        io.BytesIO(dump_data), length=len(dump_data),
        content_type="application/octet-stream",
    )
    logger.info("PostgreSQL 백업 완료: %s (%d bytes)", obj_name, len(dump_data))
    return len(dump_data)


def _restore_postgresql(client, storage_key):
    """MinIO에서 dump 다운로드 → pg_restore."""
    pg_restore = shutil.which("pg_restore")
    if not pg_restore:
        raise RuntimeError("pg_restore 바이너리를 찾을 수 없습니다.")

    obj_name = storage_key + "pg_sdl.dump"
    response = client.get_object(BACKUP_BUCKET, obj_name)
    dump_data = response.read()
    response.close()
    response.release_conn()

    info = _parse_db_url(DATABASE_URL)
    env = os.environ.copy()
    env["PGPASSWORD"] = info["password"]

    result = subprocess.run(
        [pg_restore, "--clean", "--if-exists", "--no-owner",
         "-h", info["host"], "-p", info["port"],
         "-U", info["user"], "-d", info["dbname"]],
        input=dump_data, capture_output=True, env=env, timeout=600,
    )
    # pg_restore는 --clean 사용 시 warning이 있을 수 있으므로 rc != 0이어도 로그만
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")[:500]
        logger.warning("pg_restore 경고: %s", stderr)

    logger.info("PostgreSQL 복원 완료: %s (%d bytes)", obj_name, len(dump_data))
    return len(dump_data)


# ── MinIO Buckets ───────────────────────────────────


def _backup_minio_buckets(client, storage_key):
    """sdl-files + sdl-archive 버킷을 tar.gz로 백업.

    DEPRECATED — 더 이상 호출되지 않는다. 다음 이유로 비활성화 (route 단계에서 차단):
    - 모든 객체를 BytesIO 에 통째 적재 → 대용량 시 OOM
    - 백업 산출물을 같은 MinIO 인스턴스(sdl-backup)에 저장 → 재해 복구 가치 0
    - Python 단일 스레드 스트리밍 → 매우 느림

    실제 MinIO 데이터 보호는 다음 중 하나로 처리한다:
      1) MinIO server-side replication (별도 MinIO 또는 S3)
      2) `mc mirror` 외부 destination (NFS / 별도 MinIO)
      3) 호스트 파일시스템 스냅샷 (Restic / BorgBackup, /opt/sdl-data/minio 대상)
    """
    buf = io.BytesIO()
    total = 0

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for bucket_name in ("sdl-files", "sdl-archive"):
            if not client.bucket_exists(bucket_name):
                continue
            for obj in client.list_objects(bucket_name, recursive=True):
                try:
                    response = client.get_object(bucket_name, obj.object_name)
                    data = response.read()
                    response.close()
                    response.release_conn()

                    info = tarfile.TarInfo(name=f"{bucket_name}/{obj.object_name}")
                    info.size = len(data)
                    tar.addfile(info, io.BytesIO(data))
                    total += len(data)
                except Exception as e:
                    logger.warning("MinIO 오브젝트 백업 건너뜀: %s/%s - %s",
                                   bucket_name, obj.object_name, e)

    archive_data = buf.getvalue()
    if archive_data:
        obj_name = storage_key + "minio_buckets.tar.gz"
        client.put_object(
            BACKUP_BUCKET, obj_name,
            io.BytesIO(archive_data), length=len(archive_data),
            content_type="application/gzip",
        )
        logger.info("MinIO 버킷 백업 완료: %s (%d bytes)", obj_name, len(archive_data))
    return len(archive_data)


def _restore_minio_buckets(client, storage_key):
    """tar.gz에서 MinIO 버킷 복원."""
    obj_name = storage_key + "minio_buckets.tar.gz"
    response = client.get_object(BACKUP_BUCKET, obj_name)
    archive_data = response.read()
    response.close()
    response.release_conn()

    total = 0
    buf = io.BytesIO(archive_data)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            parts = member.name.split("/", 1)
            if len(parts) != 2:
                continue
            bucket_name, object_name = parts

            f = tar.extractfile(member)
            if not f:
                continue
            data = f.read()

            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)

            client.put_object(
                bucket_name, object_name,
                io.BytesIO(data), length=len(data),
            )
            total += len(data)

    logger.info("MinIO 버킷 복원 완료: %d bytes", total)
    return total


# ── Config Files ────────────────────────────────────


def _backup_config_files(client, storage_key):
    """설정 파일 tar.gz 백업."""
    buf = io.BytesIO()
    total = 0

    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for rel_path in _CONFIG_FILES:
            abs_path = os.path.join(_PROJECT_ROOT, rel_path)
            if os.path.isfile(abs_path):
                tar.add(abs_path, arcname=rel_path)
                total += os.path.getsize(abs_path)

        # Benthos YAML 파일들 (있는 경우)
        benthos_dir = os.path.join(_PROJECT_ROOT, "benthos")
        if os.path.isdir(benthos_dir):
            for fname in os.listdir(benthos_dir):
                if fname.endswith((".yaml", ".yml")):
                    fpath = os.path.join(benthos_dir, fname)
                    tar.add(fpath, arcname=f"benthos/{fname}")
                    total += os.path.getsize(fpath)

    archive_data = buf.getvalue()
    obj_name = storage_key + "config_files.tar.gz"
    client.put_object(
        BACKUP_BUCKET, obj_name,
        io.BytesIO(archive_data), length=len(archive_data),
        content_type="application/gzip",
    )
    logger.info("설정 파일 백업 완료: %s (%d bytes)", obj_name, len(archive_data))
    return len(archive_data)


def _restore_config_files(client, storage_key):
    """설정 파일 복원."""
    obj_name = storage_key + "config_files.tar.gz"
    response = client.get_object(BACKUP_BUCKET, obj_name)
    archive_data = response.read()
    response.close()
    response.release_conn()

    buf = io.BytesIO(archive_data)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        # 안전한 추출: 상위 디렉토리 참조 차단
        for member in tar.getmembers():
            if member.name.startswith("/") or ".." in member.name:
                logger.warning("설정 파일 복원 건너뜀 (안전하지 않은 경로): %s", member.name)
                continue
            tar.extract(member, path=_PROJECT_ROOT)

    logger.info("설정 파일 복원 완료: %d bytes", len(archive_data))
    return len(archive_data)


# ── Helpers ─────────────────────────────────────────


def _get_minio_client():
    """MinIO 클라이언트 생성 (DB 설정 우선)."""
    db = SessionLocal()
    try:
        return get_minio_client(db)
    finally:
        db.close()


def _ensure_bucket(client, bucket_name):
    """버킷이 없으면 생성."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
