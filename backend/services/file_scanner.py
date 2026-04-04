"""
SFTP File Scanner Service.

Manages background threads that poll remote directories via SFTP
for new/modified files. Each FileCollector gets its own scanner thread that:
  1. Connects to the SFTP server
  2. Scans the remote watch_path for files matching file_patterns
  3. Optionally recurses into subdirectories
  4. Tracks file modification times to detect changes
  5. Reads file content and sends to the Flask callback
"""
import stat as stat_mod
import json
import fnmatch
import logging
import threading
import requests
import paramiko
from datetime import datetime

logger = logging.getLogger(__name__)

# Active scanner threads: {collector_id: Thread}
_scanners = {}
_stop_events = {}
# Tracked file mtimes: {collector_id: {filepath: mtime}}
_file_states = {}


# ── SFTP Client Helper ──────────────────────────

def _get_sftp_client(host, port, username, password="", key_path="", auth_type="password"):
    """Create and return a paramiko SFTP client."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = {
        "hostname": host,
        "port": port,
        "username": username,
        "timeout": 30,
        "allow_agent": False,
        "look_for_keys": False,
    }

    if auth_type == "key" and key_path:
        pkey = paramiko.AutoAddPolicy  # placeholder
        for key_cls in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
            try:
                pkey = key_cls.from_private_key_file(key_path)
                break
            except Exception:
                continue
        connect_kwargs["pkey"] = pkey
    else:
        connect_kwargs["password"] = password

    ssh.connect(**connect_kwargs)
    sftp = ssh.open_sftp()
    return ssh, sftp


def test_sftp_connection(host, port, username, password="", key_path="", auth_type="password"):
    """
    Test SFTP connectivity.
    Returns (success: bool, message: str, info: dict).
    """
    try:
        ssh, sftp = _get_sftp_client(host, port, username, password, key_path, auth_type)
        cwd = sftp.normalize(".")
        sftp.close()
        ssh.close()
        return True, "SFTP 연결 성공", {"cwd": cwd}
    except paramiko.AuthenticationException:
        return False, "인증 실패: 사용자명 또는 비밀번호를 확인하세요.", {}
    except paramiko.SSHException as e:
        return False, f"SSH 오류: {e}", {}
    except Exception as e:
        return False, f"연결 실패: {e}", {}


# ── SFTP Directory Operations ────────────────────

def _sftp_isdir(sftp, path):
    """Check if remote path is a directory."""
    try:
        return stat_mod.S_ISDIR(sftp.stat(path).st_mode)
    except IOError:
        return False


def _sftp_exists(sftp, path):
    """Check if remote path exists."""
    try:
        sftp.stat(path)
        return True
    except IOError:
        return False


def _sftp_walk(sftp, remote_path):
    """Recursive directory walk over SFTP, yielding (dirpath, dirnames, filenames)."""
    try:
        entries = sftp.listdir_attr(remote_path)
    except IOError:
        return

    dirs = []
    files = []
    for entry in entries:
        if stat_mod.S_ISDIR(entry.st_mode):
            dirs.append(entry.filename)
        else:
            files.append(entry.filename)

    yield remote_path, dirs, files

    for d in dirs:
        sub_path = remote_path.rstrip("/") + "/" + d
        yield from _sftp_walk(sftp, sub_path)


def _sftp_mkdir_p(sftp, remote_path):
    """Recursively create directories on SFTP server."""
    if _sftp_exists(sftp, remote_path):
        return
    parent = remote_path.rsplit("/", 1)[0]
    if parent and parent != remote_path:
        _sftp_mkdir_p(sftp, parent)
    try:
        sftp.mkdir(remote_path)
    except IOError:
        pass


# ── Public API ───────────────────────────────────

def validate_watch_path(sftp_params, watch_path):
    """
    Validate that the watch path exists on the SFTP server.
    sftp_params: dict with host, port, username, password, keyPath, authType
    """
    if not watch_path:
        return False, "감시 경로가 비어있습니다."

    try:
        ssh, sftp = _get_sftp_client(
            sftp_params["host"], sftp_params["port"], sftp_params["username"],
            sftp_params.get("password", ""), sftp_params.get("keyPath", ""),
            sftp_params.get("authType", "password"),
        )
    except Exception as e:
        return False, f"SFTP 연결 실패: {e}"

    try:
        if not _sftp_exists(sftp, watch_path):
            return False, f"경로가 존재하지 않습니다: {watch_path}"
        if not _sftp_isdir(sftp, watch_path):
            return False, f"디렉토리가 아닙니다: {watch_path}"
        return True, "경로 확인 성공"
    except Exception as e:
        return False, f"경로 검증 오류: {e}"
    finally:
        sftp.close()
        ssh.close()


def scan_directory(sftp_params, watch_path, file_patterns, recursive):
    """
    Scan a remote directory via SFTP and return matching files.
    Returns list of {path, name, size, mtime, mtime_iso}.
    """
    results = []
    try:
        ssh, sftp = _get_sftp_client(
            sftp_params["host"], sftp_params["port"], sftp_params["username"],
            sftp_params.get("password", ""), sftp_params.get("keyPath", ""),
            sftp_params.get("authType", "password"),
        )
    except Exception:
        return results

    try:
        results = _scan_directory_with_sftp(sftp, watch_path, file_patterns, recursive)
    finally:
        sftp.close()
        ssh.close()

    return results


def start_scanner(collector_id, sftp_host, sftp_port, sftp_username,
                  sftp_password, sftp_key_path, sftp_auth_type,
                  watch_path, file_patterns, recursive,
                  modified_only, poll_interval, post_action, archive_path,
                  encoding, parser_type, callback_url,
                  storage_mode="parse", target_bucket="sdl-files",
                  target_path_prefix="raw/{collector_id}/{date}/"):
    """Start a background SFTP scanner thread for a collector."""
    if collector_id in _scanners and _scanners[collector_id].is_alive():
        logger.info("Scanner %d already running", collector_id)
        return True

    stop_event = threading.Event()
    _stop_events[collector_id] = stop_event

    if collector_id not in _file_states:
        _file_states[collector_id] = {}

    thread = threading.Thread(
        target=_scan_loop,
        args=(collector_id, sftp_host, sftp_port, sftp_username,
              sftp_password, sftp_key_path, sftp_auth_type,
              watch_path, file_patterns, recursive,
              modified_only, poll_interval, post_action, archive_path,
              encoding, parser_type, callback_url, stop_event,
              storage_mode, target_bucket, target_path_prefix),
        daemon=True,
        name=f"sftp-scanner-{collector_id}",
    )
    thread.start()
    _scanners[collector_id] = thread
    logger.info("SFTP Scanner %d started for %s@%s:%d%s (mode=%s)",
                collector_id, sftp_username, sftp_host, sftp_port,
                watch_path, storage_mode)
    return True


def stop_scanner(collector_id):
    """Stop the scanner thread for a collector."""
    if collector_id in _stop_events:
        _stop_events[collector_id].set()
    if collector_id in _scanners:
        thread = _scanners[collector_id]
        thread.join(timeout=10)
        del _scanners[collector_id]
    if collector_id in _stop_events:
        del _stop_events[collector_id]
    logger.info("Scanner %d stopped", collector_id)


def is_scanner_running(collector_id):
    """Check if a scanner is running."""
    return collector_id in _scanners and _scanners[collector_id].is_alive()


def get_scanner_status(collector_id):
    """Get scanner status info."""
    running = is_scanner_running(collector_id)
    tracked = len(_file_states.get(collector_id, {}))
    return {
        "running": running,
        "trackedFiles": tracked,
    }


# ── Internal ──────────────────────────────────

def _matches_patterns(filename, patterns):
    """Check if filename matches any of the glob patterns."""
    if not patterns:
        return True
    return any(fnmatch.fnmatch(filename, p) for p in patterns)


def _sftp_file_info(sftp, filepath, filename):
    """Get file metadata from SFTP."""
    try:
        st = sftp.stat(filepath)
        return {
            "path": filepath,
            "name": filename,
            "size": st.st_size,
            "mtime": st.st_mtime,
            "mtime_iso": datetime.fromtimestamp(st.st_mtime).isoformat(),
        }
    except IOError:
        return {"path": filepath, "name": filename,
                "size": 0, "mtime": 0, "mtime_iso": ""}


def _sftp_file_info_from_attr(filepath, attr):
    """Build file info from SFTPAttributes (avoids extra stat call)."""
    return {
        "path": filepath,
        "name": attr.filename,
        "size": attr.st_size or 0,
        "mtime": attr.st_mtime or 0,
        "mtime_iso": datetime.fromtimestamp(attr.st_mtime).isoformat() if attr.st_mtime else "",
    }


def _scan_loop(collector_id, sftp_host, sftp_port, sftp_username,
               sftp_password, sftp_key_path, sftp_auth_type,
               watch_path, file_patterns, recursive,
               modified_only, poll_interval, post_action, archive_path,
               encoding, parser_type, callback_url, stop_event,
               storage_mode="parse", target_bucket="sdl-files",
               target_path_prefix="raw/{collector_id}/{date}/"):
    """Background SFTP scanning loop."""
    logger.info("SFTP scan loop started: id=%d, %s@%s:%d%s, interval=%ds, mode=%s",
                collector_id, sftp_username, sftp_host, sftp_port,
                watch_path, poll_interval, storage_mode)

    ssh = None
    sftp = None

    while not stop_event.is_set():
        try:
            # Ensure SFTP connection
            if ssh is None or sftp is None or not ssh.get_transport() or not ssh.get_transport().is_active():
                if sftp:
                    try:
                        sftp.close()
                    except Exception:
                        pass
                if ssh:
                    try:
                        ssh.close()
                    except Exception:
                        pass
                ssh, sftp = _get_sftp_client(
                    sftp_host, sftp_port, sftp_username,
                    sftp_password, sftp_key_path, sftp_auth_type,
                )
                logger.info("Scanner %d: SFTP connected", collector_id)

            # Scan directory
            tracked = _file_states.get(collector_id, {})
            files = _scan_directory_with_sftp(sftp, watch_path, file_patterns, recursive)

            for finfo in files:
                if stop_event.is_set():
                    break

                fpath = finfo["path"]
                mtime = finfo["mtime"]

                if modified_only and fpath in tracked:
                    if tracked[fpath] >= mtime:
                        continue

                try:
                    if storage_mode == "direct":
                        _process_file_direct(sftp, collector_id, finfo,
                                             target_bucket, target_path_prefix,
                                             callback_url, watch_path)
                    else:
                        _process_file(sftp, collector_id, finfo, encoding,
                                      parser_type, callback_url)
                    tracked[fpath] = mtime

                    if post_action == "move" and archive_path:
                        _move_to_archive(sftp, fpath, watch_path, archive_path)
                        tracked.pop(fpath, None)
                    elif post_action == "delete":
                        sftp.remove(fpath)
                        tracked.pop(fpath, None)

                except Exception as e:
                    logger.error("File processing error (%s): %s", fpath, e)
                    _send_error(collector_id, fpath, str(e), callback_url)

            _file_states[collector_id] = tracked

        except Exception as e:
            logger.error("SFTP scan loop error (id=%d): %s", collector_id, e)
            ssh = None
            sftp = None

        stop_event.wait(poll_interval)

    # Cleanup
    if sftp:
        try:
            sftp.close()
        except Exception:
            pass
    if ssh:
        try:
            ssh.close()
        except Exception:
            pass
    logger.info("SFTP scan loop ended: id=%d", collector_id)


def _scan_directory_with_sftp(sftp, watch_path, file_patterns, recursive):
    """Scan remote directory using existing SFTP connection."""
    results = []
    if recursive:
        for dirpath, dirnames, filenames in _sftp_walk(sftp, watch_path):
            for fname in filenames:
                if _matches_patterns(fname, file_patterns):
                    fpath = dirpath.rstrip("/") + "/" + fname
                    results.append(_sftp_file_info(sftp, fpath, fname))
    else:
        try:
            for entry in sftp.listdir_attr(watch_path):
                if not stat_mod.S_ISDIR(entry.st_mode):
                    if _matches_patterns(entry.filename, file_patterns):
                        fpath = watch_path.rstrip("/") + "/" + entry.filename
                        results.append(_sftp_file_info_from_attr(fpath, entry))
        except IOError:
            pass
    return results


def _process_file(sftp, collector_id, finfo, encoding, parser_type, callback_url):
    """Read file content via SFTP and send to callback."""
    fpath = finfo["path"]
    payload = {
        "collector_id": collector_id,
        "file": {
            "path": fpath,
            "name": finfo["name"],
            "size": finfo["size"],
            "mtime": finfo["mtime_iso"],
        },
        "parser": parser_type,
        "collected_at": datetime.utcnow().isoformat(),
    }

    try:
        with sftp.open(fpath, "r") as f:
            raw = f.read(65536)
        content = raw.decode(encoding, errors="replace") if isinstance(raw, bytes) else raw
        payload["content_preview"] = content[:2048]
        payload["content_length"] = len(content)

        if parser_type == "csv":
            lines = content.strip().split("\n")
            payload["line_count"] = len(lines)
            if lines:
                payload["header"] = lines[0]
        elif parser_type == "json":
            try:
                parsed = json.loads(content)
                payload["record_count"] = len(parsed) if isinstance(parsed, list) else 1
            except json.JSONDecodeError:
                payload["record_count"] = 0
        else:
            payload["line_count"] = content.count("\n") + (1 if content else 0)
    except UnicodeDecodeError:
        payload["content_preview"] = "(binary file)"
        payload["content_length"] = finfo["size"]

    if callback_url:
        try:
            requests.post(callback_url, json=payload, timeout=10)
        except Exception as e:
            logger.error("Callback error: %s", e)


# ── MIME type helper ──────────────────────────

_MIME_MAP = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
    ".gif": "image/gif", ".bmp": "image/bmp", ".svg": "image/svg+xml",
    ".tiff": "image/tiff", ".tif": "image/tiff", ".webp": "image/webp",
    ".mp4": "video/mp4", ".avi": "video/x-msvideo", ".mov": "video/quicktime",
    ".mkv": "video/x-matroska", ".wmv": "video/x-ms-wmv", ".flv": "video/x-flv",
    ".mp3": "audio/mpeg", ".wav": "audio/wav", ".flac": "audio/flac",
    ".pdf": "application/pdf", ".zip": "application/zip",
    ".gz": "application/gzip", ".tar": "application/x-tar",
    ".csv": "text/csv", ".json": "application/json", ".xml": "text/xml",
    ".txt": "text/plain", ".log": "text/plain",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


def _guess_content_type(filename):
    """파일 확장자로 MIME 타입 추정"""
    ext = ("." + filename.rsplit(".", 1)[-1]).lower() if "." in filename else ""
    return _MIME_MAP.get(ext, "application/octet-stream")


# ── Direct mode: SFTP → MinIO ────────────────

def _process_file_direct(sftp, collector_id, finfo, target_bucket,
                         target_path_prefix, callback_url,
                         watch_path=""):
    """SFTP 파일을 MinIO에 직접 업로드 후 메타데이터만 콜백으로 전송"""
    from backend.database import SessionLocal
    from backend.services.minio_client import get_minio_client

    fpath = finfo["path"]
    fname = finfo["name"]
    fsize = finfo["size"]

    # 상대 경로 계산 (하위 폴더 구조 보존)
    if watch_path and fpath.startswith(watch_path):
        rel_path = fpath[len(watch_path):].lstrip("/")
    else:
        rel_path = fname

    # 경로 패턴 치환
    today = datetime.utcnow().strftime("%Y-%m-%d")
    prefix = (target_path_prefix
              .replace("{collector_id}", str(collector_id))
              .replace("{date}", today))
    object_name = prefix + rel_path

    # MinIO 클라이언트
    db = SessionLocal()
    client = get_minio_client(db)
    db.close()

    if not client.bucket_exists(target_bucket):
        client.make_bucket(target_bucket)
        logger.info("Scanner %d: 버킷 생성 — %s", collector_id, target_bucket)

    # SFTP 파일 핸들 → MinIO 스트리밍 업로드
    content_type = _guess_content_type(fname)
    with sftp.open(fpath, "rb") as remote_file:
        client.put_object(target_bucket, object_name, remote_file, fsize,
                          content_type=content_type)

    logger.info("Scanner %d: direct upload %s → %s/%s (%d bytes)",
                collector_id, fname, target_bucket, object_name, fsize)

    # 메타데이터 콜백 (파일 내용 없이 위치 정보만)
    payload = {
        "collector_id": collector_id,
        "storage_mode": "direct",
        "file": {
            "path": fpath,
            "name": fname,
            "size": fsize,
            "mtime": finfo["mtime_iso"],
        },
        "minio": {
            "bucket": target_bucket,
            "objectName": object_name,
        },
        "mime_type": content_type,
        "collected_at": datetime.utcnow().isoformat(),
    }
    if callback_url:
        try:
            requests.post(callback_url, json=payload, timeout=10)
        except Exception as e:
            logger.error("Direct mode callback error: %s", e)


def _send_error(collector_id, filepath, error_msg, callback_url):
    """Send error notification to callback."""
    if callback_url:
        try:
            fname = filepath.rsplit("/", 1)[-1] if "/" in filepath else filepath
            requests.post(callback_url, json={
                "collector_id": collector_id,
                "error": True,
                "file": {"path": filepath, "name": fname},
                "message": error_msg,
            }, timeout=5)
        except Exception:
            pass


def _move_to_archive(sftp, filepath, watch_path, archive_path):
    """Move file to archive directory on SFTP server."""
    if filepath.startswith(watch_path):
        rel = filepath[len(watch_path):].lstrip("/")
    else:
        rel = filepath.rsplit("/", 1)[-1]

    dest = archive_path.rstrip("/") + "/" + rel
    dest_dir = dest.rsplit("/", 1)[0]
    _sftp_mkdir_p(sftp, dest_dir)
    sftp.rename(filepath, dest)
    logger.info("Archived via SFTP: %s → %s", filepath, dest)
