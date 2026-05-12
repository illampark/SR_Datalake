import logging
import os
import io
import psutil
from datetime import datetime
from flask import Blueprint, request, jsonify, send_file
from minio.error import S3Error
from backend.database import SessionLocal
from backend.models.storage import FileCleanupPolicy
from backend.config import MINIO_BUCKETS
from backend.services.audit_logger import audit_route
from backend.services.minio_client import get_minio_client, get_minio_config

logger = logging.getLogger(__name__)

file_bp = Blueprint("storage_file", __name__, url_prefix="/api/storage/file")


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


def _get_minio():
    db = SessionLocal()
    try:
        return get_minio_client(db)
    finally:
        db.close()


def _fmt_bytes(b):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


_DISK_PATH_CANDIDATES = (
    "/app/static/uploads",   # bind mount target (DATA_ROOT/sdl-uploads)
    "/data",                 # host data dir (스테이징 등)
    "/",                     # 최후 fallback
)


def _resolve_disk_path():
    for p in _DISK_PATH_CANDIDATES:
        if os.path.exists(p):
            return p
    return "/"


FILE_TYPE_MAP = {
    ".log": "log", ".csv": "csv", ".tsv": "csv",
    ".json": "json", ".jsonl": "json", ".ndjson": "json",
    ".png": "image", ".jpg": "image", ".jpeg": "image",
    ".gif": "image", ".bmp": "image", ".svg": "image",
    ".pdf": "doc", ".doc": "doc", ".docx": "doc",
    ".xls": "excel", ".xlsx": "excel",
    ".ppt": "doc", ".pptx": "doc", ".txt": "doc",
    ".tar": "backup", ".gz": "backup", ".zip": "backup",
    ".bak": "backup", ".7z": "backup", ".rar": "backup",
}

FILE_TYPE_LABELS = {
    "log": "로그 파일",
    "csv": "CSV 파일",
    "json": "JSON / JSONL",
    "image": "이미지",
    "doc": "문서",
    "excel": "Excel",
    "backup": "백업 파일",
    "other": "기타",
}


def _classify(filename):
    ext = os.path.splitext(filename)[1].lower()
    return FILE_TYPE_MAP.get(ext, "other"), ext


# ──────────────────────────────────────────────
# STR-011: GET /api/storage/file/status
# ──────────────────────────────────────────────
@file_bp.route("/status", methods=["GET"])
def get_storage_status():
    db = SessionLocal()
    cfg = get_minio_config(db)
    try:
        # ── 1) MinIO 사용량 ──
        minio_status = "connected"
        minio_error = None
        minio_total_size = 0
        minio_total_objects = 0
        bucket_details = []
        try:
            client = _get_minio()
            for bname in MINIO_BUCKETS:
                bsize = 0
                bcount = 0
                try:
                    for obj in client.list_objects(bname, recursive=True):
                        bsize += obj.size
                        bcount += 1
                except S3Error:
                    pass
                minio_total_size += bsize
                minio_total_objects += bcount
                bucket_details.append({
                    "bucket": bname,
                    "size_bytes": bsize,
                    "size_display": _fmt_bytes(bsize),
                    "object_count": bcount,
                })
        except Exception as e:
            minio_status = "disconnected"
            minio_error = str(e)

        # ── 2) local_path import_collector 들의 디스크 사용량 ──
        local_total_size = 0
        local_total_objects = 0
        local_paths = []
        try:
            from backend.models.collector import ImportCollector
            ic_rows = (
                db.query(ImportCollector)
                  .filter(ImportCollector.source_mode == "local_path")
                  .all()
            )
            for ic in ic_rows:
                p = ic.local_path or ""
                if not p or not os.path.isdir(p):
                    continue
                lsize = 0
                lcount = 0
                for root_dir, _subdirs, fnames in os.walk(p):
                    for fn in fnames:
                        try:
                            st = os.stat(os.path.join(root_dir, fn))
                            lsize += st.st_size
                            lcount += 1
                        except OSError:
                            continue
                local_total_size += lsize
                local_total_objects += lcount
                local_paths.append({
                    "collectorId": ic.id,
                    "collectorName": getattr(ic, "name", "") or f"import #{ic.id}",
                    "path": p,
                    "size_bytes": lsize,
                    "size_display": _fmt_bytes(lsize),
                    "object_count": lcount,
                })
        except Exception as e:
            logger.warning("local_path 사용량 집계 실패: %s", e)

        # ── 3) 디스크 전체 capacity ──
        cap_override = request.args.get("capacityGB")
        disk_path = None
        if cap_override is not None:
            try:
                total_gb = float(cap_override)
                # avail 는 합산 used 기준으로 계산 (아래에서)
            except (TypeError, ValueError):
                cap_override = None
        if cap_override is None:
            disk_path = _resolve_disk_path()
            disk = psutil.disk_usage(disk_path)
            total_gb = disk.total / (1024 ** 3)

        minio_used_gb = minio_total_size / (1024 ** 3)
        local_used_gb = local_total_size / (1024 ** 3)
        total_used_gb = minio_used_gb + local_used_gb
        avail_gb = max(total_gb - total_used_gb, 0)

        def _pct(used):
            return round((used / total_gb) * 100, 1) if total_gb > 0 else 0

        return _ok({
            # ── 하위 호환 (기존 stat 카드용 — 합계) ──
            "totalGB": round(total_gb, 2),
            "usedGB": round(total_used_gb, 4),
            "availableGB": round(avail_gb, 2),
            "usagePercent": _pct(total_used_gb),
            "totalObjects": minio_total_objects + local_total_objects,
            "buckets": bucket_details,
            "storageType": "MinIO S3",
            "endpoint": cfg["endpoint"],
            "status": minio_status,
            "diskPath": disk_path,
            "snapshot_at": datetime.utcnow().isoformat(),
            # ── 출처별 분리 ──
            "minio": {
                "usedGB": round(minio_used_gb, 4),
                "sizeBytes": minio_total_size,
                "sizeDisplay": _fmt_bytes(minio_total_size),
                "objectCount": minio_total_objects,
                "usagePercent": _pct(minio_used_gb),
                "status": minio_status,
                "error": minio_error,
                "buckets": bucket_details,
            },
            "localPath": {
                "usedGB": round(local_used_gb, 4),
                "sizeBytes": local_total_size,
                "sizeDisplay": _fmt_bytes(local_total_size),
                "objectCount": local_total_objects,
                "usagePercent": _pct(local_used_gb),
                "paths": local_paths,
            },
        })
    except Exception as e:
        return _ok({
            "totalGB": 0, "usedGB": 0, "availableGB": 0, "usagePercent": 0,
            "totalObjects": 0, "buckets": [],
            "storageType": "MinIO S3", "endpoint": cfg.get("endpoint", ""),
            "status": "disconnected",
            "error_detail": str(e),
            "snapshot_at": datetime.utcnow().isoformat(),
            "minio": {"usedGB": 0, "sizeBytes": 0, "sizeDisplay": "0.0 B",
                      "objectCount": 0, "usagePercent": 0, "status": "disconnected", "buckets": []},
            "localPath": {"usedGB": 0, "sizeBytes": 0, "sizeDisplay": "0.0 B",
                          "objectCount": 0, "usagePercent": 0, "paths": []},
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# STR-012: GET /api/storage/file/browse
# ──────────────────────────────────────────────
@file_bp.route("/browse", methods=["GET"])
def browse_files():
    try:
        client = _get_minio()
        bucket = request.args.get("bucket", MINIO_BUCKETS[0])
        path = request.args.get("path", "")
        file_type = request.args.get("type", "")
        search = request.args.get("search", "").lower()
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)

        items = []
        for obj in client.list_objects(bucket, prefix=path, recursive=True):
            name = obj.object_name
            filename = os.path.basename(name)
            if not filename:
                continue

            ftype, ext = _classify(filename)
            if file_type and ftype != file_type:
                continue
            if search and search not in filename.lower():
                continue

            dir_path = os.path.dirname(name)
            items.append({
                "name": filename,
                "objectName": name,
                "type": ftype,
                "extension": ext,
                "size": obj.size,
                "sizeDisplay": _fmt_bytes(obj.size),
                "path": "/" + dir_path + "/" if dir_path else "/",
                "bucket": bucket,
                "modifiedAt": obj.last_modified.isoformat() if obj.last_modified else None,
            })

        items.sort(key=lambda x: x["modifiedAt"] or "", reverse=True)
        total = len(items)
        start = (page - 1) * size
        paged = items[start:start + size]

        return _ok(paged, {"page": page, "size": size, "total": total, "bucket": bucket})
    except S3Error as e:
        return _err(f"MinIO 조회 오류: {e}", "S3_ERROR", 500)
    except Exception as e:
        return _err(f"파일 목록 조회 실패: {e}", "SERVER_ERROR", 500)


# ──────────────────────────────────────────────
# STR-013: GET /api/storage/file/stats
# ──────────────────────────────────────────────
@file_bp.route("/stats", methods=["GET"])
def get_file_stats():
    try:
        client = _get_minio()
        type_stats = {}

        for bname in MINIO_BUCKETS:
            try:
                for obj in client.list_objects(bname, recursive=True):
                    filename = os.path.basename(obj.object_name)
                    if not filename:
                        continue
                    ftype, _ = _classify(filename)
                    if ftype not in type_stats:
                        type_stats[ftype] = {"count": 0, "total_size": 0}
                    type_stats[ftype]["count"] += 1
                    type_stats[ftype]["total_size"] += obj.size
            except S3Error:
                pass

        stats = []
        for ftype, info in type_stats.items():
            stats.append({
                "type": ftype,
                "label": FILE_TYPE_LABELS.get(ftype, ftype),
                "count": info["count"],
                "totalSizeBytes": info["total_size"],
                "totalSizeGB": round(info["total_size"] / (1024 ** 3), 4),
                "totalSizeDisplay": _fmt_bytes(info["total_size"]),
            })

        stats.sort(key=lambda x: x["totalSizeBytes"], reverse=True)
        return _ok({"stats": stats, "snapshot_at": datetime.utcnow().isoformat()})
    except Exception as e:
        return _err(f"통계 조회 실패: {e}", "SERVER_ERROR", 500)


# ──────────────────────────────────────────────
# STR-014: GET/PUT /api/storage/file/cleanup-policy
# ──────────────────────────────────────────────
@file_bp.route("/cleanup-policy", methods=["GET"])
def get_cleanup_policy():
    db = _db()
    try:
        policy = db.query(FileCleanupPolicy).first()
        if not policy:
            return _ok({
                "retention_days": 90,
                "threshold_percent": 80.0,
                "enabled": False,
                "target_buckets": ["sdl-files", "sdl-archive"],
                "target_extensions": [".log", ".tmp", ".csv"],
            })
        return _ok(policy.to_dict())
    finally:
        db.close()


@file_bp.route("/cleanup-policy", methods=["PUT"])
@audit_route("storage", "storage.file.cleanup_policy.update", target_type="file_cleanup_policy",
             detail_keys=["bucket", "retentionDays", "enabled"])
def update_cleanup_policy():
    db = _db()
    try:
        body = request.get_json(force=True)

        ret = body.get("retentionDays") or body.get("retention_days")
        thr = body.get("thresholdPercent") or body.get("threshold_percent")

        if ret is not None and (int(ret) < 1 or int(ret) > 3650):
            return _err("보관 기간은 1~3650일 사이여야 합니다.", "VALIDATION")
        if thr is not None and (float(thr) < 0 or float(thr) > 100):
            return _err("임계치는 0~100% 사이여야 합니다.", "VALIDATION")

        policy = db.query(FileCleanupPolicy).first()
        if not policy:
            policy = FileCleanupPolicy()
            db.add(policy)

        if ret is not None:
            policy.retention_days = int(ret)
        if thr is not None:
            policy.threshold_percent = float(thr)
        if "enabled" in body:
            policy.enabled = bool(body["enabled"])
        if "target_buckets" in body:
            policy.target_buckets = body["target_buckets"]
        if "target_extensions" in body:
            policy.target_extensions = body["target_extensions"]

        policy.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(policy)
        return _ok(policy.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/storage/file/upload
# ──────────────────────────────────────────────
@file_bp.route("/upload", methods=["POST"])
@audit_route("storage", "storage.file.upload", target_type="minio_object")
def upload_file():
    try:
        if "file" not in request.files:
            return _err("파일이 선택되지 않았습니다.", "VALIDATION")

        files = request.files.getlist("file")
        if not files or files[0].filename == "":
            return _err("파일이 선택되지 않았습니다.", "VALIDATION")

        bucket = request.form.get("bucket", MINIO_BUCKETS[0])
        path = request.form.get("path", "").strip("/")

        client = _get_minio()

        if not client.bucket_exists(bucket):
            return _err(f"버킷 '{bucket}'이 존재하지 않습니다.", "NOT_FOUND", 404)

        uploaded = []
        for f in files:
            filename = f.filename
            object_name = f"{path}/{filename}" if path else filename

            file_data = f.read()
            file_size = len(file_data)

            if file_size > 500 * 1024 * 1024:
                return _err(f"파일 '{filename}'의 크기가 500MB를 초과합니다.", "VALIDATION")

            client.put_object(
                bucket, object_name,
                io.BytesIO(file_data), length=file_size,
                content_type=f.content_type or "application/octet-stream",
            )
            uploaded.append({
                "name": filename,
                "objectName": object_name,
                "bucket": bucket,
                "size": file_size,
                "sizeDisplay": _fmt_bytes(file_size),
            })

        return _ok({"uploaded": uploaded, "count": len(uploaded)}), 201
    except S3Error as e:
        return _err(f"MinIO 업로드 오류: {e}", "S3_ERROR", 500)
    except Exception as e:
        return _err(f"업로드 실패: {e}", "SERVER_ERROR", 500)


# ──────────────────────────────────────────────
# DELETE /api/storage/file/delete
# ──────────────────────────────────────────────
_PREVIEW_TEXT_EXTS = {
    ".log", ".csv", ".tsv", ".txt", ".json", ".jsonl", ".ndjson",
    ".xml", ".yaml", ".yml", ".md", ".sql", ".py", ".sh", ".conf", ".ini",
    ".html", ".htm", ".css", ".js", ".ts",
}
_PREVIEW_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".svg", ".webp", ".ico"}
_PREVIEW_PDF_EXTS = {".pdf"}
_PREVIEW_BINARY_EXTS = {
    ".bin", ".dat", ".exe", ".dll", ".so", ".dylib", ".o", ".obj",
    ".pyc", ".pyo", ".class", ".jar", ".war", ".ear",
    ".tar", ".gz", ".tgz", ".bz2", ".xz", ".zip", ".7z", ".rar",
}
_PREVIEW_MAX_BYTES_DEFAULT = 256 * 1024  # 256 KB


def _guess_inline_mime(ext):
    return {
        ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".gif": "image/gif", ".bmp": "image/bmp", ".svg": "image/svg+xml",
        ".webp": "image/webp", ".ico": "image/x-icon",
        ".pdf": "application/pdf",
    }.get(ext.lower(), "application/octet-stream")


@file_bp.route("/preview", methods=["GET"])
def preview_file():
    """파일 미리보기 — 텍스트 컨텐츠 일부 또는 raw 스트리밍 URL 반환."""
    try:
        bucket = request.args.get("bucket") or MINIO_BUCKETS[0]
        object_name = request.args.get("objectName") or request.args.get("object_name")
        max_bytes = request.args.get("maxBytes", _PREVIEW_MAX_BYTES_DEFAULT, type=int)
        if not object_name:
            return _err("objectName 이 필요합니다.", "VALIDATION")

        client = _get_minio()
        try:
            stat = client.stat_object(bucket, object_name)
        except S3Error:
            return _err(f"파일을 찾을 수 없습니다: {object_name}", "NOT_FOUND", 404)

        ext = os.path.splitext(object_name)[1].lower()
        size = int(stat.size or 0)
        raw_url = (
            f"/api/storage/file/raw?bucket={bucket}"
            f"&objectName={object_name}"
        )

        if ext in _PREVIEW_IMAGE_EXTS:
            mt = stat.content_type
            if not mt or mt == "application/octet-stream":
                mt = _guess_inline_mime(ext)
            return _ok({
                "kind": "image",
                "objectName": object_name,
                "bucket": bucket,
                "size": size,
                "extension": ext,
                "mimeType": mt,
                "rawUrl": raw_url,
            })

        if ext in _PREVIEW_PDF_EXTS:
            return _ok({
                "kind": "pdf",
                "objectName": object_name,
                "bucket": bucket,
                "size": size,
                "extension": ext,
                "rawUrl": raw_url,
            })

        if ext == ".xlsx":
            sheet = request.args.get("sheet") or ""
            max_rows = request.args.get("maxRows", 100, type=int)
            try:
                resp = client.get_object(bucket, object_name)
                try:
                    raw = resp.read()
                finally:
                    resp.close()
                    resp.release_conn()
            except Exception as e:
                return _err(f"파일 읽기 실패: {e}", "READ_ERROR", 500)
            try:
                from io import BytesIO
                import openpyxl
                wb = openpyxl.load_workbook(BytesIO(raw), read_only=True, data_only=True)
                try:
                    sheets = list(wb.sheetnames)
                    cur = sheet if (sheet and sheet in sheets) else sheets[0]
                    ws = wb[cur]
                    headers, rows = None, []
                    for ri, row in enumerate(ws.iter_rows(values_only=True), start=1):
                        if ri == 1:
                            headers = [(str(c).strip() if c is not None else f"col_{i}") for i, c in enumerate(row)]
                            continue
                        if all(c is None for c in row):
                            continue
                        rows.append([
                            (v.isoformat() if hasattr(v, "isoformat") else v)
                            for v in row
                        ])
                        if len(rows) >= max_rows:
                            break
                finally:
                    wb.close()
                return _ok({
                    "kind": "table",
                    "objectName": object_name,
                    "bucket": bucket,
                    "size": size,
                    "extension": ext,
                    "sheets": sheets,
                    "currentSheet": cur,
                    "headers": headers or [],
                    "rows": rows,
                    "previewRows": len(rows),
                    "truncated": len(rows) >= max_rows,
                    "rawUrl": raw_url,
                })
            except Exception as e:
                logger.warning("xlsx 미리보기 실패 %s: %s", object_name, e)
                return _ok({
                    "kind": "binary",
                    "objectName": object_name,
                    "bucket": bucket,
                    "size": size,
                    "extension": ext,
                    "rawUrl": raw_url,
                    "error": str(e),
                })

        if ext not in _PREVIEW_BINARY_EXTS and (ext in _PREVIEW_TEXT_EXTS or size <= 4096):
            read_size = max(1, min(size, max_bytes))
            data = b""
            try:
                resp = client.get_object(bucket, object_name)
                try:
                    data = resp.read(read_size)
                finally:
                    resp.close()
                    resp.release_conn()
            except Exception as e:
                return _err(f"파일 읽기 실패: {e}", "READ_ERROR", 500)
            try:
                content = data.decode("utf-8")
            except UnicodeDecodeError:
                content = data.decode("utf-8", errors="replace")
            return _ok({
                "kind": "text",
                "objectName": object_name,
                "bucket": bucket,
                "size": size,
                "previewBytes": len(data),
                "truncated": size > read_size,
                "content": content,
                "extension": ext,
            })

        return _ok({
            "kind": "binary",
            "objectName": object_name,
            "bucket": bucket,
            "size": size,
            "extension": ext,
            "rawUrl": raw_url,
            "message": "이 파일 형식은 직접 미리보기를 지원하지 않습니다. 다운로드해서 확인하세요.",
        })
    except Exception as e:
        return _err(str(e), "SERVER_ERROR", 500)


@file_bp.route("/raw", methods=["GET"])
def raw_file():
    """파일 원본 inline 스트리밍 (이미지/PDF 미리보기 src 용)."""
    from flask import Response, stream_with_context
    try:
        bucket = request.args.get("bucket") or MINIO_BUCKETS[0]
        object_name = request.args.get("objectName") or request.args.get("object_name")
        if not object_name:
            return _err("objectName 이 필요합니다.", "VALIDATION")
        client = _get_minio()
        try:
            stat = client.stat_object(bucket, object_name)
        except S3Error:
            return _err("파일을 찾을 수 없습니다.", "NOT_FOUND", 404)

        ext = os.path.splitext(object_name)[1].lower()
        ct = stat.content_type
        if not ct or ct == "application/octet-stream":
            ct = _guess_inline_mime(ext)

        resp = client.get_object(bucket, object_name)

        def _generate():
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
            "Content-Disposition": f"inline; filename=\"{os.path.basename(object_name)}\"",
            "X-Content-Type-Options": "nosniff",
        }
        if stat.size:
            headers["Content-Length"] = str(stat.size)

        return Response(stream_with_context(_generate()), mimetype=ct, headers=headers)
    except Exception as e:
        return _err(str(e), "SERVER_ERROR", 500)


@file_bp.route("/delete-batch", methods=["DELETE"])
@audit_route("storage", "storage.file.delete_batch", target_type="minio_object")
def delete_files_batch():
    """여러 파일을 한 번에 삭제 (멀티 선택용).

    body: {files: [{bucket, objectName}, ...]}
    """
    try:
        body = request.get_json(force=True) or {}
        files = body.get("files") or []
        if not isinstance(files, list) or not files:
            return _err("삭제할 파일 목록이 비어있습니다.", "VALIDATION")

        client = _get_minio()
        deleted = 0
        errors = []
        for f in files:
            bucket = f.get("bucket") or MINIO_BUCKETS[0]
            object_name = f.get("objectName") or f.get("object_name")
            if not object_name:
                errors.append({"bucket": bucket, "objectName": "", "error": "objectName 누락"})
                continue
            try:
                client.remove_object(bucket, object_name)
                deleted += 1
            except S3Error as se:
                errors.append({"bucket": bucket, "objectName": object_name, "error": str(se)})
            except Exception as e:
                errors.append({"bucket": bucket, "objectName": object_name, "error": str(e)})

        return _ok({
            "requested": len(files),
            "deleted": deleted,
            "failed": len(errors),
            "errors": errors[:20],
        })
    except Exception as e:
        return _err(str(e), "SERVER_ERROR", 500)


@file_bp.route("/delete", methods=["DELETE"])
@audit_route("storage", "storage.file.delete", target_type="minio_object")
def delete_file():
    try:
        body = request.get_json(force=True)
        bucket = body.get("bucket", MINIO_BUCKETS[0])
        object_name = body.get("objectName") or body.get("object_name")

        if not object_name:
            return _err("삭제할 파일의 objectName이 필요합니다.", "VALIDATION")

        client = _get_minio()

        try:
            client.stat_object(bucket, object_name)
        except S3Error:
            return _err(f"파일을 찾을 수 없습니다: {object_name}", "NOT_FOUND", 404)

        client.remove_object(bucket, object_name)
        return _ok({"deleted": object_name, "bucket": bucket})
    except S3Error as e:
        return _err(f"MinIO 삭제 오류: {e}", "S3_ERROR", 500)
    except Exception as e:
        return _err(f"삭제 실패: {e}", "SERVER_ERROR", 500)


# ──────────────────────────────────────────────
# GET /api/storage/file/download
# ──────────────────────────────────────────────
@file_bp.route("/download", methods=["GET"])
def download_file():
    try:
        bucket = request.args.get("bucket", MINIO_BUCKETS[0])
        object_name = request.args.get("objectName") or request.args.get("object_name")

        if not object_name:
            return _err("다운로드할 파일의 objectName이 필요합니다.", "VALIDATION")

        client = _get_minio()

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
    except S3Error as e:
        return _err(f"MinIO 다운로드 오류: {e}", "S3_ERROR", 500)
    except Exception as e:
        return _err(f"다운로드 실패: {e}", "SERVER_ERROR", 500)


# ══════════════════════════════════════════════════════════════════════
# Local Path file management — import_collector(source_mode='local_path')
# 가 가리키는 호스트 파일시스템 경로의 파일을 직접 조회/다운로드/삭제
# ══════════════════════════════════════════════════════════════════════

def _resolve_collector(db, collector_id):
    """ImportCollector 조회 + local_path 모드 검증. 실패 시 (None, err_resp)."""
    from backend.models.collector import ImportCollector
    ic = db.query(ImportCollector).get(int(collector_id))
    if not ic:
        return None, _err("Import Collector를 찾을 수 없습니다.", "NOT_FOUND", 404)
    if ic.source_mode != "local_path" or not ic.local_path:
        return None, _err("local_path 모드의 Import Collector가 아닙니다.", "INVALID_MODE", 400)
    if not os.path.isdir(ic.local_path):
        return None, _err(f"local_path가 존재하지 않습니다: {ic.local_path}", "PATH_NOT_FOUND", 404)
    return ic, None


def _safe_join(base, rel):
    """base 디렉토리 안에 머무는 경로만 허용 (path traversal 방지). 위반 시 None."""
    base_real = os.path.realpath(base)
    target = os.path.realpath(os.path.join(base, rel or ""))
    if target == base_real or target.startswith(base_real + os.sep):
        return target
    return None


@file_bp.route("/local/collectors", methods=["GET"])
def list_local_collectors():
    """local_path 모드 Import Collector 목록 (드롭다운용)."""
    db = SessionLocal()
    try:
        from backend.models.collector import ImportCollector
        rows = (
            db.query(ImportCollector)
              .filter(ImportCollector.source_mode == "local_path")
              .order_by(ImportCollector.id)
              .all()
        )
        items = []
        for ic in rows:
            p = ic.local_path or ""
            exists = bool(p) and os.path.isdir(p)
            size_bytes = 0
            count = 0
            if exists:
                try:
                    for root_dir, _sd, fnames in os.walk(p):
                        for fn in fnames:
                            try:
                                size_bytes += os.path.getsize(os.path.join(root_dir, fn))
                                count += 1
                            except OSError:
                                continue
                except OSError:
                    pass
            items.append({
                "id": ic.id,
                "name": ic.name,
                "path": p,
                "exists": exists,
                "sizeBytes": size_bytes,
                "sizeDisplay": _fmt_bytes(size_bytes),
                "fileCount": count,
            })
        return _ok({"items": items})
    finally:
        db.close()


@file_bp.route("/local/browse", methods=["GET"])
def browse_local_files():
    """collectorId 기준 local_path 파일 트리 조회.

    Query: collectorId, path(상대), search, page, size, date_from, date_to
    """
    db = SessionLocal()
    try:
        cid = request.args.get("collectorId", type=int)
        if not cid:
            return _err("collectorId가 필요합니다.", "VALIDATION")
        ic, err = _resolve_collector(db, cid)
        if err:
            return err

        page = max(request.args.get("page", 1, type=int), 1)
        size = max(request.args.get("size", 50, type=int), 1)
        search = (request.args.get("search", "") or "").lower()
        browse_path = (request.args.get("path", "") or "").strip().strip("/")
        date_from = request.args.get("date_from", "")
        date_to = request.args.get("date_to", "")

        base_path = ic.local_path
        current = _safe_join(base_path, browse_path) if browse_path else os.path.realpath(base_path)
        if current is None or not os.path.isdir(current):
            return _err("유효하지 않은 경로입니다.", "INVALID_PATH", 400)

        dt_from = dt_to = None
        if date_from:
            try:
                dt_from = datetime.fromisoformat(date_from)
            except ValueError:
                pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
            except ValueError:
                pass

        is_search = bool(search)
        cumulative_files = []
        current_level_files = []
        dirs = []
        seen_dirs = set()

        for root_dir, subdirs, fnames in os.walk(base_path):
            for fn in fnames:
                full = os.path.join(root_dir, fn)
                rel_path = os.path.relpath(full, base_path).replace(os.sep, "/")
                if search and search not in fn.lower() and search not in rel_path.lower():
                    continue
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                mtime = datetime.utcfromtimestamp(st.st_mtime)
                if dt_from and mtime < dt_from:
                    continue
                if dt_to and mtime >= dt_to:
                    continue
                ext = os.path.splitext(fn)[1].lower()
                ftype, _ = _classify(fn)
                info = {
                    "name": fn,
                    "path": rel_path,
                    "type": ftype,
                    "extension": ext,
                    "size": st.st_size,
                    "sizeDisplay": _fmt_bytes(st.st_size),
                    "modifiedAt": mtime.isoformat(),
                }
                cumulative_files.append(info)
                parent_rel = os.path.dirname(rel_path).replace(os.sep, "/")
                current_rel = browse_path
                if is_search or parent_rel == current_rel:
                    current_level_files.append(info)

        if not is_search:
            try:
                for entry in os.listdir(current):
                    full = os.path.join(current, entry)
                    if os.path.isdir(full) and entry not in seen_dirs:
                        seen_dirs.add(entry)
                        next_rel = (browse_path + "/" + entry).strip("/")
                        dirs.append({
                            "name": entry,
                            "type": "directory",
                            "path": next_rel + "/",
                        })
            except OSError:
                pass

        dirs.sort(key=lambda x: x["name"])
        current_level_files.sort(key=lambda x: x.get("modifiedAt") or "", reverse=True)

        total = len(current_level_files)
        total_size = sum(int(f.get("size") or 0) for f in current_level_files)
        cum_total = len(cumulative_files)
        cum_size = sum(int(f.get("size") or 0) for f in cumulative_files)

        start = (page - 1) * size
        paged = current_level_files[start:start + size]

        breadcrumb = [{"name": "root", "path": ""}]
        if browse_path:
            parts = browse_path.split("/")
            acc = ""
            for p in parts:
                acc += p + "/"
                breadcrumb.append({"name": p, "path": acc})

        return _ok({
            "collector": {"id": ic.id, "name": ic.name, "path": base_path},
            "directories": dirs,
            "items": paged,
            "total": total,
            "totalSize": total_size,
            "totalSizeDisplay": _fmt_bytes(total_size),
            "cumulativeTotal": cum_total,
            "cumulativeSize": cum_size,
            "cumulativeSizeDisplay": _fmt_bytes(cum_size),
            "page": page,
            "size": size,
            "currentPath": browse_path,
            "breadcrumb": breadcrumb,
        })
    finally:
        db.close()


@file_bp.route("/local/download", methods=["GET"])
def download_local_file():
    """local_path 파일 다운로드. Query: collectorId, path(상대)."""
    db = SessionLocal()
    try:
        cid = request.args.get("collectorId", type=int)
        rel = (request.args.get("path", "") or "").strip().lstrip("/")
        if not cid or not rel:
            return _err("collectorId와 path가 필요합니다.", "VALIDATION")
        ic, err = _resolve_collector(db, cid)
        if err:
            return err
        full = _safe_join(ic.local_path, rel)
        if not full or not os.path.isfile(full):
            return _err("파일을 찾을 수 없습니다.", "NOT_FOUND", 404)
        return send_file(full, as_attachment=True, download_name=os.path.basename(full))
    finally:
        db.close()


@file_bp.route("/local/delete-batch", methods=["DELETE"])
def delete_local_files_batch():
    """local_path 파일 일괄 삭제.

    body: {collectorId: int, paths: [str, ...]}
    경로는 collector의 local_path 기준 상대경로.
    """
    db = SessionLocal()
    try:
        body = request.get_json(force=True) or {}
        cid = body.get("collectorId")
        paths = body.get("paths") or []
        if not cid or not isinstance(paths, list) or not paths:
            return _err("collectorId와 paths가 필요합니다.", "VALIDATION")
        ic, err = _resolve_collector(db, cid)
        if err:
            return err

        deleted = []
        errors = []
        for rel in paths:
            rel = (rel or "").strip().lstrip("/")
            if not rel:
                errors.append({"path": rel, "error": "빈 경로"})
                continue
            full = _safe_join(ic.local_path, rel)
            if not full:
                errors.append({"path": rel, "error": "경로가 base 디렉토리를 벗어납니다"})
                continue
            if not os.path.isfile(full):
                errors.append({"path": rel, "error": "파일이 없습니다"})
                continue
            try:
                os.remove(full)
                deleted.append(rel)
            except OSError as e:
                errors.append({"path": rel, "error": str(e)})

        # ── 감사 로그 ──
        try:
            from backend.services.audit_logger import log_audit
            log_audit(
                action_type="data",
                action="storage.local.delete",
                target_type="import_collector",
                target_name=f"{ic.id}:{ic.name}",
                result="success" if not errors else ("failure" if not deleted else "partial"),
                detail={
                    "collectorId": ic.id,
                    "basePath": ic.local_path,
                    "requested": len(paths),
                    "deleted": deleted,
                    "errors": errors[:20],
                },
            )
        except Exception:
            pass

        return _ok({
            "requested": len(paths),
            "deleted": len(deleted),
            "failed": len(errors),
            "errors": errors[:20],
        })
    finally:
        db.close()
