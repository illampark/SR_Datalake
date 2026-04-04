import os
import io
from datetime import datetime
from flask import Blueprint, request, jsonify, send_file
from minio.error import S3Error
from backend.database import SessionLocal
from backend.models.storage import FileCleanupPolicy
from backend.config import MINIO_BUCKETS
from backend.services.minio_client import get_minio_client, get_minio_config

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


FILE_TYPE_MAP = {
    ".log": "log", ".csv": "csv", ".tsv": "csv",
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
    db.close()
    try:
        client = _get_minio()
        total_size = 0
        total_objects = 0
        bucket_details = []

        for bname in MINIO_BUCKETS:
            bsize = 0
            bcount = 0
            try:
                for obj in client.list_objects(bname, recursive=True):
                    bsize += obj.size
                    bcount += 1
            except S3Error:
                pass
            total_size += bsize
            total_objects += bcount
            bucket_details.append({
                "bucket": bname,
                "size_bytes": bsize,
                "size_display": _fmt_bytes(bsize),
                "object_count": bcount,
            })

        total_gb = float(request.args.get("capacityGB", 460))
        used_gb = total_size / (1024 ** 3)
        avail_gb = max(total_gb - used_gb, 0)
        pct = round((used_gb / total_gb) * 100, 1) if total_gb > 0 else 0

        return _ok({
            "totalGB": round(total_gb, 2),
            "usedGB": round(used_gb, 4),
            "availableGB": round(avail_gb, 2),
            "usagePercent": pct,
            "totalObjects": total_objects,
            "buckets": bucket_details,
            "storageType": "MinIO S3",
            "endpoint": cfg["endpoint"],
            "status": "connected",
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    except Exception as e:
        return _ok({
            "totalGB": 0, "usedGB": 0, "availableGB": 0, "usagePercent": 0,
            "totalObjects": 0, "buckets": [],
            "storageType": "MinIO S3", "endpoint": cfg.get("endpoint", ""),
            "status": "disconnected",
            "error_detail": str(e),
            "snapshot_at": datetime.utcnow().isoformat(),
        })


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
@file_bp.route("/delete", methods=["DELETE"])
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
