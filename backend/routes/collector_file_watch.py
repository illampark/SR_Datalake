from datetime import datetime
from flask import Blueprint, request, jsonify
from sqlalchemy import func, or_
from backend.database import SessionLocal
from backend.models.collector import FileCollector
from backend.services import file_scanner as fs
from backend.services import mqtt_manager

file_watch_bp = Blueprint("collector_file_watch", __name__, url_prefix="/api/connectors/file")


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None, "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


def _sftp_params(c):
    """Build sftp_params dict from a FileCollector model instance."""
    return {
        "host": c.sftp_host or "localhost",
        "port": c.sftp_port or 22,
        "username": c.sftp_username or "",
        "password": c.sftp_password or "",
        "keyPath": c.sftp_key_path or "",
        "authType": c.sftp_auth_type or "password",
    }


def _sftp_params_from_body(body):
    """Build sftp_params dict from request body."""
    return {
        "host": body.get("sftpHost", "localhost").strip(),
        "port": int(body.get("sftpPort", 22)),
        "username": body.get("sftpUsername", "").strip(),
        "password": body.get("sftpPassword", ""),
        "keyPath": body.get("sftpKeyPath", "").strip(),
        "authType": body.get("sftpAuthType", "password"),
    }


# ──────────────────────────────────────────────
# FC-001: GET /api/connectors/file — 목록 조회
# ──────────────────────────────────────────────
@file_watch_bp.route("", methods=["GET"])
def list_collectors():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        status_filter = request.args.get("status", "")
        search = (request.args.get("q") or "").strip()

        q = db.query(FileCollector)
        if status_filter:
            q = q.filter(FileCollector.status == status_filter)
        if search:
            like = f"%{search}%"
            q = q.filter(or_(FileCollector.name.ilike(like),
                             FileCollector.description.ilike(like)))

        total = q.count()
        rows = q.order_by(FileCollector.id).offset((page - 1) * size).limit(size).all()

        items = []
        for r in rows:
            d = r.to_dict()
            d["scanner"] = fs.get_scanner_status(r.id)
            items.append(d)

        return _ok(items, {"page": page, "size": size, "total": total})
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-002: GET /api/connectors/file/<id> — 상세 조회
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>", methods=["GET"])
def get_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)
        d = c.to_dict()
        d["scanner"] = fs.get_scanner_status(c.id)
        return _ok(d)
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-003: POST /api/connectors/file — 등록
# ──────────────────────────────────────────────
@file_watch_bp.route("", methods=["POST"])
def create_collector():
    db = _db()
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return _err("수집기명은 필수입니다.", "VALIDATION")

        if db.query(FileCollector).filter_by(name=name).first():
            return _err(f"이미 존재하는 수집기명입니다: {name}", "DUPLICATE")

        watch_path = body.get("watchPath", "").strip()
        if not watch_path:
            return _err("감시 경로는 필수입니다.", "VALIDATION")

        # Parse file patterns from comma-separated or list
        patterns = body.get("filePatterns", [])
        if isinstance(patterns, str):
            patterns = [p.strip() for p in patterns.split(",") if p.strip()]

        c = FileCollector(
            name=name,
            description=body.get("description", ""),
            sftp_host=body.get("sftpHost", "localhost").strip(),
            sftp_port=int(body.get("sftpPort", 22)),
            sftp_username=body.get("sftpUsername", "").strip(),
            sftp_password=body.get("sftpPassword", ""),
            sftp_key_path=body.get("sftpKeyPath", "").strip(),
            sftp_auth_type=body.get("sftpAuthType", "password"),
            watch_path=watch_path,
            file_patterns=patterns,
            recursive=bool(body.get("recursive", True)),
            modified_only=bool(body.get("modifiedOnly", True)),
            collect_mode=body.get("collectMode", "poll"),
            poll_interval=int(body.get("pollInterval", 30)),
            encoding=body.get("encoding", "utf-8"),
            post_action=body.get("postAction", "none"),
            archive_path=body.get("archivePath", ""),
            parser_type=body.get("parserType", "line"),
            storage_mode=body.get("storageMode", "parse"),
            target_bucket=body.get("targetBucket", "sdl-files"),
            target_path_prefix=body.get("targetPathPrefix", "raw/{collector_id}/{date}/"),
        )
        db.add(c)
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict()), 201
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-004: PUT /api/connectors/file/<id> — 수정
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>", methods=["PUT"])
def update_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        body = request.get_json(force=True)
        was_running = c.status == "running"

        if "description" in body:
            c.description = body["description"]
        if "sftpHost" in body:
            c.sftp_host = body["sftpHost"].strip()
        if "sftpPort" in body:
            c.sftp_port = int(body["sftpPort"])
        if "sftpUsername" in body:
            c.sftp_username = body["sftpUsername"].strip()
        if "sftpPassword" in body and body["sftpPassword"] != "":
            c.sftp_password = body["sftpPassword"]
        if "sftpKeyPath" in body:
            c.sftp_key_path = body["sftpKeyPath"].strip()
        if "sftpAuthType" in body:
            c.sftp_auth_type = body["sftpAuthType"]
        if "watchPath" in body:
            c.watch_path = body["watchPath"]
        if "filePatterns" in body:
            patterns = body["filePatterns"]
            if isinstance(patterns, str):
                patterns = [p.strip() for p in patterns.split(",") if p.strip()]
            c.file_patterns = patterns
        if "recursive" in body:
            c.recursive = bool(body["recursive"])
        if "modifiedOnly" in body:
            c.modified_only = bool(body["modifiedOnly"])
        if "collectMode" in body:
            c.collect_mode = body["collectMode"]
        if "pollInterval" in body:
            c.poll_interval = int(body["pollInterval"])
        if "encoding" in body:
            c.encoding = body["encoding"]
        if "postAction" in body:
            c.post_action = body["postAction"]
        if "archivePath" in body:
            c.archive_path = body["archivePath"]
        if "parserType" in body:
            c.parser_type = body["parserType"]
        if "storageMode" in body:
            c.storage_mode = body["storageMode"]
        if "targetBucket" in body:
            c.target_bucket = body["targetBucket"]
        if "targetPathPrefix" in body:
            c.target_path_prefix = body["targetPathPrefix"]

        c.updated_at = datetime.utcnow()

        # 커넥터 설명 → 카탈로그 동기화
        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "file", cid, body["description"])

        # 커넥터 이름 변경 → 카탈로그 이름 동기화
        if "name" in body:
            c.name = body["name"]
            from backend.services.catalog_sync import sync_connector_name
            sync_connector_name(db, "file", cid, body["name"])

        db.commit()
        db.refresh(c)

        # Restart scanner if was running
        if was_running:
            fs.stop_scanner(c.id)
            sp = _sftp_params(c)
            fs.start_scanner(
                c.id, sp["host"], sp["port"], sp["username"],
                sp["password"], sp["keyPath"], sp["authType"],
                c.watch_path, c.file_patterns or [], c.recursive,
                c.modified_only, c.poll_interval, c.post_action, c.archive_path,
                c.encoding, c.parser_type, _callback_url(),
                c.storage_mode or "parse",
                c.target_bucket or "sdl-files",
                c.target_path_prefix or "raw/{collector_id}/{date}/",
            )

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-005: DELETE /api/connectors/file/<id> — 삭제
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>", methods=["DELETE"])
def delete_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            fs.stop_scanner(c.id)

        # 관련 카탈로그 정리
        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "file", cid)

        db.delete(c)
        db.commit()
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-006: POST /api/connectors/file/<id>/start — 수집 시작
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>/start", methods=["POST"])
def start_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        # Validate watch path via SFTP
        sp = _sftp_params(c)
        ok, msg = fs.validate_watch_path(sp, c.watch_path)
        if not ok:
            c.status = "error"
            c.last_error = msg
            db.commit()
            return _err(msg, "PATH_ERROR")

        callback_url = _callback_url()
        fs.start_scanner(
            c.id, sp["host"], sp["port"], sp["username"],
            sp["password"], sp["keyPath"], sp["authType"],
            c.watch_path, c.file_patterns or [], c.recursive,
            c.modified_only, c.poll_interval, c.post_action, c.archive_path,
            c.encoding, c.parser_type, callback_url,
            c.storage_mode or "parse",
            c.target_bucket or "sdl-files",
            c.target_path_prefix or "raw/{collector_id}/{date}/",
        )

        c.status = "running"
        c.last_error = ""
        c.file_count = 0
        c.error_count = 0
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-007: POST /api/connectors/file/<id>/stop — 수집 중지
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>/stop", methods=["POST"])
def stop_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        fs.stop_scanner(c.id)
        c.status = "stopped"
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-008: POST /api/connectors/file/<id>/restart — 재시작
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>/restart", methods=["POST"])
def restart_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        fs.stop_scanner(c.id)

        sp = _sftp_params(c)
        ok, msg = fs.validate_watch_path(sp, c.watch_path)
        if not ok:
            c.status = "error"
            c.last_error = msg
            db.commit()
            return _err(msg, "PATH_ERROR")

        callback_url = _callback_url()
        fs.start_scanner(
            c.id, sp["host"], sp["port"], sp["username"],
            sp["password"], sp["keyPath"], sp["authType"],
            c.watch_path, c.file_patterns or [], c.recursive,
            c.modified_only, c.poll_interval, c.post_action, c.archive_path,
            c.encoding, c.parser_type, callback_url,
            c.storage_mode or "parse",
            c.target_bucket or "sdl-files",
            c.target_path_prefix or "raw/{collector_id}/{date}/",
        )

        c.status = "running"
        c.last_error = ""
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-009: POST /api/connectors/file/<id>/test — SFTP 경로 검증
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>/test", methods=["POST"])
def test_collector(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        sp = _sftp_params(c)
        ok, msg = fs.validate_watch_path(sp, c.watch_path)
        files = []
        if ok:
            files = fs.scan_directory(sp, c.watch_path, c.file_patterns or [], c.recursive)

        return _ok({
            "success": ok,
            "message": msg,
            "watchPath": c.watch_path,
            "sftpHost": sp["host"],
            "sftpPort": sp["port"],
            "matchingFiles": len(files),
            "sampleFiles": [f["name"] for f in files[:10]],
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-010: POST /api/connectors/file/test-path — SFTP 경로 검증 (등록 전)
# ──────────────────────────────────────────────
@file_watch_bp.route("/test-path", methods=["POST"])
def test_path_direct():
    body = request.get_json(force=True)
    watch_path = body.get("watchPath", "").strip()
    patterns = body.get("filePatterns", [])
    if isinstance(patterns, str):
        patterns = [p.strip() for p in patterns.split(",") if p.strip()]
    recursive = bool(body.get("recursive", True))

    sp = _sftp_params_from_body(body)
    ok, msg = fs.validate_watch_path(sp, watch_path)
    files = []
    if ok:
        files = fs.scan_directory(sp, watch_path, patterns, recursive)

    return _ok({
        "success": ok,
        "message": msg,
        "watchPath": watch_path,
        "sftpHost": sp["host"],
        "sftpPort": sp["port"],
        "matchingFiles": len(files),
        "sampleFiles": [{"name": f["name"], "size": f["size"], "mtime": f["mtime_iso"]} for f in files[:20]],
    })


# ──────────────────────────────────────────────
# FC-014: POST /api/connectors/file/test-sftp — SFTP 접속 테스트
# ──────────────────────────────────────────────
@file_watch_bp.route("/test-sftp", methods=["POST"])
def test_sftp():
    body = request.get_json(force=True)
    host = body.get("sftpHost", "localhost").strip()
    port = int(body.get("sftpPort", 22))
    username = body.get("sftpUsername", "").strip()
    password = body.get("sftpPassword", "")
    key_path = body.get("sftpKeyPath", "").strip()
    auth_type = body.get("sftpAuthType", "password")

    if not username:
        return _err("SFTP 사용자명은 필수입니다.", "VALIDATION")

    ok, msg, info = fs.test_sftp_connection(host, port, username, password, key_path, auth_type)
    return _ok({
        "success": ok,
        "message": msg,
        "host": host,
        "port": port,
        "username": username,
        "authType": auth_type,
        "cwd": info.get("cwd", ""),
    })


# ──────────────────────────────────────────────
# FC-011: GET /api/connectors/file/<id>/status — 실시간 상태
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>/status", methods=["GET"])
def collector_status(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        scanner = fs.get_scanner_status(c.id)
        return _ok({
            "id": c.id,
            "name": c.name,
            "status": c.status,
            "fileCount": c.file_count,
            "errorCount": c.error_count,
            "lastFileAt": c.last_file_at.isoformat() if c.last_file_at else None,
            "lastFileName": c.last_file_name,
            "lastError": c.last_error,
            "scanner": scanner,
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-012: GET /api/connectors/file/<id>/files — 현재 감시 디렉토리 파일 목록
# ──────────────────────────────────────────────
@file_watch_bp.route("/<int:cid>/files", methods=["GET"])
def list_files(cid):
    db = _db()
    try:
        c = db.query(FileCollector).get(cid)
        if not c:
            return _err("수집기를 찾을 수 없습니다.", "NOT_FOUND", 404)

        sp = _sftp_params(c)
        files = fs.scan_directory(sp, c.watch_path, c.file_patterns or [], c.recursive)
        return _ok({
            "watchPath": c.watch_path,
            "totalFiles": len(files),
            "files": [{"name": f["name"], "path": f["path"], "size": f["size"],
                        "mtime": f["mtime_iso"]} for f in files[:100]],
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# FC-013: GET /api/connectors/file/summary — 대시보드 통계
# ──────────────────────────────────────────────
@file_watch_bp.route("/summary", methods=["GET"])
def summary():
    db = _db()
    try:
        total = db.query(func.count(FileCollector.id)).scalar()
        running = db.query(func.count(FileCollector.id)).filter(
            FileCollector.status == "running").scalar()
        total_files = db.query(func.coalesce(
            func.sum(FileCollector.file_count), 0)).scalar()
        total_errors = db.query(func.coalesce(
            func.sum(FileCollector.error_count), 0)).scalar()

        return _ok({
            "totalCollectors": total,
            "runningCollectors": running,
            "totalFileCount": int(total_files),
            "totalErrors": int(total_errors),
            "snapshot_at": datetime.utcnow().isoformat(),
        })
    finally:
        db.close()


# ──────────────────────────────────────────────
# POST /api/connectors/file/callback — 파일 수집 콜백
# ──────────────────────────────────────────────
@file_watch_bp.route("/callback", methods=["POST"])
def file_callback():
    """Receives file collection events from the scanner thread and publishes to MQTT."""
    db = _db()
    try:
        body = request.get_json(force=True)
        collector_id = body.get("collector_id")

        if collector_id:
            c = db.query(FileCollector).get(collector_id)
            if c:
                from backend.services.metadata_tracker import ensure_connector_catalog
                ensure_connector_catalog("file", collector_id, c.name)

                if body.get("error"):
                    c.error_count = (c.error_count or 0) + 1
                    c.last_error = body.get("message", "")
                else:
                    c.file_count = (c.file_count or 0) + 1
                    c.last_file_at = datetime.utcnow()
                    file_info = body.get("file", {})
                    c.last_file_name = file_info.get("name", "")
                    c.last_error = ""

                    # MQTT 발행 — 파이프라인 엔진이 수신할 수 있도록
                    file_name = file_info.get("name", "unknown")

                    if body.get("storage_mode") == "direct":
                        # direct 모드: 파일은 이미 MinIO에 저장됨 → 메타데이터만 MQTT 발행
                        minio_info = body.get("minio", {})
                        mqtt_manager.publish_raw(
                            "file", collector_id, file_name,
                            value={
                                "minio_bucket": minio_info.get("bucket", ""),
                                "minio_path": minio_info.get("objectName", ""),
                                "file_size": file_info.get("size", 0),
                                "mime_type": body.get("mime_type", ""),
                                "original_path": file_info.get("path", ""),
                            },
                            data_type="file_meta",
                        )
                    else:
                        # parse 모드: 파일 내용 파싱 → MQTT 발행
                        parser = body.get("parser", "line")
                        content = body.get("content_preview", "")

                        if parser == "csv":
                            _publish_csv_rows(collector_id, file_name, content)
                        elif parser == "json":
                            _publish_json_content(collector_id, file_name, content)
                        else:
                            _publish_line_content(collector_id, file_name, content)

                db.commit()

        return "", 200
    except Exception:
        return "", 200
    finally:
        db.close()


def _publish_csv_rows(collector_id, file_name, content):
    """CSV 내용을 행 단위로 MQTT 발행"""
    import csv
    import io
    if not content:
        return
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        mqtt_manager.publish_raw(
            "file", collector_id, file_name, dict(row),
            data_type="json",
        )


def _publish_json_content(collector_id, file_name, content):
    """JSON 내용을 MQTT 발행"""
    import json
    if not content:
        return
    try:
        parsed = json.loads(content)
        if isinstance(parsed, list):
            for item in parsed:
                mqtt_manager.publish_raw(
                    "file", collector_id, file_name, item,
                    data_type="json",
                )
        else:
            mqtt_manager.publish_raw(
                "file", collector_id, file_name, parsed,
                data_type="json",
            )
    except (json.JSONDecodeError, ValueError):
        pass


def _publish_line_content(collector_id, file_name, content):
    """라인 단위 내용을 MQTT 발행"""
    if not content:
        return
    for line in content.strip().split("\n"):
        line = line.strip()
        if line:
            mqtt_manager.publish_raw(
                "file", collector_id, file_name, line,
                data_type="string",
            )


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _callback_url():
    return "http://localhost:5001/api/connectors/file/callback"
