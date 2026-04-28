"""
Import Collector — 오프라인 데이터 가져오기 API

엔드포인트:
  IMP-001  GET    /api/connectors/import           목록 조회
  IMP-002  GET    /api/connectors/import/<id>       상세 조회
  IMP-003  POST   /api/connectors/import           생성
  IMP-004  PUT    /api/connectors/import/<id>       수정
  IMP-005  DELETE /api/connectors/import/<id>       삭제
  IMP-006  POST   /api/connectors/import/<id>/upload   파일 업로드
  IMP-007  POST   /api/connectors/import/<id>/preview  미리보기
  IMP-008  POST   /api/connectors/import/<id>/execute  실행
  IMP-009  GET    /api/connectors/import/<id>/status    진행 상태
  IMP-010  POST   /api/connectors/import/<id>/stop     중지
  IMP-011  GET    /api/connectors/import/targets       대상 스토리지 목록
"""

import os
import logging
from flask import Blueprint, request, jsonify
from backend.database import SessionLocal
from backend.models.collector import ImportCollector

logger = logging.getLogger(__name__)
import_bp = Blueprint("collector_import", __name__, url_prefix="/api/connectors/import")

# 업로드 임시 저장: 디스크 기반 (gunicorn 멀티 워커 간 공유)
# 인메모리 dict는 워커별로 분리되어 /upload → /preview /execute 시 데이터 유실됨
import json
import os
import shutil

_UPLOAD_TMP_ROOT = "/tmp/sdl_import"


def _cid_dir(cid):
    return f"{_UPLOAD_TMP_ROOT}/{cid}"


def _store_clear(cid):
    """해당 cid의 모든 임시 파일 정리"""
    d = _cid_dir(cid)
    if os.path.exists(d):
        shutil.rmtree(d, ignore_errors=True)


def _store_set_single(cid, content, filename=""):
    """단일 파일(CSV/JSON) 저장"""
    _store_clear(cid)
    d = _cid_dir(cid)
    os.makedirs(d, exist_ok=True)
    with open(f"{d}/single.bin", "wb") as fp:
        fp.write(content)
    with open(f"{d}/meta.json", "w", encoding="utf-8") as fp:
        json.dump({"mode": "single", "filename": filename, "size": len(content)}, fp)


def _store_get_single(cid):
    """단일 파일 bytes 반환 (없으면 None)"""
    p = f"{_cid_dir(cid)}/single.bin"
    if not os.path.exists(p):
        return None
    with open(p, "rb") as fp:
        return fp.read()


def _store_set_files(cid, file_list):
    """다중 파일 저장: file_list = [{"name", "content", "size"}, ...]"""
    _store_clear(cid)
    d = _cid_dir(cid)
    files_dir = f"{d}/files"
    os.makedirs(files_dir, exist_ok=True)
    meta = {"mode": "files", "files": []}
    for i, f in enumerate(file_list):
        with open(f"{files_dir}/{i:04d}.bin", "wb") as fp:
            fp.write(f["content"])
        meta["files"].append({"idx": i, "name": f["name"], "size": f["size"]})
    with open(f"{d}/meta.json", "w", encoding="utf-8") as fp:
        json.dump(meta, fp, ensure_ascii=False)


def _store_get_files(cid):
    """다중 파일 목록 반환 (없으면 빈 리스트)"""
    d = _cid_dir(cid)
    meta_path = f"{d}/meta.json"
    if not os.path.exists(meta_path):
        return []
    with open(meta_path, encoding="utf-8") as fp:
        meta = json.load(fp)
    if meta.get("mode") != "files":
        return []
    out = []
    for f in meta["files"]:
        with open(f"{d}/files/{f['idx']:04d}.bin", "rb") as fp:
            out.append({"name": f["name"], "size": f["size"], "content": fp.read()})
    return out


def _ok(data=None, meta=None):
    resp = {"success": True, "data": data, "error": None}
    if meta:
        resp["meta"] = meta
    return jsonify(resp)


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


def _db():
    return SessionLocal()


# ═══════════════════════════════════════════
# IMP-001: 목록 조회
# ═══════════════════════════════════════════
@import_bp.route("", methods=["GET"])
def list_imports():
    db = _db()
    try:
        page = request.args.get("page", 1, type=int)
        size = request.args.get("size", 50, type=int)
        status_filter = request.args.get("status", "")

        q = db.query(ImportCollector)
        if status_filter:
            q = q.filter(ImportCollector.status == status_filter)

        total = q.count()
        items = q.order_by(ImportCollector.created_at.desc())\
                 .offset((page - 1) * size).limit(size).all()

        return _ok([c.to_dict() for c in items],
                   meta={"page": page, "size": size, "total": total})
    except Exception as e:
        logger.error(f"IMP-001 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-002: 상세 조회
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>", methods=["GET"])
def get_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)
        return _ok(c.to_dict())
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-003: 생성
# ═══════════════════════════════════════════
@import_bp.route("", methods=["POST"])
def create_import():
    db = _db()
    try:
        body = request.get_json(silent=True) or {}
        name = body.get("name", "").strip()
        if not name:
            return _err("커넥터명은 필수입니다.", "VALIDATION")
        if db.query(ImportCollector).filter_by(name=name).first():
            return _err(f"이미 존재하는 커넥터명입니다: {name}", "DUPLICATE")

        c = ImportCollector(
            name=name,
            description=body.get("description", ""),
            import_type=body.get("importType", "csv"),
            target_type=body.get("targetType", "tsdb"),
            target_id=body.get("targetId"),
            target_table=body.get("targetTable", ""),
            target_measurement=body.get("targetMeasurement", ""),
            target_bucket=body.get("targetBucket", "sdl-files"),
            timestamp_column=body.get("timestampColumn", ""),
            tag_column=body.get("tagColumn", ""),
            value_columns=body.get("valueColumns", []),
            column_mapping=body.get("columnMapping", {}),
            batch_size=body.get("batchSize", 1000),
            encoding=body.get("encoding", "utf-8"),
            delimiter=body.get("delimiter", ","),
            skip_header=body.get("skipHeader", True),
            publish_mqtt=body.get("publishMqtt", True),
            source_mode=body.get("sourceMode", "upload"),
            local_path=body.get("localPath", ""),
            file_patterns=body.get("filePatterns", ["*"]),
            recursive=body.get("recursive", True),
        )
        db.add(c)
        db.commit()
        db.refresh(c)
        return _ok(c.to_dict()), 201
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-003 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-004: 수정
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>", methods=["PUT"])
def update_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        body = request.get_json(silent=True) or {}
        field_map = {
            "name": "name", "description": "description",
            "importType": "import_type", "targetType": "target_type",
            "targetId": "target_id", "targetTable": "target_table",
            "targetMeasurement": "target_measurement",
            "targetBucket": "target_bucket",
            "timestampColumn": "timestamp_column", "tagColumn": "tag_column",
            "valueColumns": "value_columns", "columnMapping": "column_mapping",
            "batchSize": "batch_size", "encoding": "encoding",
            "delimiter": "delimiter", "skipHeader": "skip_header",
            "publishMqtt": "publish_mqtt",
            "sourceMode": "source_mode", "localPath": "local_path",
            "filePatterns": "file_patterns", "recursive": "recursive",
        }
        for api_key, db_key in field_map.items():
            if api_key in body:
                setattr(c, db_key, body[api_key])

        if "name" in body:
            dup = db.query(ImportCollector).filter(
                ImportCollector.name == body["name"],
                ImportCollector.id != cid
            ).first()
            if dup:
                return _err(f"이미 존재하는 커넥터명입니다: {body['name']}", "DUPLICATE")

        db.commit()
        db.refresh(c)

        if "description" in body:
            from backend.services.catalog_sync import sync_connector_description
            sync_connector_description(db, "import", cid, body["description"])

        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-004 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-005: 삭제
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>", methods=["DELETE"])
def delete_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        from backend.services.catalog_sync import delete_connector_catalogs
        delete_connector_catalogs(db, "import", cid)

        db.delete(c)
        db.commit()
        _store_clear(cid)
        return _ok({"deleted": cid})
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-005 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-006: 파일 업로드 (단일 또는 다중)
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/upload", methods=["POST"])
def upload_file(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        files = request.files.getlist("file")
        if not files or (len(files) == 1 and files[0].filename == ""):
            return _err("파일이 필요합니다.", "VALIDATION")

        c.status = "ready"
        c.total_rows = 0
        c.imported_rows = 0
        c.error_rows = 0
        c.progress = 0

        if c.import_type == "files":
            # 다중 파일 모드
            file_list = []
            total_size = 0
            for f in files:
                content = f.read()
                fname = f.filename
                # ZIP 파일이면 압축 해제
                if fname.lower().endswith(".zip"):
                    from backend.services.import_parser import extract_zip
                    extracted = extract_zip(content)
                    file_list.extend(extracted)
                    total_size += sum(e["size"] for e in extracted)
                else:
                    file_list.append({
                        "name": fname,
                        "content": content,
                        "size": len(content),
                    })
                    total_size += len(content)

            _store_set_files(cid, file_list)
            c.file_name = f"{len(file_list)} files"
            c.file_size = total_size
            db.commit()

            return _ok({
                "fileName": c.file_name,
                "fileSize": total_size,
                "fileCount": len(file_list),
                "connectorId": cid,
            })
        else:
            # CSV/JSON 모드 — 단일/다중 파일 + ZIP 자동 해제 지원
            from backend.services.import_parser import extract_zip
            valid_exts = (".csv", ".json", ".txt", ".tsv")
            csv_files = []
            for f in files:
                fname = f.filename or ""
                content = f.read()
                if fname.lower().endswith(".zip"):
                    try:
                        extracted = extract_zip(content)
                    except Exception as zip_err:
                        # deflate64 등 미지원 압축, 손상/암호화 ZIP, 메모리 초과 등
                        return _err(
                            (
                                f"ZIP 파일 '{fname}'을(를) 풀 수 없습니다: {zip_err}\n\n"
                                "[가능한 원인]\n"
                                "  - 표준 deflate가 아닌 압축 방식 (deflate64 / zstd / xz / ppmd 등)\n"
                                "  - 파일이 손상되었거나 비밀번호로 보호됨\n"
                                "  - 압축 풀린 총 크기가 너무 커서 처리 불가\n\n"
                                "[해결 방법]\n"
                                "  1) ZIP을 로컬에서 풀어 CSV/JSON 파일을 직접 업로드 (각 2GB 이내)\n"
                                "  2) 표준 deflate 방식으로 재압축 후 재시도\n"
                                "  3) 대용량 데이터셋은 [서버 경로 지정] 옵션 사용:\n"
                                "     - 가져오기 [편집]에서 \"서버 로컬 경로\"에 적재 위치 입력\n"
                                "     - 저장 후 업로드 아이콘 → [스캔] → [실행]"
                            ),
                            "ZIP_UNSUPPORTED",
                            status=400,
                        )
                    for e in extracted:
                        if e["name"].lower().endswith(valid_exts):
                            csv_files.append(e)
                elif fname.lower().endswith(valid_exts):
                    csv_files.append({"name": fname, "content": content, "size": len(content)})
                else:
                    # 알 수 없는 확장자도 일단 단일 파일로 받음 (기존 호환)
                    csv_files.append({"name": fname, "content": content, "size": len(content)})

            if not csv_files:
                return _err("처리 가능한 CSV/JSON/TXT/TSV 파일이 없습니다.", "VALIDATION")

            if len(csv_files) == 1:
                only = csv_files[0]
                c.file_name = only["name"]
                c.file_size = only["size"]
                db.commit()
                _store_set_single(cid, only["content"], filename=only["name"])
                return _ok({
                    "fileName": only["name"],
                    "fileSize": only["size"],
                    "connectorId": cid,
                })
            else:
                total_size = sum(f["size"] for f in csv_files)
                c.file_name = f"{len(csv_files)} files"
                c.file_size = total_size
                db.commit()
                _store_set_files(cid, csv_files)
                return _ok({
                    "fileName": c.file_name,
                    "fileSize": total_size,
                    "fileCount": len(csv_files),
                    "fileNames": [f["name"] for f in csv_files],
                    "connectorId": cid,
                })
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-006 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-007: 미리보기
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/preview", methods=["POST"])
def preview_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        if c.import_type == "files":
            # 파일 모드 — 파일 목록 미리보기
            file_list = _store_get_files(cid)
            if not file_list:
                return _err("업로드된 파일이 없습니다.", "VALIDATION")
            from backend.services.import_parser import preview_files
            result = preview_files([{"name": f["name"], "size": f["size"]} for f in file_list])
            return _ok(result)
        else:
            # 데이터 모드 — CSV/JSON 내용 미리보기 (단일/다중 모두 지원)
            content = _store_get_single(cid)
            file_list = _store_get_files(cid)

            preview_source = None
            multi_meta = None
            if content is not None:
                preview_source = content
            elif file_list:
                preview_source = file_list[0]["content"]
                multi_meta = {
                    "fileCount": len(file_list),
                    "fileNames": [f["name"] for f in file_list],
                    "totalSize": sum(f["size"] for f in file_list),
                }
            else:
                f = request.files.get("file")
                if f:
                    preview_source = f.read()
                else:
                    return _err("업로드된 파일이 없습니다.", "VALIDATION")

            from backend.services.import_parser import preview_file
            result = preview_file(
                preview_source,
                import_type=c.import_type,
                encoding=c.encoding or "utf-8",
                delimiter=c.delimiter or ",",
                skip_header=c.skip_header,
            )
            if multi_meta:
                result.update(multi_meta)
            return _ok(result)
    except Exception as e:
        logger.error(f"IMP-007 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-008: 실행
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/execute", methods=["POST"])
def execute_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        content = _store_get_single(cid)
        file_list = _store_get_files(cid) or None

        if not content and not file_list:
            return _err("업로드된 파일이 없습니다. 먼저 파일을 업로드하세요.", "VALIDATION")

        # 실행 전 상태 초기화
        c.status = "running"
        c.imported_rows = 0
        c.error_rows = 0
        c.progress = 0
        c.last_error = ""
        db.commit()

        from backend.services.import_parser import start_import
        start_import(cid, content, file_data_list=file_list)

        return _ok({"connectorId": cid, "status": "running"})
    except RuntimeError as e:
        return _err(str(e), "ALREADY_RUNNING")
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-008 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-009: 진행 상태
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/status", methods=["GET"])
def import_status(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        from backend.services.import_parser import is_running
        return _ok({
            "connectorId": cid,
            "status": c.status,
            "progress": c.progress,
            "totalRows": c.total_rows,
            "importedRows": c.imported_rows,
            "errorRows": c.error_rows,
            "lastError": c.last_error,
            "isRunning": is_running(cid),
        })
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-010: 중지
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/stop", methods=["POST"])
def stop_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        c.status = "stopped"
        db.commit()
        return _ok(c.to_dict())
    except Exception as e:
        db.rollback()
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-012: 재발행 (저장된 원본 → MQTT)
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/republish", methods=["POST"])
def republish_import(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        if c.status not in ("completed",):
            return _err("완료된 Import만 재발행할 수 있습니다.", "INVALID_STATUS")

        c.status = "running"
        c.progress = 0
        c.last_error = ""
        db.commit()

        from backend.services.import_parser import republish
        republish(cid)

        return _ok({"connectorId": cid, "status": "running", "action": "republish"})
    except RuntimeError as e:
        return _err(str(e), "ALREADY_RUNNING")
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-012 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-013: 서버 경로 스캔
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/scan-path", methods=["POST"])
def scan_path(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        body = request.get_json(silent=True) or {}
        local_path = body.get("localPath", "") or c.local_path
        patterns = body.get("filePatterns") or c.file_patterns or ["*"]
        recursive = body.get("recursive", c.recursive if c.recursive is not None else True)

        if not local_path:
            return _err("서버 경로를 입력하세요.", "VALIDATION")

        from backend.services.import_parser import scan_local_path
        result = scan_local_path(local_path, patterns, recursive)

        if result.get("error"):
            return _err(result["error"], "SCAN_ERROR")

        # 설정 업데이트
        c.local_path = local_path
        c.file_patterns = patterns
        c.recursive = recursive
        c.source_mode = "local_path"
        db.commit()

        return _ok(result)
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-013 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-014: 서버 경로에서 실행
# ═══════════════════════════════════════════
@import_bp.route("/<int:cid>/execute-path", methods=["POST"])
def execute_from_path(cid):
    db = _db()
    try:
        c = db.query(ImportCollector).get(cid)
        if not c:
            return _err("Import collector not found", "NOT_FOUND", 404)

        if c.status == "running":
            return _err("이미 실행 중입니다.", "ALREADY_RUNNING")

        if not c.local_path:
            return _err("서버 경로가 설정되지 않았습니다. 먼저 경로를 스캔하세요.", "VALIDATION")

        c.status = "running"
        c.imported_rows = 0
        c.error_rows = 0
        c.progress = 0
        c.last_error = ""
        db.commit()

        from backend.services.import_parser import start_import_from_path
        start_import_from_path(cid)

        return _ok({"connectorId": cid, "status": "running", "source": "local_path"})
    except RuntimeError as e:
        return _err(str(e), "ALREADY_RUNNING")
    except Exception as e:
        db.rollback()
        logger.error(f"IMP-014 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()


# ═══════════════════════════════════════════
# IMP-011: 대상 스토리지 목록
# ═══════════════════════════════════════════
@import_bp.route("/targets", methods=["GET"])
def list_targets():
    db = _db()
    try:
        from backend.models.storage import TsdbConfig, RdbmsConfig
        tsdbs = db.query(TsdbConfig).all()
        rdbmss = db.query(RdbmsConfig).all()

        return _ok({
            "tsdb": [{"id": t.id, "name": t.name, "dbType": t.db_type} for t in tsdbs],
            "rdbms": [{"id": r.id, "name": r.name, "dbType": r.db_type} for r in rdbmss],
            "file": [
                {"bucket": "sdl-files", "label": "SDL Files"},
                {"bucket": "sdl-archive", "label": "SDL Archive"},
            ],
        })
    except Exception as e:
        logger.error(f"IMP-011 error: {e}")
        return _err(str(e), "SERVER_ERROR", 500)
    finally:
        db.close()
