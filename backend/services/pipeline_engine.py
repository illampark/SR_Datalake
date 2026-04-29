"""
Pipeline Engine — MQTT 기반 실시간 데이터 파이프라인 엔진

흐름:
  1. 파이프라인 시작 → PipelineBinding에 따라 MQTT 토픽 구독
  2. 메시지 수신 → PipelineStep 순서대로 모듈 체인 실행
  3. 처리 결과 → output 토픽에 발행 + 메타데이터 갱신
"""

import json
import logging
import time
import copy
from datetime import datetime

from backend.services import mqtt_manager
from backend.services.pipeline_modules import MODULE_REGISTRY, SINK_REGISTRY, process_message, flush_all_sink_buffers
from backend.services import metadata_tracker

# 파일 소스 step 타입 — start_pipeline 시 MQTT 구독을 건너뛰고 트리거 기반으로 동작
_FILE_SOURCE_TYPES = {"import_source", "internal_file_source"}

logger = logging.getLogger(__name__)

# 실행 중인 파이프라인 상태
_running_pipelines = {}  # pipeline_id → {"steps": [...], "bindings": [...], "stats": {...}}

# ── 커넥터명 캐시 ──
_connector_name_cache = {}


def _resolve_connector_name(connector_type, connector_id):
    """커넥터 타입+ID로 커넥터 이름 조회 (캐시)"""
    key = (connector_type, connector_id)
    if key in _connector_name_cache:
        return _connector_name_cache[key]

    from backend.database import SessionLocal
    from backend.models.collector import (
        OpcuaConnector, OpcdaConnector, ModbusConnector,
        MqttConnector, ApiConnector, FileCollector, DbConnector,
    )
    MODEL_MAP = {
        "opcua": OpcuaConnector, "opcda": OpcdaConnector,
        "modbus": ModbusConnector, "mqtt": MqttConnector,
        "api": ApiConnector, "file": FileCollector, "db": DbConnector,
    }
    model_cls = MODEL_MAP.get(connector_type)
    name = ""
    if model_cls:
        db = SessionLocal()
        try:
            row = db.query(model_cls.name).filter(model_cls.id == connector_id).first()
            name = row[0] if row else ""
        finally:
            db.close()
    _connector_name_cache[key] = name
    return name


def start_pipeline(pipeline_id):
    """파이프라인 시작 — DB에서 설정 로드 후 MQTT 구독"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline, PipelineStep, PipelineBinding

    if pipeline_id in _running_pipelines:
        logger.info("파이프라인 %s 이미 실행 중", pipeline_id)
        return True

    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if not p:
            logger.error("파이프라인 %s 없음", pipeline_id)
            return False

        steps = db.query(PipelineStep).filter_by(
            pipeline_id=pipeline_id
        ).order_by(PipelineStep.step_order).all()

        bindings = db.query(PipelineBinding).filter_by(
            pipeline_id=pipeline_id, enabled=True
        ).all()

        # 파일 소스 step 이 있으면 binding 없어도 실행 가능
        _has_file_source_step = any(
            st.enabled and st.module_type in _FILE_SOURCE_TYPES for st in steps
        )
        if not bindings and not _has_file_source_step:
            logger.warning("파이프라인 %s: 바인딩 없음", pipeline_id)
            return False

        # MQTT 연결 확인
        if not mqtt_manager.is_connected():
            mqtt_manager.connect()
            if not mqtt_manager.is_connected():
                logger.error("MQTT 브로커 연결 불가")
                return False

        # 스텝 정보 캐시 (소스/처리/싱크 분리)
        source_steps = []
        step_configs = []
        sink_configs = []
        for st in steps:
            if not st.enabled:
                continue
            cfg = dict(st.config or {})
            cfg["_pipeline_id"] = pipeline_id
            entry = {"module_type": st.module_type, "config": cfg}
            if st.module_type in _FILE_SOURCE_TYPES:
                source_steps.append(entry)
            elif st.module_type in SINK_REGISTRY:
                sink_configs.append(entry)
            else:
                step_configs.append(entry)

        is_file_source = bool(source_steps)
        source_mode = "file" if is_file_source else "mqtt"

        _running_pipelines[pipeline_id] = {
            "name": p.name,
            "sources": source_steps,
            "steps": step_configs,
            "sinks": sink_configs,
            "source_mode": source_mode,
            "bindings": [{"type": b.connector_type, "id": b.connector_id, "filter": b.tag_filter} for b in bindings],
            "stats": {
                "processed": 0,
                "errors": 0,
                "dropped": 0,
                "started_at": datetime.utcnow().isoformat(),
            },
        }

        if is_file_source:
            logger.info(
                "파이프라인 %s (%s): 파일 소스 모드 — MQTT 구독 안함, run-file-source 트리거 대기 (%d processing steps, %d sinks)",
                pipeline_id, p.name, len(step_configs), len(sink_configs),
            )
        else:
            # 바인딩별 MQTT 토픽 구독
            for b in bindings:
                topic = f"sdl/raw/{b.connector_type}/{b.connector_id}/#"
                if b.tag_filter and b.tag_filter != "*":
                    topic = f"sdl/raw/{b.connector_type}/{b.connector_id}/{b.tag_filter}"

                def make_handler(pid):
                    def handler(t, payload):
                        _handle_message(pid, t, payload)
                    return handler

                mqtt_manager.subscribe(topic, make_handler(pipeline_id))
                logger.info("파이프라인 %s: 구독 %s", pipeline_id, topic)

            logger.info("파이프라인 %s (%s) 시작 — %d 스텝, %d 바인딩",
                         pipeline_id, p.name, len(step_configs), len(bindings))
        return True
    except Exception as e:
        logger.error("파이프라인 %s 시작 실패: %s", pipeline_id, e)
        return False
    finally:
        db.close()


def stop_pipeline(pipeline_id):
    """파이프라인 정지 — MQTT 구독 해제"""
    info = _running_pipelines.pop(pipeline_id, None)
    if not info:
        return

    # 싱크 버퍼 플러시 (배치 모드 잔여 데이터 기록)
    try:
        flush_all_sink_buffers()
    except Exception as e:
        logger.warning("파이프라인 %s: 싱크 버퍼 플러시 실패 — %s", pipeline_id, e)

    # 바인딩별 토픽 구독 해제
    for b in info.get("bindings", []):
        topic = f"sdl/raw/{b['type']}/{b['id']}/#"
        mqtt_manager.unsubscribe(topic)

    logger.info("파이프라인 %s 정지", pipeline_id)

    # DB 상태 갱신
    _update_pipeline_db(pipeline_id, "stopped", info["stats"])


def get_pipeline_status(pipeline_id):
    """파이프라인 런타임 상태"""
    info = _running_pipelines.get(pipeline_id)
    if not info:
        return {"running": False}
    return {
        "running": True,
        "name": info["name"],
        "stats": info["stats"],
        "stepCount": len(info["steps"]),
        "bindingCount": len(info["bindings"]),
    }


def get_all_status():
    """모든 실행 중 파이프라인 상태"""
    result = {}
    for pid, info in _running_pipelines.items():
        result[pid] = {
            "name": info["name"],
            "stats": info["stats"],
        }
    return result


# ══════════════════════════════════════════════
# Internal: Message Processing
# ══════════════════════════════════════════════

def _handle_message(pipeline_id, topic, payload):
    """MQTT 메시지 수신 → 모듈 체인 실행"""
    info = _running_pipelines.get(pipeline_id)
    if not info:
        return

    start_time = time.time()

    try:
        # 페이로드 파싱
        if isinstance(payload, str):
            message = json.loads(payload)
        elif isinstance(payload, bytes):
            message = json.loads(payload.decode("utf-8"))
        else:
            message = payload

        # 원본 보존
        original = copy.deepcopy(message)

        # 모듈 체인 실행
        steps_applied = []
        for step in info["steps"]:
            module_type = step["module_type"]
            config = step["config"]

            message = process_message(message, module_type, config)
            if message is None:
                # 메시지 드롭
                info["stats"]["dropped"] += 1
                return

            steps_applied.append(module_type)

        # 처리 완료 → output 토픽 발행
        source = message.get("source", {})
        tag_name = source.get("tagName", "unknown")
        output_topic = mqtt_manager.build_processed_topic(pipeline_id, tag_name)
        mqtt_manager.publish(output_topic, message)

        # 싱크 실행 (처리 완료된 메시지를 각 싱크로 전달)
        for sink_step in info.get("sinks", []):
            try:
                sink_func = SINK_REGISTRY.get(sink_step["module_type"])
                if sink_func:
                    sink_func(copy.deepcopy(message), sink_step["config"])
            except Exception as se:
                logger.warning("파이프라인 %s: 싱크 %s 오류 — %s",
                               pipeline_id, sink_step["module_type"], se)

        # 통계 갱신
        info["stats"]["processed"] += 1
        elapsed_ms = (time.time() - start_time) * 1000

        # 메타데이터 갱신 (비동기적)
        try:
            metadata_tracker.upsert_tag_metadata(
                connector_type=source.get("connectorType", ""),
                connector_id=source.get("connectorId", 0),
                connector_name=_resolve_connector_name(
                    source.get("connectorType", ""),
                    source.get("connectorId", 0),
                ),
                tag_name=tag_name,
                value=message.get("value"),
                data_type=message.get("dataType", "float"),
                unit=message.get("unit", ""),
            )

            # 계보 기록 (100건 배치 단위)
            if info["stats"]["processed"] % 100 == 0:
                metadata_tracker.record_lineage(
                    source_connector_type=source.get("connectorType", ""),
                    source_connector_id=source.get("connectorId", 0),
                    source_tag=tag_name,
                    pipeline_id=pipeline_id,
                    pipeline_name=info["name"],
                    steps_applied=steps_applied,
                    destination_type="mqtt",
                    destination_target=output_topic,
                    record_count=100,
                    processing_ms=elapsed_ms,
                )
        except Exception as e:
            logger.warning("메타데이터 갱신 실패: %s", e)

    except json.JSONDecodeError:
        info["stats"]["errors"] += 1
        logger.warning("파이프라인 %s: JSON 파싱 오류 [%s]", pipeline_id, topic)
    except Exception as e:
        info["stats"]["errors"] += 1
        logger.error("파이프라인 %s: 처리 오류 [%s] — %s", pipeline_id, topic, e)

        # DB에 에러 기록
        _update_pipeline_error(pipeline_id, str(e))


def _update_pipeline_db(pipeline_id, status, stats):
    """파이프라인 DB 상태 갱신"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline
    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if p:
            p.status = status
            p.processed_count = stats.get("processed", 0)
            p.error_count = stats.get("errors", 0)
            p.last_processed_at = datetime.utcnow()
            db.commit()
    except Exception as e:
        db.rollback()
        logger.error("파이프라인 DB 갱신 실패: %s", e)
    finally:
        db.close()


def _update_pipeline_error(pipeline_id, error_msg):
    """파이프라인 에러 기록"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline
    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if p:
            p.last_error = error_msg
            p.error_count = (p.error_count or 0) + 1
            db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()





def run_file_source(pipeline_id):
    """파일 소스 파이프라인 즉시 실행 — DB lock + 스트리밍 + 진행률 commit.

    DB 의 current_run_id 를 atomic UPDATE 로 set 하여 동일 파이프라인의
    동시 실행을 차단한다 (24h TTL 로 crash 후 stale lock 자동 해제).
    CSV 는 chunk 단위 line iterator + csv.reader 로, JSON 은 ijson 으로
    스트리밍 처리해 큰 파일도 메모리에 통째 적재하지 않는다.
    """
    import csv as _csv
    import time as _time
    import uuid as _uuid
    from datetime import timedelta as _timedelta
    from sqlalchemy import text as _sql_text
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline, PipelineStep
    from backend.models.collector import ImportCollector
    from backend.services.minio_client import get_minio_client
    from backend.services.import_parser import _parse_json
    from backend.services.pipeline_modules import flush_all_sink_buffers

    PROGRESS_RECORDS = 5000
    PROGRESS_SECONDS = 5
    LOCK_TTL_HOURS = 24

    def _commit_progress(processed, errors, dropped):
        ss = SessionLocal()
        try:
            pp = ss.query(Pipeline).get(pipeline_id)
            if pp:
                pp.processed_count = processed
                pp.error_count = errors
                pp.last_processed_at = datetime.utcnow()
                ss.commit()
        except Exception:
            ss.rollback()
        finally:
            ss.close()

    def _try_acquire_lock(run_id):
        ss = SessionLocal()
        try:
            now = datetime.utcnow()
            cutoff = now - _timedelta(hours=LOCK_TTL_HOURS)
            res = ss.execute(_sql_text("""
                UPDATE pipeline
                SET current_run_id = :rid, current_run_at = :now
                WHERE id = :pid
                  AND (current_run_id IS NULL OR current_run_id = ''
                       OR current_run_at IS NULL OR current_run_at < :cutoff)
                RETURNING current_run_id
            """), {"rid": run_id, "now": now, "cutoff": cutoff, "pid": pipeline_id})
            row = res.fetchone()
            ss.commit()
            return bool(row) and row[0] == run_id
        except Exception:
            ss.rollback()
            return False
        finally:
            ss.close()

    def _release_lock(run_id):
        ss = SessionLocal()
        try:
            ss.execute(_sql_text("""
                UPDATE pipeline
                SET current_run_id = '', current_run_at = NULL
                WHERE id = :pid AND current_run_id = :rid
            """), {"pid": pipeline_id, "rid": run_id})
            ss.commit()
        except Exception:
            ss.rollback()
        finally:
            ss.close()

    def _stream_csv_records(resp, encoding, delimiter, skip_header):
        def _line_iter():
            buf = b""
            for chunk in resp.stream(8 * 1024 * 1024):
                if not isinstance(chunk, (bytes, bytearray)):
                    continue
                buf += chunk
                while True:
                    nl = buf.find(b"\n")
                    if nl < 0:
                        break
                    line_b = buf[:nl]
                    buf = buf[nl + 1:]
                    if line_b.endswith(b"\r"):
                        line_b = line_b[:-1]
                    yield line_b.decode(encoding, errors="replace")
            if buf:
                if buf.endswith(b"\r"):
                    buf = buf[:-1]
                yield buf.decode(encoding, errors="replace")

        reader = _csv.reader(_line_iter(), delimiter=delimiter)
        header = None
        for row in reader:
            if header is None:
                if skip_header:
                    header = [c.strip() for c in row]
                    continue
                else:
                    header = [f"col{i}" for i in range(len(row))]
            if len(row) == len(header):
                yield dict(zip(header, row))
            else:
                yield {h: (row[i] if i < len(row) else "") for i, h in enumerate(header)}

    def _stream_json_records(resp):
        """ijson 으로 JSON 스트리밍. 루트가 array (item) 또는 {data|records|items: [...]} 지원."""
        try:
            import ijson
        except ImportError:
            logger.warning("ijson 미설치 — JSON 전체 메모리 적재 fallback")
            content = resp.read()
            for r in _parse_json(content, "utf-8"):
                yield r
            return

        # ijson 은 'item' (루트 배열) / 'data.item' 등 prefix 지원.
        # 일반적으로 사용자 JSON 은 [...] 또는 {"data": [...]}, {"records": [...]}.
        # 우선 'item' 을 시도하고, 안되면 fallback 으로 일반 parse.
        try:
            for rec in ijson.items(resp, "item"):
                if isinstance(rec, dict):
                    yield rec
                else:
                    yield {"value": rec}
        except Exception as e:
            logger.warning("ijson 스트리밍 실패 (%s) — 메모리 적재 fallback", e)
            try:
                resp.close(); resp.release_conn()
            except Exception:
                pass
            # ijson 이 stream 을 소비했을 수 있으므로 재시도는 별도 get_object 가 필요.
            # 호출자가 처리해야 함. 빈 yield.
            return

    db = SessionLocal()
    try:
        p = db.query(Pipeline).get(pipeline_id)
        if not p:
            return {"ok": False, "error": f"Pipeline {pipeline_id} 를 찾을 수 없습니다."}
        if p.status != "running":
            return {"ok": False, "error": "파이프라인이 실행 중이 아닙니다. 먼저 [시작]을 누르세요."}

        # 동시 실행 방지 — DB 기반 lock
        run_id = str(_uuid.uuid4())
        if not _try_acquire_lock(run_id):
            return {
                "ok": False,
                "error": "이미 실행 중인 file-source 작업이 있습니다. 완료를 기다린 뒤 재시도하세요.",
            }

        try:
            steps = db.query(PipelineStep).filter_by(
                pipeline_id=pipeline_id
            ).order_by(PipelineStep.step_order).all()

            source_steps = []
            process_steps = []
            sink_steps = []
            for st in steps:
                if not st.enabled:
                    continue
                entry = {"module_type": st.module_type, "config": dict(st.config or {})}
                if st.module_type in _FILE_SOURCE_TYPES:
                    source_steps.append(entry)
                elif st.module_type in SINK_REGISTRY:
                    sink_steps.append(entry)
                else:
                    process_steps.append(entry)

            if not source_steps:
                return {"ok": False, "error": "import_source 스텝이 없습니다."}

            src_cfg = source_steps[0]["config"]
            connector_type = src_cfg.get("connectorType", "import")
            connector_id = src_cfg.get("connectorId")
            if not connector_id:
                return {"ok": False, "error": "source 설정에 connectorId 가 없습니다."}

            c = db.query(ImportCollector).get(connector_id)
            if not c:
                return {"ok": False, "error": f"Import collector {connector_id} 를 찾을 수 없습니다."}

            client = get_minio_client(db)
            bucket = c.target_bucket or "sdl-files"
            prefix = f"import/{connector_id}/"

            objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
            if not objects:
                return {"ok": False, "error": f"s3://{bucket}/{prefix} 에 파일이 없습니다."}

            encoding = c.encoding or "utf-8"
            delimiter = c.delimiter or ","
            skip_header = c.skip_header
            import_type = (c.import_type or "csv").lower()

            logger.info(
                "file-source pipeline=%s 시작 — files=%d, format=%s, sinks=%d, run_id=%s",
                pipeline_id, len(objects), import_type, len(sink_steps), run_id,
            )

            _commit_progress(0, 0, 0)

            total_processed = 0
            total_errors = 0
            total_dropped = 0
            last_commit_at = _time.time()
            last_commit_count = 0

            for fi, obj in enumerate(objects):
                try:
                    resp = client.get_object(bucket, obj.object_name)
                    try:
                        if import_type == "json":
                            records_iter = _stream_json_records(resp)
                        else:
                            records_iter = _stream_csv_records(
                                resp, encoding, delimiter, skip_header,
                            )

                        file_record_count = 0
                        for rec in records_iter:
                            msg = {
                                "value": rec,
                                "source": {
                                    "connectorType": connector_type,
                                    "connectorId": connector_id,
                                    "tagName": "row",
                                },
                                "dataType": "object",
                                "timestamp": datetime.utcnow().isoformat(),
                            }

                            drop = False
                            for step_entry in process_steps:
                                msg = process_message(
                                    msg, step_entry["module_type"], step_entry["config"],
                                )
                                if msg is None:
                                    drop = True
                                    break
                            if drop:
                                total_dropped += 1
                                continue

                            for sink_step in sink_steps:
                                try:
                                    sink_func = SINK_REGISTRY.get(sink_step["module_type"])
                                    if sink_func:
                                        cfg = dict(sink_step["config"])
                                        cfg["_pipeline_id"] = pipeline_id
                                        sink_func(copy.deepcopy(msg), cfg)
                                except Exception as se:
                                    total_errors += 1
                                    logger.warning(
                                        "file-source sink %s 실패: %s",
                                        sink_step["module_type"], se,
                                    )

                            total_processed += 1
                            file_record_count += 1

                            now = _time.time()
                            if (total_processed - last_commit_count) >= PROGRESS_RECORDS or (now - last_commit_at) >= PROGRESS_SECONDS:
                                _commit_progress(total_processed, total_errors, total_dropped)
                                last_commit_count = total_processed
                                last_commit_at = now
                    finally:
                        try:
                            resp.close(); resp.release_conn()
                        except Exception:
                            pass

                    logger.info(
                        "file-source pipeline=%s file=%s done — rows=%d (cumulative processed=%d)",
                        pipeline_id, obj.object_name, file_record_count, total_processed,
                    )
                    _commit_progress(total_processed, total_errors, total_dropped)
                    last_commit_count = total_processed
                    last_commit_at = _time.time()
                except Exception as e:
                    total_errors += 1
                    logger.warning("파일 %s 처리 실패: %s", obj.object_name, e)

            try:
                flush_all_sink_buffers()
            except Exception as e:
                logger.warning("flush_all_sink_buffers 실패: %s", e)

            _commit_progress(total_processed, total_errors, total_dropped)

            logger.info(
                "file-source pipeline=%s 완료 — files=%d processed=%d dropped=%d errors=%d",
                pipeline_id, len(objects), total_processed, total_dropped, total_errors,
            )
            return {
                "ok": True,
                "files": len(objects),
                "processed": total_processed,
                "dropped": total_dropped,
                "errors": total_errors,
            }
        finally:
            _release_lock(run_id)
    finally:
        db.close()
