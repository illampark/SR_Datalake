"""
Pipeline Processing Modules — 7개 전처리 모듈

각 모듈은 동일한 인터페이스: process(message, config) → message | None
- message: {"source": {...}, "value": ..., "dataType": ..., "unit": ..., "quality": ..., "timestamp": ...}
- config: PipelineStep.config JSON
- 반환: 처리된 message (drop 시 None)
"""

import io
import json
import logging
import math
import statistics
from collections import deque
from datetime import datetime

logger = logging.getLogger(__name__)

# 이동 윈도우 캐시 (태그별)
_window_cache = {}  # key: "{pipeline_id}:{tag_name}" → deque


# ══════════════════════════════════════════════
# 1. Normalize — 데이터 정규화
# ══════════════════════════════════════════════

def module_normalize(message, config):
    """
    config:
      targetType: "float" | "int" | "string"
      nullStrategy: "skip" | "zero" | "last"
      trimWhitespace: true
      defaultValue: 0
    """
    value = message.get("value")
    target = config.get("targetType", "float")

    # Null 처리
    if value is None or value == "" or value == "null":
        strategy = config.get("nullStrategy", "skip")
        if strategy == "skip":
            return None  # drop
        elif strategy == "zero":
            value = 0
        elif strategy == "last":
            value = config.get("_lastValue", 0)
        else:
            value = config.get("defaultValue", 0)

    # 공백 트림
    if isinstance(value, str) and config.get("trimWhitespace", True):
        value = value.strip()

    # 타입 변환
    try:
        if target == "float":
            value = float(value)
        elif target == "int":
            value = int(float(value))
        elif target == "string":
            value = str(value)
    except (ValueError, TypeError):
        value = config.get("defaultValue", 0)

    config["_lastValue"] = value
    message["value"] = value
    message["dataType"] = target
    return message


# ══════════════════════════════════════════════
# 2. Unit Convert — 단위 변환
# ══════════════════════════════════════════════

# 내장 변환 테이블
_CONVERSIONS = {
    ("celsius", "fahrenheit"): lambda v: v * 9 / 5 + 32,
    ("fahrenheit", "celsius"): lambda v: (v - 32) * 5 / 9,
    ("celsius", "kelvin"): lambda v: v + 273.15,
    ("kelvin", "celsius"): lambda v: v - 273.15,
    ("bar", "psi"): lambda v: v * 14.5038,
    ("psi", "bar"): lambda v: v / 14.5038,
    ("kpa", "psi"): lambda v: v * 0.145038,
    ("psi", "kpa"): lambda v: v / 0.145038,
    ("mm", "inch"): lambda v: v / 25.4,
    ("inch", "mm"): lambda v: v * 25.4,
    ("m", "ft"): lambda v: v * 3.28084,
    ("ft", "m"): lambda v: v / 3.28084,
    ("kg", "lb"): lambda v: v * 2.20462,
    ("lb", "kg"): lambda v: v / 2.20462,
    ("lpm", "gpm"): lambda v: v * 0.264172,
    ("gpm", "lpm"): lambda v: v / 0.264172,
}


def module_unit_convert(message, config):
    """
    config:
      sourceUnit: "celsius"
      targetUnit: "fahrenheit"
      factor: 1.0  (커스텀 계수)
      offset: 0.0  (커스텀 오프셋)
    """
    value = message.get("value")
    if value is None:
        return message

    try:
        value = float(value)
    except (ValueError, TypeError):
        return message

    src = config.get("sourceUnit", "").lower()
    tgt = config.get("targetUnit", "").lower()

    if src and tgt and src != tgt:
        key = (src, tgt)
        if key in _CONVERSIONS:
            value = _CONVERSIONS[key](value)
        else:
            # 커스텀 선형 변환: result = value * factor + offset
            factor = config.get("factor", 1.0)
            offset = config.get("offset", 0.0)
            value = value * factor + offset

    message["value"] = round(value, 6)
    message["unit"] = tgt or message.get("unit", "")
    return message


# ══════════════════════════════════════════════
# 3. Filter — 필터링
# ══════════════════════════════════════════════

def module_filter(message, config):
    """
    config:
      filterType: "range" | "condition" | "deadband"
      field: "value"
      minValue: 0
      maxValue: 100
      condition: "value > 0"
      action: "drop" | "flag" | "clamp"
      deadband: 0.5
    """
    value = message.get("value")
    filter_type = config.get("filterType", "range")
    action = config.get("action", "drop")

    try:
        fval = float(value) if value is not None else None
    except (ValueError, TypeError):
        fval = None

    out_of_range = False

    if filter_type == "range" and fval is not None:
        min_v = config.get("minValue")
        max_v = config.get("maxValue")
        if min_v is not None and fval < min_v:
            out_of_range = True
        if max_v is not None and fval > max_v:
            out_of_range = True

    elif filter_type == "deadband" and fval is not None:
        deadband = config.get("deadband", 0.5)
        last = config.get("_lastFilterValue")
        if last is not None and abs(fval - last) < deadband:
            return None  # 변화량이 deadband 이내면 drop
        config["_lastFilterValue"] = fval

    if out_of_range:
        if action == "drop":
            return None
        elif action == "flag":
            message["_flagged"] = True
            message["quality"] = max(0, message.get("quality", 100) - 30)
        elif action == "clamp":
            min_v = config.get("minValue")
            max_v = config.get("maxValue")
            if min_v is not None and fval < min_v:
                message["value"] = min_v
            if max_v is not None and fval > max_v:
                message["value"] = max_v

    return message


# ══════════════════════════════════════════════
# 4. Anomaly Detection — 이상치 탐지
# ══════════════════════════════════════════════

def module_anomaly(message, config):
    """
    config:
      method: "zscore" | "iqr" | "moving_avg" | "sigma"
      threshold: 3.0
      windowSize: 60
      action: "flag" | "drop" | "clamp" | "replace"
      replaceStrategy: "mean" | "median" | "last"
    """
    value = message.get("value")
    try:
        fval = float(value)
    except (ValueError, TypeError):
        return message

    method = config.get("method", "zscore")
    threshold = config.get("threshold", 3.0)
    window_size = config.get("windowSize", 60)
    action = config.get("action", "flag")

    # 윈도우 관리
    tag = message.get("source", {}).get("tagName", "unknown")
    cache_key = f"{config.get('_pipeline_id', 0)}:{tag}"
    if cache_key not in _window_cache:
        _window_cache[cache_key] = deque(maxlen=window_size)
    window = _window_cache[cache_key]

    is_anomaly = False

    if len(window) >= 3:
        mean = statistics.mean(window)
        stdev = statistics.stdev(window) if len(window) > 1 else 0

        if method == "zscore" and stdev > 0:
            z = abs(fval - mean) / stdev
            is_anomaly = z > threshold

        elif method == "iqr":
            sorted_w = sorted(window)
            n = len(sorted_w)
            q1 = sorted_w[n // 4]
            q3 = sorted_w[3 * n // 4]
            iqr = q3 - q1
            is_anomaly = fval < (q1 - threshold * iqr) or fval > (q3 + threshold * iqr)

        elif method == "moving_avg":
            is_anomaly = abs(fval - mean) > threshold * (stdev if stdev > 0 else 1)

        elif method == "sigma":
            is_anomaly = abs(fval - mean) > threshold * stdev if stdev > 0 else False

    window.append(fval)

    if is_anomaly:
        if action == "drop":
            return None
        elif action == "flag":
            message["_anomaly"] = True
            message["quality"] = max(0, message.get("quality", 100) - 50)
        elif action == "replace":
            strategy = config.get("replaceStrategy", "mean")
            if strategy == "mean" and window:
                message["value"] = round(statistics.mean(window), 6)
            elif strategy == "median" and window:
                message["value"] = round(statistics.median(window), 6)
            elif strategy == "last" and len(window) >= 2:
                message["value"] = window[-2]
        elif action == "clamp":
            mean = statistics.mean(window) if window else fval
            stdev = statistics.stdev(window) if len(window) > 1 else 0
            lo = mean - threshold * stdev
            hi = mean + threshold * stdev
            message["value"] = max(lo, min(hi, fval))

    return message


# ══════════════════════════════════════════════
# 5. Aggregate — 데이터 집계
# ══════════════════════════════════════════════

_agg_buffers = {}  # key → {"values": [], "start": datetime, "count": int}


def module_aggregate(message, config):
    """
    config:
      windowSeconds: 60
      functions: ["avg", "min", "max", "count", "sum"]
      emitMode: "end"  (윈도우 종료 시 방출)
    """
    value = message.get("value")
    try:
        fval = float(value)
    except (ValueError, TypeError):
        return message

    window_sec = config.get("windowSeconds", 60)
    functions = config.get("functions", ["avg"])
    tag = message.get("source", {}).get("tagName", "unknown")
    cache_key = f"agg:{config.get('_pipeline_id', 0)}:{tag}"

    now = datetime.utcnow()

    if cache_key not in _agg_buffers:
        _agg_buffers[cache_key] = {"values": [], "start": now, "count": 0}

    buf = _agg_buffers[cache_key]
    buf["values"].append(fval)
    buf["count"] += 1

    elapsed = (now - buf["start"]).total_seconds()
    if elapsed < window_sec:
        return None  # 아직 윈도우 내, 버퍼링

    # 윈도우 종료 → 집계 결과 방출
    vals = buf["values"]
    result = {}
    if "avg" in functions and vals:
        result["avg"] = round(statistics.mean(vals), 6)
    if "min" in functions and vals:
        result["min"] = min(vals)
    if "max" in functions and vals:
        result["max"] = max(vals)
    if "count" in functions:
        result["count"] = len(vals)
    if "sum" in functions and vals:
        result["sum"] = round(sum(vals), 6)

    # 버퍼 리셋
    _agg_buffers[cache_key] = {"values": [], "start": now, "count": 0}

    message["value"] = result
    message["dataType"] = "aggregate"
    message["_aggregated"] = True
    message["_windowStart"] = buf["start"].isoformat()
    message["_windowEnd"] = now.isoformat()
    return message


# ══════════════════════════════════════════════
# 6. Enrich — 데이터 보강
# ══════════════════════════════════════════════

def module_enrich(message, config):
    """
    config:
      fields: {"area": "라인1", "equipment": "CNC-01", "processType": "가공"}
      lookupTable: "tag_metadata"  (미래 확장: 외부 테이블 조인)
      addTimestamp: true
    """
    # 고정 필드 추가
    fields = config.get("fields", {})
    if fields:
        if "enrichment" not in message:
            message["enrichment"] = {}
        message["enrichment"].update(fields)

    # 타임스탬프 추가
    if config.get("addTimestamp", True):
        message["enrichment"] = message.get("enrichment", {})
        message["enrichment"]["processedAt"] = datetime.utcnow().isoformat() + "Z"

    # 메타데이터 조인 (태그 메타에서 unit, description 추가)
    if config.get("lookupTable") == "tag_metadata":
        try:
            from backend.database import SessionLocal
            from backend.models.metadata import TagMetadata
            src = message.get("source", {})
            db = SessionLocal()
            meta = db.query(TagMetadata).filter_by(
                connector_type=src.get("connectorType", ""),
                connector_id=src.get("connectorId", 0),
                tag_name=src.get("tagName", ""),
            ).first()
            if meta:
                message["enrichment"]["unit"] = meta.unit or ""
                message["enrichment"]["qualityScore"] = meta.quality_score
                message["enrichment"]["connectorName"] = meta.connector_name
            db.close()
        except Exception as e:
            logger.warning("Enrich 메타 조인 실패: %s", e)

    return message


# ══════════════════════════════════════════════
# 7. Script — 커스텀 스크립트
# ══════════════════════════════════════════════

def module_script(message, config):
    """
    config:
      language: "python"
      code: "value = message['value'] * 2; message['value'] = value"
      timeout: 5
    """
    code = config.get("code", "")
    if not code:
        return message

    language = config.get("language", "python")
    if language != "python":
        logger.warning("지원하지 않는 스크립트 언어: %s", language)
        return message

    # 안전한 실행 환경
    safe_globals = {
        "__builtins__": {
            "abs": abs, "round": round, "min": min, "max": max,
            "int": int, "float": float, "str": str, "len": len,
            "sum": sum, "pow": pow, "bool": bool,
            "True": True, "False": False, "None": None,
        },
        "math": math,
        "message": message,
        "value": message.get("value"),
    }

    try:
        exec(code, safe_globals)
        # exec 후 message/value 변경 반영
        if "message" in safe_globals:
            message = safe_globals["message"]
        if "value" in safe_globals and safe_globals["value"] != message.get("value"):
            message["value"] = safe_globals["value"]
        return message
    except Exception as e:
        logger.error("스크립트 실행 오류: %s", e)
        message["_script_error"] = str(e)
        return message


# ══════════════════════════════════════════════
# 8. Sink — 내부 시계열DB
# ══════════════════════════════════════════════

_sink_buffers = {}  # cache_key → [rows]

QUALITY_MAP = {"good": 100, "ok": 80, "bad": 0, "uncertain": 50}


def _parse_quality(raw):
    """quality 값을 Integer(0-100)로 변환"""
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str):
        low = raw.strip().lower()
        if low in QUALITY_MAP:
            return QUALITY_MAP[low]
        try:
            return int(low)
        except (ValueError, TypeError):
            return 100
    return 100


def sink_internal_tsdb(message, config):
    """내부 시계열DB 싱크 — TimeSeriesData 테이블에 데이터 기록

    config:
      tsdbId: 1                 (TsdbConfig.id)
      tsdbName: "Production DB" (표시용)
      measurement: "sensor_data"
      writeMode: "single" | "batch"
      batchSize: 100
    """
    tsdb_id = config.get("tsdbId", 0)
    measurement = config.get("measurement", "sensor_data")
    write_mode = config.get("writeMode", "single")
    pipeline_id = config.get("_pipeline_id", 0)

    source = message.get("source", {})
    tag_name = source.get("tagName", "unknown")
    connector_type = source.get("connectorType", "")
    connector_id = source.get("connectorId", 0)

    raw_value = message.get("value")
    data_type = message.get("dataType", "float")

    num_value = None
    str_value = ""
    if isinstance(raw_value, (int, float)):
        num_value = float(raw_value)
    elif isinstance(raw_value, dict):
        str_value = str(raw_value)
        if "avg" in raw_value:
            num_value = raw_value["avg"]
    elif isinstance(raw_value, str):
        str_value = raw_value
        try:
            num_value = float(raw_value)
        except (ValueError, TypeError):
            pass
    elif raw_value is not None:
        str_value = str(raw_value)

    ts_str = message.get("timestamp")
    ts = None
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "").replace("+00:00", ""))
        except (ValueError, TypeError):
            pass
    if ts is None:
        ts = datetime.utcnow()

    row = {
        "tsdb_id": tsdb_id,
        "measurement": measurement,
        "tag_name": tag_name,
        "connector_type": connector_type,
        "connector_id": connector_id,
        "pipeline_id": pipeline_id,
        "value": num_value,
        "value_str": str_value,
        "data_type": data_type,
        "unit": message.get("unit", ""),
        "quality": _parse_quality(message.get("quality", 100)),
        "tags": {k: v for k, v in source.items()
                 if k not in ("tagName", "connectorType", "connectorId")},
        "timestamp": ts,
    }

    if write_mode == "batch":
        batch_size = config.get("batchSize", 100)
        cache_key = f"tsdb_sink:{pipeline_id}:{tsdb_id}"
        if cache_key not in _sink_buffers:
            _sink_buffers[cache_key] = []
        _sink_buffers[cache_key].append(row)
        if len(_sink_buffers[cache_key]) >= batch_size:
            _flush_tsdb_batch(cache_key)
    else:
        _write_tsdb_rows([row])

    return message  # 싱크는 메시지를 패스스루


def _flush_tsdb_batch(cache_key):
    rows = _sink_buffers.pop(cache_key, [])
    if rows:
        _write_tsdb_rows(rows)


def _write_tsdb_rows(rows):
    try:
        from backend.database import SessionLocal
        from backend.models.storage import TimeSeriesData
        db = SessionLocal()
        try:
            for r in rows:
                db.add(TimeSeriesData(**r))
            db.commit()
            logger.debug("TSDB sink: %d rows written", len(rows))
        except Exception as e:
            db.rollback()
            logger.error("TSDB sink write error: %s", e)
        finally:
            db.close()
    except Exception as e:
        logger.error("TSDB sink DB session error: %s", e)


def flush_all_sink_buffers():
    """남은 배치 버퍼 모두 플러시 (파이프라인 정지 시 호출)"""
    for key in list(_sink_buffers.keys()):
        if key.startswith("rdbms_sink:"):
            _flush_rdbms_batch(key)
        elif key.startswith("file_sink:"):
            _flush_file_batch(key)
        else:
            _flush_tsdb_batch(key)


# ── 버퍼 상태 조회 / 개별 플러시 (engine_buffer API 용) ──

def get_sink_buffer_status():
    """싱크 버퍼 상태 스냅샷 반환"""
    result = {}
    for key, rows in list(_sink_buffers.items()):
        if key.startswith("tsdb_sink:"):
            buf_type = "tsdb"
        elif key.startswith("rdbms_sink:"):
            buf_type = "rdbms"
        elif key.startswith("file_sink:"):
            buf_type = "file"
        else:
            buf_type = "unknown"
        result[key] = {"count": len(rows), "type": buf_type}
    return result


def get_agg_buffer_status():
    """집계 버퍼 상태 스냅샷 반환"""
    result = {}
    for key, buf in list(_agg_buffers.items()):
        result[key] = {
            "valuesCount": len(buf.get("values", [])),
            "count": buf.get("count", 0),
            "startedAt": buf["start"].isoformat() if buf.get("start") else None,
        }
    return result


def get_window_cache_status():
    """이상치 탐지 윈도우 캐시 상태 스냅샷 반환"""
    result = {}
    for key, window in list(_window_cache.items()):
        result[key] = {
            "size": len(window),
            "maxlen": window.maxlen if hasattr(window, "maxlen") else None,
        }
    return result


def flush_single_sink_buffer(buffer_key):
    """개별 싱크 버퍼 플러시. 플러시된 건수 반환, 키 없으면 -1"""
    if buffer_key not in _sink_buffers:
        return -1
    count = len(_sink_buffers.get(buffer_key, []))
    if buffer_key.startswith("rdbms_sink:"):
        _flush_rdbms_batch(buffer_key)
    elif buffer_key.startswith("file_sink:"):
        _flush_file_batch(buffer_key)
    else:
        _flush_tsdb_batch(buffer_key)
    return count


# ══════════════════════════════════════════════
# 9. Sink — 내부 관계형DB (RDBMS)
# ══════════════════════════════════════════════

def sink_internal_rdbms(message, config):
    """내부 관계형DB 싱크 — RdbmsConfig 인스턴스의 지정 테이블에 데이터 기록

    config:
      rdbmsId: 1                 (RdbmsConfig.id)
      rdbmsName: "SDL RDBMS"     (표시용)
      tableName: "pipeline_data" (대상 테이블명)
      writeMode: "single" | "batch"
      batchSize: 100
      columnMapping: "auto" | "flatten"
        - auto: source/value/metadata 를 고정 컬럼으로 기록
        - flatten: value가 dict이면 각 key를 컬럼으로 펼쳐서 기록
    """
    rdbms_id = config.get("rdbmsId", 0)
    table_name = config.get("tableName") or "pipeline_data"
    write_mode = config.get("writeMode", "single")
    column_mapping = config.get("columnMapping", "auto")
    pipeline_id = config.get("_pipeline_id", 0)

    source = message.get("source", {})
    tag_name = source.get("tagName", "unknown")
    connector_type = source.get("connectorType", "")
    connector_id = source.get("connectorId", 0)
    raw_value = message.get("value")

    # 타임스탬프 파싱
    ts_str = message.get("timestamp")
    ts = None
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "").replace("+00:00", ""))
        except (ValueError, TypeError):
            pass
    if ts is None:
        ts = datetime.utcnow()

    # 행 데이터 구성
    if column_mapping == "flatten" and isinstance(raw_value, dict):
        # value dict의 각 key를 컬럼으로 펼침 + 메타 컬럼 추가
        row = dict(raw_value)
        row["_pipeline_id"] = pipeline_id
        row["_connector_type"] = connector_type
        row["_connector_id"] = connector_id
        row["_tag_name"] = tag_name
        row["_collected_at"] = ts.isoformat()
    else:
        # 고정 스키마: 표준 컬럼으로 기록
        num_value = None
        str_value = ""
        if isinstance(raw_value, (int, float)):
            num_value = float(raw_value)
        elif isinstance(raw_value, dict):
            import json as _json
            str_value = _json.dumps(raw_value, ensure_ascii=False, default=str)
        elif isinstance(raw_value, str):
            str_value = raw_value
            try:
                num_value = float(raw_value)
            except (ValueError, TypeError):
                pass
        elif raw_value is not None:
            str_value = str(raw_value)

        row = {
            "pipeline_id": pipeline_id,
            "connector_type": connector_type,
            "connector_id": connector_id,
            "tag_name": tag_name,
            "value_num": num_value,
            "value_str": str_value,
            "data_type": message.get("dataType", "float"),
            "unit": message.get("unit", ""),
            "quality": _parse_quality(message.get("quality", 100)),
            "collected_at": ts.isoformat(),
        }

    cache_entry = {"rdbms_id": rdbms_id, "table_name": table_name, "row": row}

    if write_mode == "batch":
        batch_size = config.get("batchSize", 100)
        cache_key = f"rdbms_sink:{pipeline_id}:{rdbms_id}:{table_name}"
        if cache_key not in _sink_buffers:
            _sink_buffers[cache_key] = []
        _sink_buffers[cache_key].append(cache_entry)
        if len(_sink_buffers[cache_key]) >= batch_size:
            _flush_rdbms_batch(cache_key)
    else:
        _write_rdbms_rows([cache_entry])

    return message


def _flush_rdbms_batch(cache_key):
    entries = _sink_buffers.pop(cache_key, [])
    if entries:
        _write_rdbms_rows(entries)


def _write_rdbms_rows(entries):
    """RdbmsConfig 기반으로 외부 RDBMS에 INSERT"""
    if not entries:
        return

    rdbms_id = entries[0]["rdbms_id"]
    table_name = entries[0]["table_name"]
    rows = [e["row"] for e in entries]

    try:
        from backend.database import SessionLocal
        from backend.models.storage import RdbmsConfig

        db = SessionLocal()
        try:
            rdbms = db.query(RdbmsConfig).get(rdbms_id)
            if not rdbms:
                logger.error("RDBMS sink: RdbmsConfig %d not found", rdbms_id)
                return
            db_type = (rdbms.db_type or "").lower()
            host = rdbms.host
            port = rdbms.port
            database = rdbms.database_name
            username = rdbms.username or "sdl_user"
            password = rdbms.password or "sdl_password_2025"
            schema = rdbms.schema_name or "public"
        finally:
            db.close()

        # 모든 행의 컬럼 합집합
        all_cols = []
        seen = set()
        for r in rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    all_cols.append(k)

        if "mysql" in db_type or "maria" in db_type:
            _rdbms_write_mysql(host, port, database, username, password,
                               table_name, all_cols, rows)
        else:
            _rdbms_write_pg(host, port, database, username, password,
                            schema, table_name, all_cols, rows)

        logger.debug("RDBMS sink: %d rows written to %s", len(rows), table_name)
    except Exception as e:
        logger.error("RDBMS sink write error: %s", e)


def _rdbms_write_mysql(host, port, database, username, password,
                        table_name, columns, rows):
    import pymysql
    conn = pymysql.connect(
        host=host, port=port, database=database,
        user=username, password=password,
        charset="utf8mb4", connect_timeout=10,
    )
    try:
        cur = conn.cursor()
        # 테이블 자동 생성 (없으면)
        col_defs = ", ".join(
            f"`{c}` TEXT" for c in columns
        )
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS `{table_name}` "
            f"(id INT AUTO_INCREMENT PRIMARY KEY, {col_defs})"
        )
        # INSERT
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(f"`{c}`" for c in columns)
        sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
        values = [
            tuple(str(r.get(c, "")) if r.get(c) is not None else None for c in columns)
            for r in rows
        ]
        cur.executemany(sql, values)
        conn.commit()
    finally:
        conn.close()


def _rdbms_write_pg(host, port, database, username, password,
                     schema, table_name, columns, rows):
    import psycopg2
    conn = psycopg2.connect(
        host=host, port=port, dbname=database or "postgres",
        user=username, password=password,
        connect_timeout=10,
    )
    try:
        cur = conn.cursor()
        # 테이블 자동 생성 (없으면)
        col_defs = ", ".join(
            f'"{c}" TEXT' for c in columns
        )
        full_table = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {full_table} "
            f'(id SERIAL PRIMARY KEY, {col_defs})'
        )
        # INSERT
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(f'"{c}"' for c in columns)
        sql = f"INSERT INTO {full_table} ({col_names}) VALUES ({placeholders})"
        values = [
            tuple(str(r.get(c, "")) if r.get(c) is not None else None for c in columns)
            for r in rows
        ]
        cur.executemany(sql, values)
        conn.commit()
    finally:
        conn.close()


# ══════════════════════════════════════════════
# 10. Sink — 내부 파일 스토리지 (MinIO S3)
# ══════════════════════════════════════════════

def _get_minio():
    """MinIO 클라이언트 생성 (DB 설정 우선, config.py 폴백)"""
    from backend.database import SessionLocal
    from backend.services.minio_client import get_minio_client
    db = SessionLocal()
    try:
        return get_minio_client(db)
    finally:
        db.close()


def sink_internal_file(message, config):
    """내부 파일 스토리지 싱크 — MinIO에 파일(JSONL/CSV/JSON)로 데이터 기록

    config:
      bucket: "sdl-files"                     (MinIO 버킷)
      pathPrefix: "pipeline/"                  (경로 접두사)
      fileFormat: "jsonl" | "csv" | "json"     (파일 포맷)
      fileNamePattern: "pipeline_{pipeline_id}_{date}" (파일명 패턴)
      batchSize: 50                            (배치 크기)
    """
    pipeline_id = config.get("_pipeline_id", 0)
    bucket = config.get("bucket") or "sdl-files"
    path_prefix = config.get("pathPrefix") or "pipeline/"
    file_format = config.get("fileFormat") or "jsonl"
    file_name_pattern = config.get("fileNamePattern") or "pipeline_{pipeline_id}_{date}"
    batch_size = config.get("batchSize") or 50

    # 메시지에서 행 데이터 추출 (RDBMS sink auto 매핑과 동일)
    source = message.get("source", {})
    tag_name = source.get("tagName", "unknown")
    connector_type = source.get("connectorType", "")
    connector_id = source.get("connectorId", 0)
    raw_value = message.get("value")

    ts_str = message.get("timestamp")
    ts = None
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "").replace("+00:00", ""))
        except (ValueError, TypeError):
            pass
    if ts is None:
        ts = datetime.utcnow()

    num_value = None
    str_value = ""
    if isinstance(raw_value, (int, float)):
        num_value = float(raw_value)
    elif isinstance(raw_value, dict):
        str_value = json.dumps(raw_value, ensure_ascii=False, default=str)
    elif isinstance(raw_value, str):
        str_value = raw_value
        try:
            num_value = float(raw_value)
        except (ValueError, TypeError):
            pass
    elif raw_value is not None:
        str_value = str(raw_value)

    row = {
        "pipeline_id": pipeline_id,
        "connector_type": connector_type,
        "connector_id": connector_id,
        "tag_name": tag_name,
        "value_num": num_value,
        "value_str": str_value,
        "data_type": message.get("dataType", "float"),
        "unit": message.get("unit", ""),
        "quality": _parse_quality(message.get("quality", 100)),
        "collected_at": ts.isoformat(),
    }

    # 파일명 생성 (날짜 치환)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = file_name_pattern.replace("{pipeline_id}", str(pipeline_id)).replace("{date}", today)
    ext = {"jsonl": "jsonl", "csv": "csv", "json": "json"}.get(file_format, "jsonl")
    object_name = f"{path_prefix}{file_name}.{ext}"

    cache_key = f"file_sink:{pipeline_id}:{bucket}:{object_name}"
    cache_entry = {
        "bucket": bucket,
        "object_name": object_name,
        "file_format": file_format,
        "row": row,
    }

    if cache_key not in _sink_buffers:
        _sink_buffers[cache_key] = []
    _sink_buffers[cache_key].append(cache_entry)

    if len(_sink_buffers[cache_key]) >= batch_size:
        _flush_file_batch(cache_key)

    return message


def _flush_file_batch(cache_key):
    entries = _sink_buffers.pop(cache_key, [])
    if entries:
        _write_file_to_minio(entries)


def _write_file_to_minio(entries):
    """MinIO에 파일 쓰기 — append 방식 (기존 파일 + 신규 데이터)"""
    if not entries:
        return

    bucket = entries[0]["bucket"]
    object_name = entries[0]["object_name"]
    file_format = entries[0]["file_format"]
    rows = [e["row"] for e in entries]

    try:
        client = _get_minio()

        # 버킷 존재 확인 → 없으면 생성
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info("File sink: 버킷 생성 — %s", bucket)

        # 기존 파일 로드 (append 모드)
        existing_content = b""
        try:
            resp = client.get_object(bucket, object_name)
            existing_content = resp.read()
            resp.close()
            resp.release_conn()
        except Exception:
            pass  # 파일이 없으면 새로 생성

        # 포맷별 직렬화
        if file_format == "jsonl":
            new_lines = "\n".join(
                json.dumps(r, ensure_ascii=False, default=str) for r in rows
            ) + "\n"
            content = existing_content.decode("utf-8") + new_lines if existing_content else new_lines

        elif file_format == "csv":
            import csv as csv_mod
            # 모든 행의 컬럼 합집합
            all_cols = list(rows[0].keys())
            if existing_content:
                # 기존 CSV에서 헤더 읽기
                existing_text = existing_content.decode("utf-8")
                lines = existing_text.strip().split("\n")
                if lines:
                    all_cols = lines[0].split(",")
                buf = io.StringIO()
                buf.write(existing_text)
                if not existing_text.endswith("\n"):
                    buf.write("\n")
            else:
                buf = io.StringIO()
                writer = csv_mod.writer(buf)
                writer.writerow(all_cols)

            writer = csv_mod.writer(buf)
            for r in rows:
                writer.writerow([str(r.get(c, "")) for c in all_cols])
            content = buf.getvalue()

        elif file_format == "json":
            # JSON 배열 모드: 기존 배열 + 새 행
            existing_list = []
            if existing_content:
                try:
                    existing_list = json.loads(existing_content.decode("utf-8"))
                except (json.JSONDecodeError, ValueError):
                    pass
            existing_list.extend(rows)
            content = json.dumps(existing_list, ensure_ascii=False, indent=2, default=str)
        else:
            content = "\n".join(
                json.dumps(r, ensure_ascii=False, default=str) for r in rows
            ) + "\n"

        # MinIO에 업로드
        data = content.encode("utf-8")
        client.put_object(
            bucket, object_name,
            io.BytesIO(data), len(data),
            content_type="application/octet-stream",
        )
        logger.debug("File sink: %d rows → %s/%s (%d bytes)",
                      len(rows), bucket, object_name, len(data))

    except Exception as e:
        logger.error("File sink write error: %s", e)


# ══════════════════════════════════════════════
# Module Registry
# ══════════════════════════════════════════════

MODULE_REGISTRY = {
    "normalize": module_normalize,
    "unit_convert": module_unit_convert,
    "filter": module_filter,
    "anomaly": module_anomaly,
    "aggregate": module_aggregate,
    "enrich": module_enrich,
    "script": module_script,
}

# ── External Sink Placeholders ──

def _get_external_connection(connection_id):
    """ExternalConnection 레코드 조회"""
    from backend.database import SessionLocal
    from backend.models.integration import ExternalConnection
    db = SessionLocal()
    try:
        return db.query(ExternalConnection).get(connection_id)
    finally:
        db.close()


def sink_external_tsdb(message, step_config, context=None):
    """외부 시계열DB 싱크 — ExternalConnection 참조"""
    conn_id = step_config.get("connection_id")
    if conn_id:
        conn = _get_external_connection(conn_id)
        if conn:
            tag = message.get("source", {}).get("tagName", "?")
            logger.warning("[EXT TSDB] → %s:%s (conn=%d) tag=%s value=%s",
                           conn.host, conn.port, conn.id, tag, message.get("value"))
        else:
            logger.warning("[EXT TSDB] connection_id=%s not found", conn_id)
    else:
        logger.warning("[EXT TSDB] no connection_id in config")
    return message


def sink_external_rdbms(message, step_config, context=None):
    """외부 관계형DB 싱크 — ExternalConnection 참조"""
    conn_id = step_config.get("connection_id")
    if conn_id:
        conn = _get_external_connection(conn_id)
        if conn:
            tag = message.get("source", {}).get("tagName", "?")
            logger.warning("[EXT RDBMS] → %s:%s/%s (conn=%d) tag=%s value=%s",
                           conn.host, conn.port, conn.database_name, conn.id,
                           tag, message.get("value"))
        else:
            logger.warning("[EXT RDBMS] connection_id=%s not found", conn_id)
    else:
        logger.warning("[EXT RDBMS] no connection_id in config")
    return message


def sink_external_kafka(message, step_config, context=None):
    """외부 Kafka 싱크 — ExternalConnection 참조"""
    conn_id = step_config.get("connection_id")
    if conn_id:
        conn = _get_external_connection(conn_id)
        if conn:
            cfg = conn.config or {}
            logger.info("External Kafka sink → %s (id=%d)",
                        cfg.get("bootstrap_servers", conn.host), conn.id)
    return message


def sink_external_file(message, step_config, context=None):
    """외부 파일 스토리지 싱크 — ExternalConnection 참조"""
    conn_id = step_config.get("connection_id")
    if conn_id:
        conn = _get_external_connection(conn_id)
        if conn:
            cfg = conn.config or {}
            logger.info("External File sink → %s/%s (id=%d)",
                        conn.host, cfg.get("bucket", cfg.get("base_path", "")), conn.id)
    return message


SINK_REGISTRY = {
    "internal_tsdb_sink": sink_internal_tsdb,
    "internal_rdbms_sink": sink_internal_rdbms,
    "internal_file_sink": sink_internal_file,
    "external_tsdb_sink": sink_external_tsdb,
    "external_rdbms_sink": sink_external_rdbms,
    "external_kafka_sink": sink_external_kafka,
    "external_file_sink": sink_external_file,
}


def process_message(message, module_type, config):
    """단일 모듈 실행 — targetField 지원

    config에 targetField가 지정되고 message["value"]가 dict(JSON 행)이면,
    해당 필드만 추출하여 모듈에 전달한 뒤 결과를 원본 행에 다시 삽입한다.
    targetField가 비어있거나 value가 스칼라이면 기존 동작 그대로.
    """
    func = MODULE_REGISTRY.get(module_type)
    if not func:
        logger.warning("알 수 없는 모듈 타입: %s", module_type)
        return message

    target_field = config.get("targetField", "")

    if target_field and isinstance(message.get("value"), dict):
        row = message["value"]
        if target_field not in row:
            logger.warning("targetField '%s' not found in row keys: %s",
                           target_field, list(row.keys()))
            return message

        # 원본 행 보존, value를 해당 필드 값으로 교체
        original_row = row.copy()
        message["value"] = row[target_field]

        result = func(message, config)
        if result is None:
            return None  # drop

        # 처리된 값을 원본 행에 다시 삽입
        original_row[target_field] = result["value"]
        result["value"] = original_row
        return result

    return func(message, config)
