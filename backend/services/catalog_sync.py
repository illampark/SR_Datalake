"""카탈로그 ↔ 커넥터/파이프라인 동기화 유틸리티"""

import logging

from backend.models.catalog import DataCatalog, CatalogSearchTag
from backend.models.collector import (
    MqttConnector, DbConnector, OpcuaConnector,
    OpcdaConnector, ModbusConnector, ApiConnector, FileCollector,
)

logger = logging.getLogger(__name__)

_MODEL_MAP = {
    "mqtt": MqttConnector,
    "db": DbConnector,
    "opcua": OpcuaConnector,
    "opcda": OpcdaConnector,
    "modbus": ModbusConnector,
    "api": ApiConnector,
    "file": FileCollector,
}

_CONNECTOR_LABELS = {
    "opcua": "OPC-UA", "opcda": "OPC-DA", "modbus": "Modbus",
    "mqtt": "MQTT", "db": "DB", "file": "File", "api": "API",
}


def get_connector_description(db, connector_type, connector_id):
    """connector_type/id 로 해당 커넥터의 description 을 조회한다."""
    model = _MODEL_MAP.get(connector_type)
    if model and connector_id:
        c = db.query(model).get(connector_id)
        if c:
            return c.description or ""
    return ""


def sync_connector_description(db, connector_type, connector_id, description):
    """커넥터 설명이 변경되면 관련 DataCatalog 의 connector_description 을 일괄 업데이트한다."""
    db.query(DataCatalog).filter_by(
        connector_type=connector_type,
        connector_id=connector_id,
    ).update({"connector_description": description or ""})


def sync_connector_name(db, connector_type, connector_id, new_name):
    """커넥터 이름이 변경되면 관련 DataCatalog 의 이름을 갱신한다."""
    label = _CONNECTOR_LABELS.get(connector_type, connector_type)
    conn_label = new_name or f"#{connector_id}"

    catalogs = db.query(DataCatalog).filter_by(
        connector_type=connector_type,
        connector_id=connector_id,
    ).all()

    for cat in catalogs:
        if cat.tag_name:
            cat.name = f"{cat.tag_name} ({label} {conn_label})"
        else:
            cat.name = f"{label} {conn_label} — 전체 데이터"


def delete_connector_catalogs(db, connector_type, connector_id):
    """커넥터 삭제 시 관련 DataCatalog 및 검색 태그를 모두 제거한다."""
    catalogs = db.query(DataCatalog).filter_by(
        connector_type=connector_type,
        connector_id=connector_id,
    ).all()

    count = len(catalogs)
    for cat in catalogs:
        db.delete(cat)  # cascade="all, delete-orphan" → CatalogSearchTag도 함께 삭제

    if count:
        logger.info("커넥터 삭제 → 카탈로그 %d건 정리 (connector=%s#%s)",
                     count, connector_type, connector_id)


# ══════════════════════════════════════════════
# 파이프라인 싱크 ↔ 카탈로그 동기화
# ══════════════════════════════════════════════

_SINK_TYPES = {"internal_tsdb_sink", "internal_rdbms_sink", "internal_file_sink"}

_SINK_LABELS = {
    "internal_tsdb_sink": "TSDB",
    "internal_rdbms_sink": "RDBMS",
    "internal_file_sink": "File(S3)",
}


def _extract_sink_info(step):
    """PipelineStep에서 싱크 카탈로그에 필요한 정보를 추출한다."""
    cfg = step.config or {}
    module = step.module_type

    if module == "internal_tsdb_sink":
        measurement = cfg.get("measurement", "")
        tsdb_name = cfg.get("tsdbName", "")
        return {
            "tag_name": measurement,
            "access_url": f"tsdb://{tsdb_name}/{measurement}" if measurement else "",
            "format": "timeseries",
            "sink_config_id": cfg.get("tsdbId"),
        }
    elif module == "internal_rdbms_sink":
        table_name = cfg.get("tableName", "")
        rdbms_name = cfg.get("rdbmsName", "")
        return {
            "tag_name": table_name,
            "access_url": f"rdbms://{rdbms_name}/{table_name}" if table_name else "",
            "format": "table",
            "sink_config_id": cfg.get("rdbmsId"),
        }
    elif module == "internal_file_sink":
        bucket = cfg.get("bucket", "sdl-files")
        path_prefix = cfg.get("pathPrefix", "")
        file_format = cfg.get("fileFormat", "jsonl")
        return {
            "tag_name": path_prefix or bucket,
            "access_url": f"s3://{bucket}/{path_prefix}" if path_prefix else f"s3://{bucket}",
            "format": file_format,
            "sink_config_id": None,
        }
    return None


def sync_pipeline_catalogs(db, pipeline_id, pipeline_name, steps):
    """파이프라인 싱크 설정 기반으로 카탈로그를 생성/갱신/정리한다."""
    try:
        # 현재 싱크 스텝에서 카탈로그 대상 추출
        new_sinks = []
        for step in steps:
            if step.module_type not in _SINK_TYPES or not step.enabled:
                continue
            info = _extract_sink_info(step)
            if info and info["tag_name"]:
                new_sinks.append((step.module_type, info))

        # 기존 파이프라인 카탈로그 조회
        existing = db.query(DataCatalog).filter_by(
            connector_type="pipeline",
            pipeline_id=pipeline_id,
        ).all()
        existing_map = {(c.sink_type, c.tag_name): c for c in existing}

        seen_keys = set()
        created = 0

        for sink_type, info in new_sinks:
            key = (sink_type, info["tag_name"])
            seen_keys.add(key)

            if key in existing_map:
                # 기존 카탈로그 갱신
                cat = existing_map[key]
                cat.name = f"{info['tag_name']} (Pipeline {pipeline_name} — {_SINK_LABELS.get(sink_type, sink_type)})"
                cat.access_url = info["access_url"]
                cat.format = info["format"]
            else:
                # 새 카탈로그 생성
                cat = DataCatalog(
                    name=f"{info['tag_name']} (Pipeline {pipeline_name} — {_SINK_LABELS.get(sink_type, sink_type)})",
                    description=f"파이프라인 '{pipeline_name}'의 {_SINK_LABELS.get(sink_type, sink_type)} 싱크 출력 데이터",
                    connector_type="pipeline",
                    connector_id=pipeline_id,
                    pipeline_id=pipeline_id,
                    sink_type=sink_type,
                    tag_name=info["tag_name"],
                    owner="시스템 자동",
                    category="파이프라인",
                    data_level="processed",
                    sensitivity="internal",
                    access_url=info["access_url"],
                    format=info["format"],
                    is_published=True,
                )
                db.add(cat)
                db.flush()

                # 검색 태그 추가
                sink_label = _SINK_LABELS.get(sink_type, sink_type)
                search_tags = ["pipeline", pipeline_name, sink_label, info["tag_name"]]
                for st in search_tags:
                    if st:
                        db.add(CatalogSearchTag(catalog_id=cat.id, tag=st))
                created += 1

        # 제거된 싱크의 카탈로그 삭제
        removed = 0
        for key, cat in existing_map.items():
            if key not in seen_keys:
                db.delete(cat)
                removed += 1

        db.commit()
        if created or removed:
            logger.info("파이프라인 카탈로그 동기화: pipeline=%d, 생성=%d, 삭제=%d",
                         pipeline_id, created, removed)
    except Exception as e:
        db.rollback()
        logger.warning("파이프라인 카탈로그 동기화 실패 (pipeline=%d): %s", pipeline_id, e)


def delete_pipeline_catalogs(db, pipeline_id):
    """파이프라인 삭제 시 관련 카탈로그를 모두 제거한다."""
    catalogs = db.query(DataCatalog).filter_by(
        connector_type="pipeline",
        pipeline_id=pipeline_id,
    ).all()

    count = len(catalogs)
    for cat in catalogs:
        db.delete(cat)

    if count:
        logger.info("파이프라인 삭제 → 카탈로그 %d건 정리 (pipeline=%d)",
                     count, pipeline_id)


def backfill_pipeline_catalogs():
    """기존 파이프라인에 대한 싱크 카탈로그 일괄 생성 (서버 시작 시 1회)"""
    from backend.database import SessionLocal
    from backend.models.pipeline import Pipeline

    db = SessionLocal()
    try:
        pipelines = db.query(Pipeline).all()
        created_total = 0
        for p in pipelines:
            if not p.steps:
                continue
            has_sink = any(s.module_type in _SINK_TYPES for s in p.steps)
            if not has_sink:
                continue
            # 이미 카탈로그가 있는지 확인
            existing = db.query(DataCatalog).filter_by(
                connector_type="pipeline",
                pipeline_id=p.id,
            ).count()
            if existing == 0:
                sync_pipeline_catalogs(db, p.id, p.name, p.steps)
                created_total += 1

        if created_total:
            logger.info("파이프라인 카탈로그 %d건 일괄 생성 완료", created_total)
    except Exception as e:
        logger.warning("파이프라인 카탈로그 일괄 생성 실패: %s", e)
    finally:
        db.close()
