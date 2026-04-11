"""
Metadata Tracker — 태그 메타데이터 자동 갱신 + 데이터 계보 기록

커넥터 수집 시 자동으로:
  1. TagMetadata upsert (첫 등록 / 값 범위 갱신 / 품질 스코어 업데이트)
  2. DataLineage 배치 단위 기록
"""

import logging
import uuid
from datetime import datetime

from backend.database import SessionLocal
from backend.models.metadata import TagMetadata, DataLineage
from backend.models.catalog import DataCatalog, CatalogSearchTag

logger = logging.getLogger(__name__)


def upsert_tag_metadata(connector_type, connector_id, connector_name, tag_name,
                        value=None, data_type="float", unit=""):
    """태그 메타데이터 upsert — 수집 때마다 호출"""
    db = SessionLocal()
    try:
        meta = db.query(TagMetadata).filter_by(
            connector_type=connector_type,
            connector_id=connector_id,
            tag_name=tag_name,
        ).first()

        now = datetime.utcnow()

        if not meta:
            meta = TagMetadata(
                tag_name=tag_name,
                connector_type=connector_type,
                connector_id=connector_id,
                connector_name=connector_name,
                data_type=data_type,
                unit=unit,
                first_seen_at=now,
                last_seen_at=now,
                sample_count=0,
                mqtt_topic=f"sdl/raw/{connector_type}/{connector_id}/{tag_name}",
            )
            db.add(meta)

        meta.last_seen_at = now
        meta.is_active = True
        meta.sample_count = (meta.sample_count or 0) + 1

        # 커넥터명이 비어있으면 갱신 (기존 레코드 자동 채움)
        if connector_name and not meta.connector_name:
            meta.connector_name = connector_name

        # 수치 값인 경우 통계 갱신
        if value is not None:
            try:
                fval = float(value)
                meta.last_value = str(value)
                if meta.min_observed is None or fval < meta.min_observed:
                    meta.min_observed = fval
                if meta.max_observed is None or fval > meta.max_observed:
                    meta.max_observed = fval
                # Running average
                n = meta.sample_count
                if meta.avg_observed is None:
                    meta.avg_observed = fval
                else:
                    meta.avg_observed = meta.avg_observed + (fval - meta.avg_observed) / n
            except (ValueError, TypeError):
                meta.last_value = str(value) if value is not None else ""

        db.commit()

        # 새 태그인 경우 카탈로그 자동 생성 (중복 방지)
        if meta.sample_count == 1:
            _auto_create_catalog(db, connector_type, connector_id,
                                 connector_name, tag_name, data_type, unit)

        return meta.id
    except Exception as e:
        db.rollback()
        logger.error("TagMetadata upsert 실패: %s", e)
        return None
    finally:
        db.close()


def ensure_connector_catalog(connector_type, connector_id, connector_name=""):
    """커넥터 그룹 카탈로그가 없으면 생성 (callback에서 호출)"""
    db = SessionLocal()
    try:
        _auto_create_connector_catalog(db, connector_type, connector_id, connector_name)
    except Exception as e:
        logger.warning("커넥터 카탈로그 확인 실패: %s", e)
    finally:
        db.close()


def update_quality_score(connector_type, connector_id, tag_name,
                         null_count=0, anomaly_count=0, total_count=1):
    """품질 점수 갱신"""
    db = SessionLocal()
    try:
        meta = db.query(TagMetadata).filter_by(
            connector_type=connector_type,
            connector_id=connector_id,
            tag_name=tag_name,
        ).first()
        if not meta:
            return

        if total_count > 0:
            meta.null_ratio = null_count / total_count
            meta.anomaly_ratio = anomaly_count / total_count
            meta.quality_score = max(0.0, 100.0 * (1 - meta.null_ratio - meta.anomaly_ratio))
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("품질 점수 갱신 실패: %s", e)
    finally:
        db.close()


def record_lineage(source_connector_type, source_connector_id, source_tag,
                   pipeline_id=None, pipeline_name="",
                   steps_applied=None, destination_type="", destination_target="",
                   record_count=0, error_count=0, processing_ms=0.0):
    """데이터 계보 기록 (배치 단위)"""
    db = SessionLocal()
    try:
        lineage = DataLineage(
            source_connector_type=source_connector_type,
            source_connector_id=source_connector_id,
            source_tag=source_tag,
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            steps_applied=steps_applied or [],
            destination_type=destination_type,
            destination_target=destination_target,
            batch_id=str(uuid.uuid4())[:12],
            record_count=record_count,
            error_count=error_count,
            processing_ms=processing_ms,
        )
        db.add(lineage)
        db.commit()
        return lineage.id
    except Exception as e:
        db.rollback()
        logger.error("DataLineage 기록 실패: %s", e)
        return None
    finally:
        db.close()


def mark_tags_inactive(connector_type, connector_id):
    """커넥터 중지 시 해당 태그들 비활성"""
    db = SessionLocal()
    try:
        db.query(TagMetadata).filter_by(
            connector_type=connector_type,
            connector_id=connector_id,
        ).update({"is_active": False})
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("태그 비활성 처리 실패: %s", e)
    finally:
        db.close()


# ── 카탈로그 자동 생성 ──

_CONNECTOR_LABELS = {
    "opcua": "OPC-UA", "opcda": "OPC-DA", "modbus": "Modbus",
    "mqtt": "MQTT", "db": "DB", "file": "File", "api": "API",
}

_CATEGORY_MAP = {
    "TEMP": "온도", "HUMID": "습도", "PRESS": "압력",
    "VIB": "진동", "RPM": "회전", "CURR": "전류",
    "SPEED": "속도", "LEVEL": "레벨", "FLOW": "유량",
    "SENSOR": "센서",
}


def _guess_category(tag_name):
    """태그명에서 카테고리 추정"""
    upper = (tag_name or "").upper()
    for key, cat in _CATEGORY_MAP.items():
        if key in upper:
            return cat
    return "기타"


def _auto_create_connector_catalog(db, connector_type, connector_id,
                                    connector_name):
    """커넥터 레벨 카탈로그 자동 생성 (tag_name="", 커넥터 전체 데이터용)"""
    try:
        existing = db.query(DataCatalog).filter_by(
            connector_type=connector_type,
            connector_id=connector_id,
            tag_name="",
        ).first()
        if existing:
            return

        label = _CONNECTOR_LABELS.get(connector_type, connector_type)
        conn_label = connector_name or f"#{connector_id}"

        # 커넥터 설명 조회
        try:
            from backend.services.catalog_sync import get_connector_description
            conn_desc = get_connector_description(db, connector_type, connector_id)
        except Exception:
            conn_desc = ""

        # Import 커넥터인 경우 sink_type 자동 설정
        _sink_type = ""
        if connector_type == "import":
            try:
                from backend.models.collector import ImportCollector
                imp = db.query(ImportCollector).get(connector_id)
                if imp:
                    _SINK_MAP = {"tsdb": "internal_tsdb_sink", "rdbms": "internal_rdbms_sink", "file": "internal_file_sink"}
                    _sink_type = _SINK_MAP.get(imp.target_type, "")
            except Exception:
                pass

        catalog = DataCatalog(
            name=f"{label} {conn_label} — 전체 데이터",
            description=f"{label} 커넥터 {conn_label}의 모든 태그 데이터를 조회합니다",
            connector_type=connector_type,
            connector_id=connector_id,
            connector_description=conn_desc,
            tag_name="",
            owner="시스템 자동",
            category="기타",
            data_level="raw",
            sensitivity="internal",
            access_url=f"sdl/raw/{connector_type}/{connector_id}/*",
            format="mixed",
            sink_type=_sink_type,
            is_published=True,
        )
        db.add(catalog)
        db.flush()

        search_tags = [connector_type, label, conn_label]
        for st in search_tags:
            db.add(CatalogSearchTag(catalog_id=catalog.id, tag=st))

        db.commit()
        logger.info("커넥터 카탈로그 자동 생성: %s (connector=%s#%s)",
                     catalog.name, connector_type, connector_id)
    except Exception as e:
        db.rollback()
        logger.warning("커넥터 카탈로그 자동 생성 실패: %s", e)


def _auto_create_catalog(db, connector_type, connector_id,
                         connector_name, tag_name, data_type, unit):
    """새 태그 최초 수집 시: 커넥터 그룹 카탈로그 + TagMetadata 거버넌스 기본값 설정

    태그별 카탈로그는 생성하지 않음 — TagMetadata에서 통합 관리.
    """
    try:
        # 커넥터 레벨 카탈로그가 없으면 생성
        _auto_create_connector_catalog(db, connector_type, connector_id,
                                       connector_name)

        # TagMetadata에 거버넌스 기본값 설정
        meta = db.query(TagMetadata).filter_by(
            connector_type=connector_type,
            connector_id=connector_id,
            tag_name=tag_name,
        ).first()
        if meta and not meta.category:
            meta.category = _guess_category(tag_name)
            meta.owner = "시스템 자동"
            meta.data_level = "raw"
            meta.sensitivity = "internal"
            meta.is_published = True
            db.commit()
            logger.info("태그 거버넌스 설정: %s (connector=%s#%s)",
                         tag_name, connector_type, connector_id)
    except Exception as e:
        db.rollback()
        logger.warning("태그 거버넌스 설정 실패: %s", e)


def backfill_connector_catalogs():
    """기존 커넥터에 대한 커넥터 레벨 카탈로그 일괄 생성 (서버 시작 시 1회)"""
    db = SessionLocal()
    try:
        # 태그 메타데이터에서 고유 커넥터 목록 조회
        connectors = db.query(
            TagMetadata.connector_type,
            TagMetadata.connector_id,
            TagMetadata.connector_name,
        ).distinct().all()

        created = 0
        for connector_type, connector_id, connector_name in connectors:
            existing = db.query(DataCatalog).filter_by(
                connector_type=connector_type,
                connector_id=connector_id,
                tag_name="",
            ).first()
            if not existing:
                _auto_create_connector_catalog(db, connector_type, connector_id,
                                               connector_name or "")
                created += 1

        if created:
            logger.info("커넥터 레벨 카탈로그 %d건 일괄 생성 완료", created)
    except Exception as e:
        logger.warning("커넥터 카탈로그 일괄 생성 실패: %s", e)
    finally:
        db.close()
