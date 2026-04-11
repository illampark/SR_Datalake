from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Float, JSON
from backend.database import Base


# ══════════════════════════════════════════════
# 시스템 수준 메타데이터 (자동 생성/관리)
# ══════════════════════════════════════════════

class TagMetadata(Base):
    """태그 메타데이터 — 커넥터가 수집 시작할 때 자동 등록, 수집 중 자동 갱신"""
    __tablename__ = "tag_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 태그 식별
    tag_name = Column(String(200), nullable=False)
    connector_type = Column(String(20), nullable=False)      # opcua / opcda / modbus / mqtt / db / file / api
    connector_id = Column(Integer, nullable=False)
    connector_name = Column(String(100), default="")
    # 데이터 프로파일
    data_type = Column(String(50), default="float")          # float / int / string / boolean / json
    unit = Column(String(50), default="")                    # 물리 단위 (C, psi, mm, ...)
    min_observed = Column(Float, nullable=True)              # 관측 최소값
    max_observed = Column(Float, nullable=True)              # 관측 최대값
    avg_observed = Column(Float, nullable=True)              # 관측 평균
    sample_count = Column(Integer, default=0)                # 누적 샘플 수
    # 품질 지표
    quality_score = Column(Float, default=100.0)             # 0~100 (결측, 이상치 반영)
    null_ratio = Column(Float, default=0.0)                  # 결측률 (0~1)
    anomaly_ratio = Column(Float, default=0.0)               # 이상치 비율 (0~1)
    # 수집 상태
    first_seen_at = Column(DateTime, nullable=True)
    last_seen_at = Column(DateTime, nullable=True)
    last_value = Column(String(500), default="")             # 마지막 관측값 (문자열)
    is_active = Column(Boolean, default=True)                # 현재 수집 활성 여부
    # 메시지 경로
    mqtt_topic = Column(String(500), default="")             # sdl/raw/{type}/{id}/{tag}
    # 거버넌스 (사용자 관리)
    description = Column(Text, default="")                   # 사용자 설명
    owner = Column(String(100), default="")                  # 데이터 소유자
    category = Column(String(100), default="")               # 분류 (온도, 습도, 압력 등)
    data_level = Column(String(20), default="raw")           # raw / processed / user_created
    sensitivity = Column(String(20), default="internal")     # public / internal / confidential / restricted
    retention_policy = Column(String(100), default="")       # 보관 정책
    is_published = Column(Boolean, default=True)             # 카탈로그 공개 여부
    is_deprecated = Column(Boolean, default=False)           # 폐기 여부
    # 시간
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "tagName": self.tag_name,
            "connectorType": self.connector_type,
            "connectorId": self.connector_id,
            "connectorName": self.connector_name,
            "dataType": self.data_type,
            "unit": self.unit,
            "minObserved": self.min_observed,
            "maxObserved": self.max_observed,
            "avgObserved": self.avg_observed,
            "sampleCount": self.sample_count,
            "qualityScore": self.quality_score,
            "nullRatio": self.null_ratio,
            "anomalyRatio": self.anomaly_ratio,
            "firstSeenAt": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "lastSeenAt": self.last_seen_at.isoformat() if self.last_seen_at else None,
            "lastValue": self.last_value,
            "isActive": self.is_active,
            "mqttTopic": self.mqtt_topic,
            "description": self.description or "",
            "owner": self.owner or "",
            "category": self.category or "",
            "dataLevel": self.data_level or "raw",
            "sensitivity": self.sensitivity or "internal",
            "retentionPolicy": self.retention_policy or "",
            "isPublished": self.is_published if self.is_published is not None else True,
            "isDeprecated": self.is_deprecated or False,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }


class DataLineage(Base):
    """데이터 계보 — 파이프라인 실행 시 자동 기록 (배치 단위)"""
    __tablename__ = "data_lineage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 소스
    source_connector_type = Column(String(20), nullable=False)
    source_connector_id = Column(Integer, nullable=False)
    source_tag = Column(String(200), default="")
    # 파이프라인
    pipeline_id = Column(Integer, nullable=True)
    pipeline_name = Column(String(200), default="")
    steps_applied = Column(JSON, default=[])                 # ["normalize", "filter", "anomaly"]
    # 결과
    destination_type = Column(String(50), default="")        # tsdb / rdbms / file / mqtt
    destination_target = Column(String(200), default="")     # 테이블명 / 토픽명
    # 배치 정보
    batch_id = Column(String(100), default="")               # 유니크 배치 ID
    record_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    processing_ms = Column(Float, default=0.0)               # 처리 소요 시간
    # 시간
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "sourceConnectorType": self.source_connector_type,
            "sourceConnectorId": self.source_connector_id,
            "sourceTag": self.source_tag,
            "pipelineId": self.pipeline_id,
            "pipelineName": self.pipeline_name,
            "stepsApplied": self.steps_applied or [],
            "destinationType": self.destination_type,
            "destinationTarget": self.destination_target,
            "batchId": self.batch_id,
            "recordCount": self.record_count,
            "errorCount": self.error_count,
            "processingMs": self.processing_ms,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }
