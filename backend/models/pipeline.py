from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from backend.database import Base


# ══════════════════════════════════════════════
# Pipeline 정의 모델
# ══════════════════════════════════════════════

class Pipeline(Base):
    __tablename__ = "pipeline"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False, unique=True)
    description = Column(String(500), default="")
    status = Column(String(20), default="stopped")          # running / stopped / error
    enabled = Column(Boolean, default=True)
    # MQTT 토픽 설정
    input_topic = Column(String(500), default="")            # sdl/raw/# (자동생성)
    output_topic = Column(String(500), default="")           # sdl/processed/{id}/# (자동생성)
    # 런타임 통계
    processed_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_processed_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    # 파일 소스 run lock: 동시 실행 방지 + crash 시 24h TTL 후 자동 해제
    current_run_id = Column(String(64), default="")
    current_run_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    steps = relationship("PipelineStep", back_populates="pipeline",
                         cascade="all, delete-orphan", order_by="PipelineStep.step_order")
    bindings = relationship("PipelineBinding", back_populates="pipeline",
                            cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status,
            "enabled": self.enabled,
            "inputTopic": self.input_topic,
            "outputTopic": self.output_topic,
            "processedCount": self.processed_count,
            "errorCount": self.error_count,
            "lastProcessedAt": self.last_processed_at.isoformat() if self.last_processed_at else None,
            "lastError": self.last_error,
            "stepCount": len(self.steps) if self.steps else 0,
            "bindingCount": len(self.bindings) if self.bindings else 0,
            "hasFileSource": any(
                getattr(st, "module_type", "") in ("import_source", "internal_file_source")
                for st in (self.steps or [])
            ),
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }


class PipelineStep(Base):
    """파이프라인 처리 단계 (순서대로 실행)"""
    __tablename__ = "pipeline_step"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(Integer, ForeignKey("pipeline.id", ondelete="CASCADE"), nullable=False)
    step_order = Column(Integer, nullable=False, default=0)
    module_type = Column(String(50), nullable=False)         # normalize / unit_convert / filter / anomaly / aggregate / enrich / script
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})                        # 모듈별 설정 JSON
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="steps")

    def to_dict(self):
        return {
            "id": self.id,
            "pipelineId": self.pipeline_id,
            "stepOrder": self.step_order,
            "moduleType": self.module_type,
            "enabled": self.enabled,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }


class PipelineBinding(Base):
    """파이프라인-커넥터 바인딩 (어떤 커넥터의 어떤 태그들을 파이프라인에 연결)"""
    __tablename__ = "pipeline_binding"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(Integer, ForeignKey("pipeline.id", ondelete="CASCADE"), nullable=False)
    connector_type = Column(String(20), nullable=False)      # opcua / opcda / modbus / mqtt / db / file / api
    connector_id = Column(Integer, nullable=False)            # 해당 커넥터 테이블의 ID
    tag_filter = Column(String(500), default="*")            # 태그 필터 (와일드카드 지원)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    pipeline = relationship("Pipeline", back_populates="bindings")

    def to_dict(self):
        return {
            "id": self.id,
            "pipelineId": self.pipeline_id,
            "connectorType": self.connector_type,
            "connectorId": self.connector_id,
            "tagFilter": self.tag_filter,
            "enabled": self.enabled,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# 전처리 모듈 설정 모델 (재사용 가능한 규칙 세트)
# ══════════════════════════════════════════════

class NormalizeRule(Base):
    """데이터 정규화 규칙"""
    __tablename__ = "normalize_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    source_type = Column(String(50), default="")             # 원본 데이터 타입
    target_type = Column(String(50), default="float")        # 변환 대상 타입
    null_strategy = Column(String(20), default="skip")       # skip / zero / last / interpolate
    trim_whitespace = Column(Boolean, default=True)
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "sourceType": self.source_type,
            "targetType": self.target_type,
            "nullStrategy": self.null_strategy,
            "trimWhitespace": self.trim_whitespace,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


class UnitConversion(Base):
    """단위 변환 규칙"""
    __tablename__ = "unit_conversion"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    category = Column(String(50), default="temperature")     # temperature / pressure / length / flow / weight
    source_unit = Column(String(50), nullable=False)         # celsius / psi / mm / ...
    target_unit = Column(String(50), nullable=False)
    formula = Column(String(500), default="")                # 사용자 정의 수식 (value * factor + offset)
    factor = Column(Float, default=1.0)
    offset = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "category": self.category,
            "sourceUnit": self.source_unit,
            "targetUnit": self.target_unit,
            "formula": self.formula,
            "factor": self.factor,
            "offset": self.offset,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


class FilterRule(Base):
    """필터링 규칙"""
    __tablename__ = "filter_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    filter_type = Column(String(20), default="range")        # range / condition / regex / deadband
    field = Column(String(200), default="value")
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    condition = Column(String(500), default="")              # 조건식
    action = Column(String(20), default="drop")              # drop / flag / clamp
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "filterType": self.filter_type,
            "field": self.field,
            "minValue": self.min_value,
            "maxValue": self.max_value,
            "condition": self.condition,
            "action": self.action,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


class AnomalyConfig(Base):
    """이상치 탐지 설정"""
    __tablename__ = "anomaly_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    method = Column(String(30), default="zscore")            # zscore / iqr / moving_avg / sigma
    threshold = Column(Float, default=3.0)                   # Z-score 기준값 or IQR 배수
    window_size = Column(Integer, default=60)                # 이동 평균 윈도우 (초)
    action = Column(String(20), default="flag")              # flag / drop / clamp / replace
    replace_strategy = Column(String(20), default="mean")    # mean / median / last / interpolate
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "method": self.method,
            "threshold": self.threshold,
            "windowSize": self.window_size,
            "action": self.action,
            "replaceStrategy": self.replace_strategy,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


class AggregateConfig(Base):
    """집계 설정"""
    __tablename__ = "aggregate_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    window_seconds = Column(Integer, default=60)                # 집계 윈도우 (초)
    functions = Column(JSON, default=["avg"])                    # avg / min / max / count / sum
    emit_mode = Column(String(20), default="end")               # end / start / both
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "windowSeconds": self.window_seconds,
            "functions": self.functions or ["avg"],
            "emitMode": self.emit_mode,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


class EnrichConfig(Base):
    """데이터 보강 설정"""
    __tablename__ = "enrich_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    fields = Column(JSON, default={})                           # 고정 필드 {key: value}
    lookup_table = Column(String(100), default="")              # tag_metadata 등
    add_timestamp = Column(Boolean, default=True)
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "fields": self.fields or {},
            "lookupTable": self.lookup_table,
            "addTimestamp": self.add_timestamp,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


class ScriptConfig(Base):
    """커스텀 스크립트 설정"""
    __tablename__ = "script_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    language = Column(String(20), default="python")             # python
    code = Column(Text, default="")
    timeout = Column(Integer, default=5)                        # 초
    description = Column(String(500), default="")
    config = Column(JSON, default={})
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "language": self.language,
            "code": self.code,
            "timeout": self.timeout,
            "description": self.description,
            "config": self.config or {},
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }
