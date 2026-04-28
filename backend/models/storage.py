from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, Float,
    BigInteger,
)
from sqlalchemy.orm import relationship
from backend.database import Base


class TsdbConfig(Base):
    __tablename__ = "tsdb_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    db_type = Column(String(50), nullable=False, default="PostgreSQL")
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False, default=5432)
    organization = Column(String(100), default="")
    bucket = Column(String(100), default="")
    database_name = Column(String(100), default="")
    username = Column(String(100), default="")
    password = Column(String(500), default="")
    api_token = Column(String(500), default="")
    tls_enabled = Column(Boolean, default=False)
    retention_days = Column(Integer, default=30)
    status = Column(String(20), default="disconnected")
    description = Column(Text, default="")
    last_connected_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    downsampling_policies = relationship(
        "DownsamplingPolicy", back_populates="tsdb", cascade="all, delete-orphan"
    )

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "db_type": self.db_type,
            "host": self.host,
            "port": self.port,
            "organization": self.organization,
            "bucket": self.bucket,
            "database_name": self.database_name,
            "username": self.username,
            "tls_enabled": self.tls_enabled,
            "retention_days": self.retention_days,
            "status": self.status,
            "description": self.description,
            "last_connected_at": self.last_connected_at.isoformat() if self.last_connected_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class DownsamplingPolicy(Base):
    __tablename__ = "downsampling_policy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tsdb_id = Column(Integer, ForeignKey("tsdb_config.id", ondelete="CASCADE"), nullable=False)
    policy_name = Column(String(200), nullable=False)
    source_retention = Column(String(50), nullable=False, default="Raw (1초)")
    target_retention = Column(String(50), nullable=False, default="1년")
    aggregate_functions = Column(JSON, default=["MEAN"])
    aggregation_period = Column(String(50), nullable=False, default="1분")
    target_data = Column(String(500), default="")
    enabled = Column(Boolean, default=True)
    last_executed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tsdb = relationship("TsdbConfig", back_populates="downsampling_policies")

    def to_dict(self):
        return {
            "id": self.id,
            "tsdb_id": self.tsdb_id,
            "policy_name": self.policy_name,
            "source_retention": self.source_retention,
            "target_retention": self.target_retention,
            "aggregate_functions": self.aggregate_functions or [],
            "aggregation_period": self.aggregation_period,
            "target_data": self.target_data,
            "enabled": self.enabled,
            "last_executed_at": self.last_executed_at.isoformat() if self.last_executed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class RdbmsConfig(Base):
    __tablename__ = "rdbms_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    db_type = Column(String(50), nullable=False, default="PostgreSQL")
    host = Column(String(255), nullable=False)
    port = Column(Integer, nullable=False, default=5432)
    database_name = Column(String(100), default="")
    schema_name = Column(String(100), default="public")
    username = Column(String(100), default="")
    password = Column(String(500), default="")
    charset = Column(String(50), default="UTF-8")
    max_connections = Column(Integer, default=100)
    connection_timeout = Column(Integer, default=30)
    pool_size = Column(Integer, default=10)
    status = Column(String(20), default="disconnected")
    description = Column(Text, default="")
    last_connected_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "db_type": self.db_type,
            "host": self.host,
            "port": self.port,
            "database_name": self.database_name,
            "schema_name": self.schema_name,
            "username": self.username,
            "charset": self.charset,
            "max_connections": self.max_connections,
            "connection_timeout": self.connection_timeout,
            "pool_size": self.pool_size,
            "status": self.status,
            "description": self.description,
            "last_connected_at": self.last_connected_at.isoformat() if self.last_connected_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class FileCleanupPolicy(Base):
    __tablename__ = "file_cleanup_policy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    retention_days = Column(Integer, nullable=False, default=90)
    threshold_percent = Column(Float, nullable=False, default=80.0)
    target_buckets = Column(JSON, default=["sdl-files", "sdl-archive"])
    target_extensions = Column(JSON, default=[".log", ".tmp", ".csv"])
    enabled = Column(Boolean, default=False)
    last_executed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "retention_days": self.retention_days,
            "threshold_percent": self.threshold_percent,
            "target_buckets": self.target_buckets or [],
            "target_extensions": self.target_extensions or [],
            "enabled": self.enabled,
            "last_executed_at": self.last_executed_at.isoformat() if self.last_executed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class WarmAggregatedData(Base):
    __tablename__ = "warm_aggregated_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_table = Column(String(200), nullable=False)
    metric_name = Column(String(200), nullable=False)
    bucket_start = Column(DateTime, nullable=False)
    bucket_end = Column(DateTime, nullable=False)
    avg_value = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    count_value = Column(Integer, default=0)
    source_db = Column(String(100), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "source_table": self.source_table,
            "metric_name": self.metric_name,
            "bucket_start": self.bucket_start.isoformat() if self.bucket_start else None,
            "bucket_end": self.bucket_end.isoformat() if self.bucket_end else None,
            "avg_value": self.avg_value,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "count_value": self.count_value,
            "source_db": self.source_db,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class RetentionPolicy(Base):
    __tablename__ = "retention_policy"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Hot tier (시계열 DB)
    hot_retention_days = Column(Integer, nullable=False, default=30)
    hot_resolution = Column(String(50), nullable=False, default="1초 (Raw)")
    hot_storage_type = Column(String(50), nullable=False, default="TimescaleDB (PostgreSQL)")
    hot_capacity_gb = Column(Float, nullable=False, default=500.0)

    # Warm tier (RDBMS)
    warm_retention_months = Column(Integer, nullable=False, default=12)
    warm_resolution = Column(String(50), nullable=False, default="집계/메타데이터")
    warm_storage_type = Column(String(50), nullable=False, default="PostgreSQL (RDBMS)")
    warm_capacity_gb = Column(Float, nullable=False, default=2048.0)

    # Cold tier (파일 스토리지)
    cold_retention_years = Column(Integer, nullable=False, default=3)
    cold_resolution = Column(String(50), nullable=False, default="아카이브 파일")
    cold_storage_type = Column(String(50), nullable=False, default="MinIO (S3)")
    cold_capacity_gb = Column(Float, nullable=False, default=10240.0)

    # 전환 스케줄
    hot_to_warm_schedule = Column(String(100), nullable=False, default="매일 새벽 3시")
    warm_to_cold_schedule = Column(String(100), nullable=False, default="매주 일요일 새벽 4시")
    expiry_cleanup_schedule = Column(String(100), nullable=False, default="매주")

    # 옵션
    auto_delete_enabled = Column(Boolean, nullable=False, default=True)
    alert_threshold_enabled = Column(Boolean, nullable=False, default=True)
    alert_threshold_percent = Column(Float, nullable=False, default=80.0)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "hot_retention_days": self.hot_retention_days,
            "hot_resolution": self.hot_resolution,
            "hot_storage_type": self.hot_storage_type,
            "hot_capacity_gb": self.hot_capacity_gb,
            "warm_retention_months": self.warm_retention_months,
            "warm_resolution": self.warm_resolution,
            "warm_storage_type": self.warm_storage_type,
            "warm_capacity_gb": self.warm_capacity_gb,
            "cold_retention_years": self.cold_retention_years,
            "cold_resolution": self.cold_resolution,
            "cold_storage_type": self.cold_storage_type,
            "cold_capacity_gb": self.cold_capacity_gb,
            "hot_to_warm_schedule": self.hot_to_warm_schedule,
            "warm_to_cold_schedule": self.warm_to_cold_schedule,
            "expiry_cleanup_schedule": self.expiry_cleanup_schedule,
            "auto_delete_enabled": self.auto_delete_enabled,
            "alert_threshold_enabled": self.alert_threshold_enabled,
            "alert_threshold_percent": self.alert_threshold_percent,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class RetentionExecutionLog(Base):
    __tablename__ = "retention_execution_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    task_type = Column(String(50), nullable=False)
    tier_transition = Column(String(50), nullable=False)
    processed_bytes = Column(BigInteger, nullable=False, default=0)
    processed_records = Column(Integer, nullable=False, default=0)
    duration_seconds = Column(Float, nullable=False, default=0.0)
    status = Column(String(20), nullable=False, default="success")
    error_message = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "execution_time": self.execution_time.isoformat() if self.execution_time else None,
            "task_type": self.task_type,
            "tier_transition": self.tier_transition,
            "processed_bytes": self.processed_bytes,
            "processed_records": self.processed_records,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class TimeSeriesData(Base):
    """내부 시계열DB — 파이프라인 싱크에서 기록하는 시계열 데이터 포인트"""
    __tablename__ = "time_series_data"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    tsdb_id = Column(Integer, nullable=False)                     # TsdbConfig.id
    measurement = Column(String(200), nullable=False)             # 측정 이름 (테이블/버킷)
    tag_name = Column(String(200), nullable=False)                # 태그 이름
    connector_type = Column(String(50), default="")
    connector_id = Column(Integer, default=0)
    pipeline_id = Column(Integer, default=0)
    value = Column(Float, nullable=True)                          # 숫자 값
    value_str = Column(Text, default="")                          # 문자열/JSON 값
    data_type = Column(String(30), default="float")
    unit = Column(String(50), default="")
    quality = Column(Integer, default=100)
    tags = Column(JSON, default={})                               # 추가 태그 (key-value)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "tsdbId": self.tsdb_id,
            "measurement": self.measurement,
            "tagName": self.tag_name,
            "connectorType": self.connector_type,
            "connectorId": self.connector_id,
            "pipelineId": self.pipeline_id,
            "value": self.value,
            "valueStr": self.value_str,
            "dataType": self.data_type,
            "unit": self.unit,
            "quality": self.quality,
            "tags": self.tags or {},
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }
