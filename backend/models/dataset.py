"""데이터셋 추출 요청 모델 — 대량 데이터 검색/조회/Export"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, Float, JSON, BigInteger,
)
from backend.database import Base


class DatasetRequest(Base):
    """데이터셋 추출 요청 — 다중 태그 대량 데이터 Export 관리"""
    __tablename__ = "dataset_request"

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(String(50), unique=True, nullable=False)    # ds-YYYYMMDD-NNN
    name = Column(String(200), nullable=False)
    description = Column(Text, default="")
    requested_by = Column(String(100), default="")                  # 요청자 username

    # 단일 카탈로그 export — Tier 2 비동기 카탈로그 다운로드용. 기존 다중 태그 export 와는 배타적.
    catalog_id = Column(Integer, nullable=True)

    # 조회 조건
    tags = Column(JSON, default=[])                                 # ["tag1", "tag2", ...]
    connector_types = Column(JSON, default=[])                      # ["opcua", "modbus", ...]
    connector_ids = Column(JSON, default=[])                        # [1, 2, ...]
    date_from = Column(DateTime, nullable=True)
    date_to = Column(DateTime, nullable=True)
    quality_min = Column(Integer, default=0)                        # 최소 품질 점수

    # 샘플링 설정
    sampling_method = Column(String(20), default="none")            # none / downsample / random
    sampling_interval = Column(String(20), default="")              # 1s, 1min, 5min, 1h, ...
    sampling_ratio = Column(Float, default=1.0)                     # random 시 비율 (0.0~1.0)

    # 출력 설정
    format = Column(String(20), default="csv")                      # csv / json / parquet
    include_metadata = Column(Boolean, default=True)
    compression = Column(String(20), default="none")                # none / gzip

    # 상태 추적
    status = Column(String(20), default="queued")                   # queued / processing / ready / failed / expired
    progress = Column(Integer, default=0)                           # 0~100
    total_rows = Column(BigInteger, default=0)
    file_size_bytes = Column(BigInteger, default=0)
    file_name = Column(String(300), default="")                     # MinIO object name
    storage_bucket = Column(String(100), default="sdl-files")
    error_message = Column(Text, default="")

    # 데이터 프로파일 (자동 생성)
    profile = Column(JSON, default={})                              # 컬럼별 min/max/avg/null_ratio 등

    # 시간
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)                    # 다운로드 만료 시간
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "requestId": self.request_id,
            "name": self.name,
            "description": self.description,
            "requestedBy": self.requested_by,
            "catalogId": self.catalog_id,
            "tags": self.tags or [],
            "connectorTypes": self.connector_types or [],
            "connectorIds": self.connector_ids or [],
            "dateFrom": self.date_from.isoformat() if self.date_from else None,
            "dateTo": self.date_to.isoformat() if self.date_to else None,
            "qualityMin": self.quality_min,
            "samplingMethod": self.sampling_method,
            "samplingInterval": self.sampling_interval,
            "samplingRatio": self.sampling_ratio,
            "format": self.format,
            "includeMetadata": self.include_metadata,
            "compression": self.compression,
            "status": self.status,
            "progress": self.progress,
            "totalRows": self.total_rows,
            "fileSizeBytes": self.file_size_bytes,
            "fileSizeDisplay": _fmt_bytes(self.file_size_bytes),
            "fileName": self.file_name,
            "errorMessage": self.error_message,
            "profile": self.profile or {},
            "startedAt": self.started_at.isoformat() if self.started_at else None,
            "completedAt": self.completed_at.isoformat() if self.completed_at else None,
            "expiresAt": self.expires_at.isoformat() if self.expires_at else None,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


def _fmt_bytes(b):
    if not b:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"
