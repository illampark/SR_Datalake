"""API Gateway 모델: 접근 로그, API 키"""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, Text, Boolean, DateTime, BigInteger, Index,
)
from backend.database import Base


class ApiAccessLog(Base):
    __tablename__ = "api_access_log"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    timestamp      = Column(DateTime, default=datetime.utcnow)
    method         = Column(String(10), nullable=False)
    path           = Column(String(500), nullable=False)
    status_code    = Column(Integer, default=0)
    response_time_ms = Column(Float, default=0.0)
    remote_addr    = Column(String(50), default="")
    user_agent     = Column(String(500), default="")
    request_size   = Column(BigInteger, default=0)
    response_size  = Column(BigInteger, default=0)
    api_key_id     = Column(Integer, nullable=True)
    error_message  = Column(Text, default="")

    __table_args__ = (
        Index("ix_access_log_ts", "timestamp"),
        Index("ix_access_log_path", "path"),
        Index("ix_access_log_status", "status_code"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else "",
            "method": self.method,
            "path": self.path,
            "statusCode": self.status_code,
            "responseTimeMs": round(self.response_time_ms, 2) if self.response_time_ms else 0,
            "remoteAddr": self.remote_addr,
            "userAgent": self.user_agent or "",
            "requestSize": self.request_size or 0,
            "responseSize": self.response_size or 0,
            "apiKeyId": self.api_key_id,
            "errorMessage": self.error_message or "",
        }


class ApiKey(Base):
    __tablename__ = "api_key"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    name          = Column(String(100), nullable=False)
    key_value     = Column(String(64), unique=True, nullable=False)
    description   = Column(Text, default="")
    allowed_paths = Column(String(1000), default="*")
    is_active     = Column(Boolean, default=True)
    expires_at    = Column(DateTime, nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)
    last_used_at  = Column(DateTime, nullable=True)
    request_count = Column(Integer, default=0)

    def to_dict(self, mask_key=True):
        kv = self.key_value or ""
        if mask_key and len(kv) > 12:
            kv = kv[:8] + "..." + kv[-4:]
        return {
            "id": self.id,
            "name": self.name,
            "keyValue": kv,
            "description": self.description or "",
            "allowedPaths": self.allowed_paths or "*",
            "isActive": self.is_active,
            "expiresAt": self.expires_at.strftime("%Y-%m-%d") if self.expires_at else None,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else "",
            "lastUsedAt": self.last_used_at.strftime("%Y-%m-%d %H:%M:%S") if self.last_used_at else None,
            "requestCount": self.request_count or 0,
        }
