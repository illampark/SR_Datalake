"""API Gateway 모델: 접근 로그, API 키 (Phase 6 v2)."""

from datetime import datetime

from sqlalchemy import (
    Column, Integer, String, Float, Text, Boolean, DateTime, BigInteger, Index, ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
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
    """API 키 — Phase 6 v2.

    - tenant_id 로 키별 1 tenant 고정 (D10)
    - role 로 권한 등급 분리 (기존 viewer 고정 → 명시 선택)
    - scopes 는 미세 권한 (1차 미적용, Phase 8 본격)
    - key_prefix 는 UI 표시용 (전체 값은 발급 시 1회만 반환)
    - revoked_at + is_active 로 폐기 상태 관리
    """
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

    # ── Phase 6 추가 ──
    tenant_id  = Column(
        BigInteger,
        ForeignKey("tenant.id", ondelete="RESTRICT"),
        nullable=False, default=1, server_default="1",
    )
    role       = Column(
        String(30), nullable=False,
        default="tenant_viewer", server_default="tenant_viewer",
    )
    scopes     = Column(JSONB, nullable=False, default=list, server_default="[]")
    key_prefix = Column(String(16), nullable=True)
    revoked_at = Column(DateTime, nullable=True)
    created_by = Column(Integer, ForeignKey("app_user.id"), nullable=True)

    def to_dict(self, mask_key=True):
        kv = self.key_value or ""
        if mask_key:
            # prefix + "..." + 마지막 4자
            kv = (self.key_prefix or kv[:8]) + "..." + kv[-4:] if len(kv) > 12 else kv
        return {
            "id": self.id,
            "name": self.name,
            "keyValue": kv,
            "keyPrefix": self.key_prefix or "",
            "description": self.description or "",
            "allowedPaths": self.allowed_paths or "*",
            "isActive": self.is_active and self.revoked_at is None,
            "expiresAt": self.expires_at.strftime("%Y-%m-%d") if self.expires_at else None,
            "revokedAt": self.revoked_at.strftime("%Y-%m-%d %H:%M:%S") if self.revoked_at else None,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else "",
            "createdBy": self.created_by,
            "lastUsedAt": self.last_used_at.strftime("%Y-%m-%d %H:%M:%S") if self.last_used_at else None,
            "requestCount": self.request_count or 0,
            "tenantId": self.tenant_id,
            "role": self.role,
            "scopes": self.scopes or [],
        }
