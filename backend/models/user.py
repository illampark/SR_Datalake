"""사용자 관리 모델 — 사용자, 로그인 이력, 관리 설정"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index
from backend.database import Base


class User(Base):
    """시스템 사용자"""
    __tablename__ = "app_user"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False)
    display_name = Column(String(100), nullable=False)
    email = Column(String(200), default="")
    password_hash = Column(String(200), nullable=False)
    role = Column(String(30), default="viewer")  # admin / engineer / operator / viewer
    enabled = Column(Boolean, default=True)
    login_fail_count = Column(Integer, default=0)
    locked_until = Column(DateTime, nullable=True)
    last_login_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "displayName": self.display_name,
            "email": self.email,
            "role": self.role,
            "enabled": self.enabled,
            "loginFailCount": self.login_fail_count,
            "lockedUntil": self.locked_until.strftime("%Y-%m-%d %H:%M:%S") if self.locked_until else None,
            "lastLoginAt": self.last_login_at.strftime("%Y-%m-%d %H:%M:%S") if self.last_login_at else None,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else "",
            "updatedAt": self.updated_at.strftime("%Y-%m-%d %H:%M:%S") if self.updated_at else "",
        }


class LoginHistory(Base):
    """로그인 이력"""
    __tablename__ = "login_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), nullable=False)
    event_type = Column(String(30), nullable=False)  # login / logout / fail / lock / unlock
    ip_address = Column(String(50), default="")
    user_agent = Column(String(300), default="")
    detail = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_login_hist_user", "username"),
        Index("ix_login_hist_ts", "created_at"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "eventType": self.event_type,
            "ipAddress": self.ip_address,
            "userAgent": self.user_agent,
            "detail": self.detail,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else "",
        }


class AdminSetting(Base):
    """관리 설정 (KV 저장)"""
    __tablename__ = "admin_setting"

    key = Column(String(100), primary_key=True)
    value = Column(String(500), default="")
