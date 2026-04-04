"""감사 로그 모델 — 사용자 행위 추적"""

from datetime import datetime
from sqlalchemy import Column, BigInteger, String, DateTime, JSON, Index
from backend.database import Base


class AuditLog(Base):
    """감사 로그 — 사용자의 모든 주요 행위를 기록"""
    __tablename__ = "audit_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    username = Column(String(50), nullable=False)
    action_type = Column(String(30), nullable=False)       # login/config/connector/user/security/data/alarm/pipeline
    action = Column(String(100), nullable=False)            # auth.login, user.create, connector.restart, ...
    target_type = Column(String(50), default="")            # user, connector, pipeline, alarm_rule, ...
    target_name = Column(String(200), default="")           # 대상 식별자
    ip_address = Column(String(50), default="")
    user_agent = Column(String(500), default="")
    result = Column(String(20), default="success")          # success / failure
    detail = Column(JSON, default={})                       # 변경 전/후 값 등

    __table_args__ = (
        Index("ix_audit_ts", "timestamp"),
        Index("ix_audit_user", "username"),
        Index("ix_audit_action_type", "action_type"),
        Index("ix_audit_result", "result"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S") if self.timestamp else "",
            "username": self.username,
            "actionType": self.action_type,
            "action": self.action,
            "targetType": self.target_type,
            "targetName": self.target_name,
            "ipAddress": self.ip_address,
            "userAgent": self.user_agent,
            "result": self.result,
            "detail": self.detail or {},
        }
