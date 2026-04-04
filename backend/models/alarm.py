"""알람 관리 모델 — 규칙, 이벤트, 알림 채널"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Float, JSON
from backend.database import Base


class AlarmRule(Base):
    """알람 규칙 정의"""
    __tablename__ = "alarm_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    enabled = Column(Boolean, default=True)

    # 대상: infra / connector / pipeline / system
    source_type = Column(String(30), nullable=False)
    # 특정 컴포넌트명 또는 "*" (해당 타입 전체)
    source_name = Column(String(200), default="*")

    # 조건: status == down | status == error | cpu > 90 | memory > 90 | disk > 90
    condition = Column(String(300), nullable=False)
    severity = Column(String(20), default="warning")  # critical / warning / info

    # 같은 소스에서 반복 알람 억제 (초)
    cooldown_sec = Column(Integer, default=300)
    message_template = Column(String(500), default="")

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AlarmEvent(Base):
    """발생한 알람 이벤트 이력"""
    __tablename__ = "alarm_event"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, nullable=True)       # AlarmRule.id (삭제 시에도 이벤트 보존)
    rule_name = Column(String(200), default="")

    source_type = Column(String(30), default="")
    source_name = Column(String(200), default="")
    severity = Column(String(20), default="warning")
    message = Column(Text, default="")

    status = Column(String(20), default="active")  # active / acknowledged / resolved
    fired_at = Column(DateTime, default=datetime.utcnow)
    acked_at = Column(DateTime, nullable=True)
    resolved_at = Column(DateTime, nullable=True)


class AlarmChannel(Base):
    """알림 발송 채널"""
    __tablename__ = "alarm_channel"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    enabled = Column(Boolean, default=True)

    # webhook / email
    channel_type = Column(String(20), nullable=False)
    # JSON: webhook → {"url": "..."}, email → {"smtp_host":..., "recipients":[...]}
    config = Column(JSON, default=dict)
    # 수신 심각도 필터: ["critical","warning","info"]
    severity_filter = Column(JSON, default=list)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
