"""시스템 로그 모델 — Python logging → PostgreSQL 저장용"""

from datetime import datetime

from sqlalchemy import BigInteger, Column, DateTime, Index, String, Text
from sqlalchemy.dialects.postgresql import JSON

from backend.database import Base


class SystemLog(Base):
    __tablename__ = "system_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    level = Column(String(10), nullable=False)       # DEBUG / INFO / WARNING / ERROR
    component = Column(String(100), nullable=False)   # 한글 표시명
    logger_name = Column(String(200), default="")     # 원본 logger name
    message = Column(Text, nullable=False)
    host = Column(String(100), default="")
    extra = Column(JSON, nullable=True)               # traceback, pathname, lineno 등

    __table_args__ = (
        Index("ix_syslog_ts", "timestamp"),
        Index("ix_syslog_level", "level"),
        Index("ix_syslog_component", "component"),
        Index("ix_syslog_ts_level", "timestamp", "level"),
    )
