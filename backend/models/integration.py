from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, JSON
from backend.database import Base


class ExternalConnection(Base):
    """외부 연동 연결 설정 (시계열DB / 관계형DB / Kafka / 파일 스토리지)"""
    __tablename__ = "external_connection"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    connection_type = Column(String(30), nullable=False)       # tsdb / rdbms / kafka / file_storage
    host = Column(String(500), default="")
    port = Column(Integer, default=0)
    database_name = Column(String(200), default="")
    username = Column(String(100), default="")
    password = Column(String(200), default="")
    config = Column(JSON, default={})                          # 타입별 추가 설정
    enabled = Column(Boolean, default=True)
    status = Column(String(20), default="unknown")             # connected / error / unknown
    last_tested_at = Column(DateTime, nullable=True)
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "connectionType": self.connection_type,
            "host": self.host,
            "port": self.port,
            "databaseName": self.database_name,
            "username": self.username,
            "password": self.password,
            "config": self.config or {},
            "enabled": self.enabled,
            "status": self.status,
            "lastTestedAt": self.last_tested_at.isoformat() if self.last_tested_at else None,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }
