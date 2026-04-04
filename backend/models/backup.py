"""백업/복구 이력 모델"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, Float, BigInteger, Index
from backend.database import Base


class BackupHistory(Base):
    """백업/복구 실행 이력"""
    __tablename__ = "backup_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    backup_type = Column(String(20), nullable=False, default="manual")
    operation = Column(String(20), nullable=False, default="backup")
    targets = Column(String(200), nullable=False, default="")
    storage_key = Column(String(500), default="")
    total_bytes = Column(BigInteger, default=0)
    duration_seconds = Column(Float, default=0.0)
    status = Column(String(20), nullable=False, default="running")
    error_message = Column(Text, default="")
    restore_from_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_backup_hist_ts", "execution_time"),
        Index("ix_backup_hist_status", "status"),
    )

    def to_dict(self):
        return {
            "id": self.id,
            "executionTime": self.execution_time.strftime("%Y-%m-%d %H:%M:%S") if self.execution_time else None,
            "backupType": self.backup_type,
            "operation": self.operation,
            "targets": self.targets,
            "storageKey": self.storage_key,
            "totalBytes": self.total_bytes,
            "durationSeconds": self.duration_seconds,
            "status": self.status,
            "errorMessage": self.error_message,
            "restoreFromId": self.restore_from_id,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
