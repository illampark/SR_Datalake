"""File Index 모델 — local_path 디렉토리 스캔 결과를 PG에 캐싱

UI 의 /local/browse 응답을 ms 단위로 빠르게 만들기 위해, file_indexer 서비스가
주기적으로 os.walk 결과를 이 테이블에 upsert / stale row 삭제한다.
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Index, Integer, String, Text,
    UniqueConstraint,
)

from backend.database import Base


class FileIndex(Base):
    __tablename__ = "file_index"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    collector_id = Column(Integer, nullable=False)
    rel_path = Column(String(1024), nullable=False)        # base 기준 상대 (디렉토리는 끝에 '/' 없음)
    parent_path = Column(String(1024), nullable=False, default="")
    name = Column(String(512), nullable=False)
    is_dir = Column(Boolean, default=False, nullable=False)
    ftype = Column(String(20), default="")                 # _classify() 결과
    extension = Column(String(20), default="")
    size = Column(BigInteger, default=0)
    modified_at = Column(DateTime, nullable=True)
    indexed_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("collector_id", "rel_path", name="uq_file_index_path"),
        Index("ix_file_index_browse", "collector_id", "parent_path", "is_dir"),
        Index("ix_file_index_collector", "collector_id"),
        Index("ix_file_index_mtime", "collector_id", "modified_at"),
    )


class FileIndexState(Base):
    """인덱서 상태 — collector 별 마지막 스캔 정보"""
    __tablename__ = "file_index_state"

    collector_id = Column(Integer, primary_key=True)
    last_scan_started_at = Column(DateTime, nullable=True)
    last_scan_finished_at = Column(DateTime, nullable=True)
    last_scan_files = Column(Integer, default=0)
    last_scan_dirs = Column(Integer, default=0)
    last_scan_duration_ms = Column(BigInteger, default=0)
    last_error = Column(Text, default="")
    is_running = Column(Boolean, default=False, nullable=False)

    def to_dict(self):
        return {
            "collectorId": self.collector_id,
            "lastScanStartedAt": self.last_scan_started_at.isoformat() if self.last_scan_started_at else None,
            "lastScanFinishedAt": self.last_scan_finished_at.isoformat() if self.last_scan_finished_at else None,
            "lastScanFiles": self.last_scan_files or 0,
            "lastScanDirs": self.last_scan_dirs or 0,
            "lastScanDurationMs": self.last_scan_duration_ms or 0,
            "lastError": self.last_error or "",
            "isRunning": bool(self.is_running),
        }
