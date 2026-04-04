"""공지사항 모델"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from backend.database import Base


class Notice(Base):
    """대시보드 공지사항"""
    __tablename__ = "notice"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    title      = Column(String(200), nullable=False)
    content    = Column(Text, nullable=False)
    # general / maintenance / urgent
    category   = Column(String(20), default="general", nullable=False)
    is_pinned  = Column(Boolean, default=False, nullable=False)
    author     = Column(String(100), default="", nullable=False)
    expires_at = Column(DateTime, nullable=True)   # null = 무기한
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow, nullable=False)

    def to_dict(self):
        return {
            "id":        self.id,
            "title":     self.title,
            "content":   self.content,
            "category":  self.category,
            "isPinned":  self.is_pinned,
            "author":    self.author,
            "expiresAt": self.expires_at.isoformat() if self.expires_at else None,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }
