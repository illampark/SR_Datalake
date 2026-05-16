"""MinIO Object Index 모델 — 이벤트 기반 객체 인덱스

MinIO Bucket Notification(webhook)이 객체 생성/삭제 이벤트를 보내면
backend/services/minio_indexer 가 이 테이블을 upsert/delete 로 실시간 갱신한다.

전환 배경(claudedocs/minio-event-index-design.md): 기존 '주기적 전체 LIST +
파일 캐시(/tmp/sdl_minio_cache.json)' 를 대체 — MinIO 가 변경분만 push 하고
UI(/browse·/status·/stats·이관 여부·정리)는 이 테이블을 SQL 로 조회한다.

file_index(local_path inbox 인덱스)와 의도적으로 동일한 구조 — 'DB 인덱스 테이블'
패턴 통일.
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger, Column, DateTime, Index, String, UniqueConstraint,
)

from backend.database import Base


class MinioObject(Base):
    __tablename__ = "minio_object"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    bucket = Column(String(63), nullable=False)
    object_name = Column(String(1024), nullable=False)              # 전체 키
    name = Column(String(512), nullable=False)                       # basename
    parent_path = Column(String(1024), nullable=False, default="")   # 디렉토리 prefix (끝 '/' 없음)
    ftype = Column(String(20), default="")                           # _classify() 결과
    extension = Column(String(20), default="")
    size = Column(BigInteger, default=0)
    etag = Column(String(64), default="")
    content_type = Column(String(160), default="")
    last_modified = Column(DateTime, nullable=True)                  # 이벤트의 객체 시각
    event_seq = Column(String(40), default="")                       # MinIO sequencer — 순서 가드
    indexed_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("bucket", "object_name", name="uq_minio_object_key"),
        Index("ix_minio_object_browse", "bucket", "parent_path"),
        Index("ix_minio_object_ftype", "bucket", "ftype"),
        Index("ix_minio_object_mtime", "bucket", "last_modified"),
        # import/{cid}/ prefix LIKE (이관 여부/정리) — varchar_pattern_ops 로
        # 비-C locale 에서도 prefix 범위 스캔 가능. 비-PG 백엔드면 ops 무시됨.
        Index("ix_minio_object_prefix", "bucket", "object_name",
              postgresql_ops={"object_name": "varchar_pattern_ops"}),
    )

    def to_dict(self):
        return {
            "bucket": self.bucket,
            "objectName": self.object_name,
            "name": self.name,
            "path": "/" + self.parent_path + "/" if self.parent_path else "/",
            "type": self.ftype or "other",
            "extension": self.extension or "",
            "size": int(self.size or 0),
            "etag": self.etag or "",
            "contentType": self.content_type or "",
            "modifiedAt": self.last_modified.isoformat() if self.last_modified else None,
        }
