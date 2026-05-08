from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship
from backend.database import Base


# ══════════════════════════════════════════════
# 사용자 수준 데이터 카탈로그 (수동 관리)
# ══════════════════════════════════════════════

class DataCatalog(Base):
    """데이터 카탈로그 — 사용자가 직접 등록/관리하는 데이터 자산 목록"""
    __tablename__ = "data_catalog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, default="")
    # 계층 구조 (커넥터 기반)
    connector_type = Column(String(20), nullable=False)      # opcua / modbus / mqtt / db / file / api
    connector_id = Column(Integer, nullable=True)            # 특정 커넥터 인스턴스 (NULL이면 타입 그룹)
    connector_description = Column(Text, default="")         # 커넥터 설명 (비정규화, 동기화)
    tag_name = Column(String(200), default="")               # 특정 태그 (빈 문자열이면 커넥터 그룹)
    # 사용자 관리 필드
    owner = Column(String(100), default="")                  # 데이터 소유자
    category = Column(String(100), default="")               # 사용자 정의 분류 (공정, 품질, 설비 등)
    data_level = Column(String(20), default="raw")           # raw / processed / user_created
    sensitivity = Column(String(20), default="normal")       # public / internal / confidential / restricted
    retention_policy = Column(String(100), default="")       # 보관 정책 설명
    # 접근 정보
    access_url = Column(String(500), default="")             # 접근 경로 or MQTT 토픽
    format = Column(String(50), default="")                  # json / csv / binary / avro
    schema_info = Column(Text, default="")                   # 스키마 설명 (자유 텍스트)
    # 파이프라인 연결
    pipeline_id = Column(Integer, nullable=True)             # 파이프라인 ID (NULL이면 커넥터 직접 수집)
    sink_type = Column(String(30), default="")               # internal_tsdb_sink / internal_rdbms_sink / internal_file_sink
    # 상태
    is_published = Column(Boolean, default=False)            # 카탈로그 공개 여부
    is_deprecated = Column(Boolean, default=False)           # 폐기 여부
    # 시간
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    search_tags = relationship("CatalogSearchTag", back_populates="catalog",
                               cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "connectorType": self.connector_type,
            "connectorId": self.connector_id,
            "connectorDescription": self.connector_description,
            "tagName": self.tag_name,
            "owner": self.owner,
            "category": self.category,
            "dataLevel": self.data_level,
            "sensitivity": self.sensitivity,
            "retentionPolicy": self.retention_policy,
            "accessUrl": self.access_url,
            "format": self.format,
            "schemaInfo": self.schema_info,
            "pipelineId": self.pipeline_id,
            "sinkType": self.sink_type or "",
            "isPublished": self.is_published,
            "isDeprecated": self.is_deprecated,
            "tagCount": len(self.search_tags) if self.search_tags else 0,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }


class CatalogSearchTag(Base):
    """카탈로그 검색 태그 — 사용자가 카탈로그 항목에 붙이는 검색용 키워드"""
    __tablename__ = "catalog_search_tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    catalog_id = Column(Integer, ForeignKey("data_catalog.id", ondelete="CASCADE"), nullable=False)
    tag = Column(String(100), nullable=False)                # 검색 키워드
    created_at = Column(DateTime, default=datetime.utcnow)

    catalog = relationship("DataCatalog", back_populates="search_tags")

    def to_dict(self):
        return {
            "id": self.id,
            "catalogId": self.catalog_id,
            "tag": self.tag,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# 데이터 레시피 (사용자 생성 데이터)
# ══════════════════════════════════════════════

class DataRecipe(Base):
    """데이터 레시피 — SQL 기반 데이터 변환/집계 정의"""
    __tablename__ = "data_recipe"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name            = Column(String(200), nullable=False)
    description     = Column(Text, default="")
    # 소스
    source_type     = Column(String(20), nullable=False)     # "tsdb" | "rdbms"
    rdbms_id        = Column(Integer, nullable=True)          # RdbmsConfig.id
    tsdb_id         = Column(Integer, nullable=True)          # TsdbConfig.id
    # 레시피 내용
    query_mode      = Column(String(10), default="sql")       # "visual" | "sql"
    query_sql       = Column(Text, default="")                # 최종 SQL 쿼리
    visual_config   = Column(JSON, default={})                # 비주얼 빌더 설정
    # 실행 설정
    execution_mode  = Column(String(10), default="snapshot")  # "snapshot" | "view"
    # 결과 상태
    catalog_id      = Column(Integer, nullable=True)          # 생성된 DataCatalog.id
    last_executed_at = Column(DateTime, nullable=True)
    last_row_count  = Column(Integer, default=0)
    last_error      = Column(Text, default="")
    # 관리
    owner           = Column(String(100), default="")
    category        = Column(String(100), default="")
    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    results = relationship("AggregatedData", back_populates="recipe",
                           cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "sourceType": self.source_type,
            "rdbmsId": self.rdbms_id,
            "tsdbId": self.tsdb_id,
            "queryMode": self.query_mode,
            "querySql": self.query_sql,
            "visualConfig": self.visual_config or {},
            "executionMode": self.execution_mode,
            "catalogId": self.catalog_id,
            "lastExecutedAt": self.last_executed_at.isoformat() if self.last_executed_at else None,
            "lastRowCount": self.last_row_count,
            "lastError": self.last_error,
            "owner": self.owner,
            "category": self.category,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }


class AggregatedData(Base):
    """레시피 실행 결과 — 스냅샷 모드에서 저장되는 행 데이터"""
    __tablename__ = "aggregated_data"

    id         = Column(BigInteger, primary_key=True, autoincrement=True)
    recipe_id  = Column(Integer, ForeignKey("data_recipe.id", ondelete="CASCADE"), nullable=False)
    row_data   = Column(JSON, nullable=False)     # {"col1": v1, "col2": v2, ...}
    row_index  = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    recipe = relationship("DataRecipe", back_populates="results")

    def to_dict(self):
        return {
            "id": self.id,
            "recipeId": self.recipe_id,
            "rowData": self.row_data,
            "rowIndex": self.row_index,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }
