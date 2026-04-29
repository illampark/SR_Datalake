from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from backend.database import Base


class MqttConnector(Base):
    __tablename__ = "mqtt_connector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    host = Column(String(200), nullable=False, default="localhost")
    port = Column(Integer, nullable=False, default=1883)
    status = Column(String(20), default="stopped")  # running / stopped / error
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats (updated by callback)
    message_rate = Column(Float, default=0.0)
    message_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_message_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tags = relationship("MqttTag", back_populates="connector", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "host": self.host,
            "port": self.port,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "tagCount": len(self.tags) if self.tags else 0,
            "messageRate": self.message_rate,
            "messageCount": self.message_count,
            "errorCount": self.error_count,
            "lastMessageAt": self.last_message_at.isoformat() if self.last_message_at else None,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

    def broker_url(self):
        scheme = "ssl" if self.config.get("tls", False) else "tcp"
        return f"{scheme}://{self.host}:{self.port}"

    def benthos_stream_id(self):
        return f"mqtt-{self.id}"


class MqttTag(Base):
    __tablename__ = "mqtt_tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(Integer, ForeignKey("mqtt_connector.id", ondelete="CASCADE"), nullable=False)
    topic = Column(String(500), nullable=False)
    tag_name = Column(String(200), nullable=False)
    data_type = Column(String(50), default="string")  # float / int / string / json
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    connector = relationship("MqttConnector", back_populates="tags")

    def to_dict(self):
        return {
            "id": self.id,
            "connectorId": self.connector_id,
            "topic": self.topic,
            "tagName": self.tag_name,
            "dataType": self.data_type,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# DB Connector Models
# ══════════════════════════════════════════════

class DbConnector(Base):
    __tablename__ = "db_connector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    db_type = Column(String(20), nullable=False, default="mysql")   # oracle / mssql / postgresql / mysql
    host = Column(String(200), nullable=False, default="localhost")
    port = Column(Integer, nullable=False, default=3306)
    database = Column(String(200), nullable=False, default="")
    schema_name = Column(String(100), default="")
    username = Column(String(100), default="")
    password = Column(String(200), default="")
    collect_mode = Column(String(20), default="polling")  # polling / cdc / trigger
    status = Column(String(20), default="stopped")  # running / stopped / error
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats
    row_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_collected_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tags = relationship("DbTag", back_populates="connector", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "dbType": self.db_type,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "schemaName": self.schema_name,
            "username": self.username,
            "collectMode": self.collect_mode,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "tagCount": len(self.tags) if self.tags else 0,
            "rowCount": self.row_count,
            "errorCount": self.error_count,
            "lastCollectedAt": self.last_collected_at.isoformat() if self.last_collected_at else None,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

    def dsn(self):
        """Build DB-specific DSN for Benthos."""
        if self.db_type in ("mysql", "mariadb"):
            return f"{self.username}:{self.password}@tcp({self.host}:{self.port})/{self.database}"
        elif self.db_type == "postgresql":
            return f"postgres://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode=disable"
        elif self.db_type == "mssql":
            return f"sqlserver://{self.username}:{self.password}@{self.host}:{self.port}?database={self.database}"
        elif self.db_type == "oracle":
            return f"oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        return ""

    def benthos_driver(self):
        """Return Benthos SQL driver name."""
        drivers = {"mysql": "mysql", "mariadb": "mysql", "postgresql": "postgres",
                    "mssql": "mssql", "oracle": "oracle"}
        return drivers.get(self.db_type, "mysql")

    def benthos_stream_id(self):
        return f"db-{self.id}"


class DbTag(Base):
    __tablename__ = "db_tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(Integer, ForeignKey("db_connector.id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String(200), nullable=False)
    tag_name = Column(String(200), nullable=False)
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    connector = relationship("DbConnector", back_populates="tags")

    def to_dict(self):
        return {
            "id": self.id,
            "connectorId": self.connector_id,
            "tableName": self.table_name,
            "tagName": self.tag_name,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# File Collector Models
# ══════════════════════════════════════════════

class FileCollector(Base):
    __tablename__ = "file_collector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    # SFTP 접속 정보
    sftp_host = Column(String(200), nullable=False, default="localhost")
    sftp_port = Column(Integer, nullable=False, default=22)
    sftp_username = Column(String(100), default="")
    sftp_password = Column(String(200), default="")
    sftp_key_path = Column(String(500), default="")
    sftp_auth_type = Column(String(20), default="password")  # password / key
    # 원격 경로 (SFTP 서버 상의 경로)
    watch_path = Column(String(500), nullable=False)
    file_patterns = Column(JSON, default=[])          # ["*.csv", "*.log"]
    recursive = Column(Boolean, default=True)          # 하위 디렉토리 포함
    modified_only = Column(Boolean, default=True)      # 변경된 파일만 수집
    collect_mode = Column(String(20), default="poll")  # poll (SFTP는 poll만 지원)
    poll_interval = Column(Integer, default=30)        # 폴링 주기 (초)
    encoding = Column(String(20), default="utf-8")
    post_action = Column(String(20), default="none")   # move / delete / none
    archive_path = Column(String(500), default="")     # SFTP 서버 상의 아카이브 경로
    parser_type = Column(String(20), default="line")   # line / csv / json / regex
    # 저장 모드: parse(기존 파싱→MQTT) / direct(SFTP→MinIO 직접 저장)
    storage_mode = Column(String(20), default="parse")
    target_bucket = Column(String(100), default="sdl-files")
    target_path_prefix = Column(String(500), default="raw/{collector_id}/{date}/")
    status = Column(String(20), default="stopped")     # running / stopped / error
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats
    file_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_file_at = Column(DateTime, nullable=True)
    last_file_name = Column(String(300), default="")
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def benthos_stream_id(self):
        return f"file-{self.id}"

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "sftpHost": self.sftp_host,
            "sftpPort": self.sftp_port,
            "sftpUsername": self.sftp_username,
            "sftpAuthType": self.sftp_auth_type,
            "sftpKeyPath": self.sftp_key_path,
            "watchPath": self.watch_path,
            "filePatterns": self.file_patterns or [],
            "recursive": self.recursive,
            "modifiedOnly": self.modified_only,
            "collectMode": self.collect_mode,
            "pollInterval": self.poll_interval,
            "encoding": self.encoding,
            "postAction": self.post_action,
            "archivePath": self.archive_path,
            "parserType": self.parser_type,
            "storageMode": self.storage_mode,
            "targetBucket": self.target_bucket,
            "targetPathPrefix": self.target_path_prefix,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "fileCount": self.file_count,
            "errorCount": self.error_count,
            "lastFileAt": self.last_file_at.isoformat() if self.last_file_at else None,
            "lastFileName": self.last_file_name,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }


# ══════════════════════════════════════════════
# OPC-UA Connector Models
# ══════════════════════════════════════════════

class OpcuaConnector(Base):
    __tablename__ = "opcua_connector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    server_url = Column(String(500), nullable=False, default="opc.tcp://localhost:4840")
    namespace_index = Column(Integer, default=2)
    security_policy = Column(String(50), default="None")         # None / Basic256 / Basic256Sha256
    security_mode = Column(String(50), default="None")           # None / Sign / SignAndEncrypt
    auth_type = Column(String(20), default="anonymous")          # anonymous / username / certificate
    username = Column(String(100), default="")
    password = Column(String(200), default="")
    polling_interval = Column(Integer, default=1000)             # ms
    status = Column(String(20), default="stopped")
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats
    point_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_collected_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tags = relationship("OpcuaTag", back_populates="connector", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "serverUrl": self.server_url,
            "namespaceIndex": self.namespace_index,
            "securityPolicy": self.security_policy,
            "securityMode": self.security_mode,
            "authType": self.auth_type,
            "username": self.username,
            "pollingInterval": self.polling_interval,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "tagCount": len(self.tags) if self.tags else 0,
            "pointCount": self.point_count,
            "errorCount": self.error_count,
            "lastCollectedAt": self.last_collected_at.isoformat() if self.last_collected_at else None,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

    def benthos_stream_id(self):
        return f"opcua-{self.id}"


class OpcuaTag(Base):
    __tablename__ = "opcua_tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(Integer, ForeignKey("opcua_connector.id", ondelete="CASCADE"), nullable=False)
    node_id = Column(String(500), nullable=False)
    tag_name = Column(String(200), nullable=False)
    data_type = Column(String(50), default="float")
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    connector = relationship("OpcuaConnector", back_populates="tags")

    def to_dict(self):
        return {
            "id": self.id,
            "connectorId": self.connector_id,
            "nodeId": self.node_id,
            "tagName": self.tag_name,
            "dataType": self.data_type,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# OPC-DA Connector Models
# ══════════════════════════════════════════════

class OpcdaConnector(Base):
    __tablename__ = "opcda_connector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    server_name = Column(String(300), nullable=False, default="")
    host = Column(String(200), nullable=False, default="localhost")
    polling_interval = Column(Integer, default=1000)
    dcom_auth = Column(String(20), default="default")
    username = Column(String(100), default="")
    password = Column(String(200), default="")
    status = Column(String(20), default="stopped")
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats
    group_count = Column(Integer, default=0)
    point_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_collected_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tags = relationship("OpcdaTag", back_populates="connector", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "serverName": self.server_name,
            "host": self.host,
            "pollingInterval": self.polling_interval,
            "dcomAuth": self.dcom_auth,
            "username": self.username,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "tagCount": len(self.tags) if self.tags else 0,
            "groupCount": self.group_count,
            "pointCount": self.point_count,
            "errorCount": self.error_count,
            "lastCollectedAt": self.last_collected_at.isoformat() if self.last_collected_at else None,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

    def benthos_stream_id(self):
        return f"opcda-{self.id}"


class OpcdaTag(Base):
    __tablename__ = "opcda_tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(Integer, ForeignKey("opcda_connector.id", ondelete="CASCADE"), nullable=False)
    group_name = Column(String(200), nullable=False)
    item_name = Column(String(500), nullable=False)
    tag_name = Column(String(200), nullable=False)
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    connector = relationship("OpcdaConnector", back_populates="tags")

    def to_dict(self):
        return {
            "id": self.id,
            "connectorId": self.connector_id,
            "groupName": self.group_name,
            "itemName": self.item_name,
            "tagName": self.tag_name,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# Modbus Connector Models
# ══════════════════════════════════════════════

class ModbusConnector(Base):
    __tablename__ = "modbus_connector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    modbus_type = Column(String(10), nullable=False, default="tcp")
    host = Column(String(200), default="localhost")
    port = Column(Integer, default=502)
    serial_port = Column(String(200), default="")
    baudrate = Column(Integer, default=9600)
    data_bits = Column(Integer, default=8)
    parity = Column(String(1), default="N")
    stop_bits = Column(Integer, default=1)
    slave_ids = Column(String(200), default="1")
    polling_interval = Column(Integer, default=1000)
    function_codes = Column(JSON, default=[3, 4])
    timeout = Column(Integer, default=3000)
    status = Column(String(20), default="stopped")
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats
    point_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    last_collected_at = Column(DateTime, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tags = relationship("ModbusTag", back_populates="connector", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "modbusType": self.modbus_type,
            "host": self.host,
            "port": self.port,
            "serialPort": self.serial_port,
            "baudrate": self.baudrate,
            "dataBits": self.data_bits,
            "parity": self.parity,
            "stopBits": self.stop_bits,
            "slaveIds": self.slave_ids,
            "pollingInterval": self.polling_interval,
            "functionCodes": self.function_codes or [3, 4],
            "timeout": self.timeout,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "tagCount": len(self.tags) if self.tags else 0,
            "pointCount": self.point_count,
            "errorCount": self.error_count,
            "lastCollectedAt": self.last_collected_at.isoformat() if self.last_collected_at else None,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

    def benthos_stream_id(self):
        return f"modbus-{self.id}"


class ModbusTag(Base):
    __tablename__ = "modbus_tag"

    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(Integer, ForeignKey("modbus_connector.id", ondelete="CASCADE"), nullable=False)
    tag_name = Column(String(200), nullable=False)
    function_code = Column(Integer, default=3)
    register_address = Column(Integer, nullable=False)
    register_count = Column(Integer, default=1)
    data_type = Column(String(50), default="int16")
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    connector = relationship("ModbusConnector", back_populates="tags")

    def to_dict(self):
        return {
            "id": self.id,
            "connectorId": self.connector_id,
            "tagName": self.tag_name,
            "functionCode": self.function_code,
            "registerAddress": self.register_address,
            "registerCount": self.register_count,
            "dataType": self.data_type,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# API Connector Models (MES/ERP/SCADA REST API)
# ══════════════════════════════════════════════

class ApiConnector(Base):
    __tablename__ = "api_connector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    base_url = Column(String(1000), nullable=False)
    auth_type = Column(String(20), default="none")          # none / basic / bearer / apikey / oauth2
    auth_config = Column(JSON, default={})                   # 인증 세부 설정 (토큰, 키 등)
    request_format = Column(String(10), default="json")      # json / xml / form
    schedule = Column(String(100), default="*/5 * * * *")    # cron 표현식
    timeout = Column(Integer, default=30)                    # 초
    custom_headers = Column(JSON, default={})                # 커스텀 HTTP 헤더
    status = Column(String(20), default="stopped")           # running / stopped / error
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # runtime stats
    request_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    avg_response_ms = Column(Float, default=0.0)
    last_called_at = Column(DateTime, nullable=True)
    last_status_code = Column(Integer, nullable=True)
    last_error = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    endpoints = relationship("ApiEndpoint", back_populates="connector", cascade="all, delete-orphan")

    def to_dict(self):
        # 인증 설정에서 비밀 정보 마스킹
        safe_auth = dict(self.auth_config or {})
        for k in ("password", "clientSecret", "keyValue", "token"):
            if k in safe_auth and safe_auth[k]:
                safe_auth[k] = "****"
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "baseUrl": self.base_url,
            "authType": self.auth_type,
            "authConfig": safe_auth,
            "requestFormat": self.request_format,
            "schedule": self.schedule,
            "timeout": self.timeout,
            "customHeaders": self.custom_headers or {},
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "endpointCount": len(self.endpoints) if self.endpoints else 0,
            "requestCount": self.request_count,
            "successCount": self.success_count,
            "errorCount": self.error_count,
            "avgResponseMs": self.avg_response_ms,
            "lastCalledAt": self.last_called_at.isoformat() if self.last_called_at else None,
            "lastStatusCode": self.last_status_code,
            "lastError": self.last_error,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

    def benthos_stream_id(self):
        return f"api-{self.id}"


class ApiEndpoint(Base):
    __tablename__ = "api_endpoint"

    id = Column(Integer, primary_key=True, autoincrement=True)
    connector_id = Column(Integer, ForeignKey("api_connector.id", ondelete="CASCADE"), nullable=False)
    tag_name = Column(String(200), nullable=False)           # 파이프라인 태그명
    method = Column(String(10), default="GET")               # GET / POST / PUT / DELETE
    path = Column(String(500), default="/")                  # 엔드포인트 경로
    body_template = Column(Text, default="")                 # POST/PUT 요청 바디 템플릿
    response_path = Column(String(500), default="")          # JSONPath ($.data.value)
    data_type = Column(String(50), default="json")           # json / float / int / string
    enabled = Column(Boolean, default=True)
    description = Column(String(500), default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    connector = relationship("ApiConnector", back_populates="endpoints")

    def to_dict(self):
        return {
            "id": self.id,
            "connectorId": self.connector_id,
            "tagName": self.tag_name,
            "method": self.method,
            "path": self.path,
            "bodyTemplate": self.body_template,
            "responsePath": self.response_path,
            "dataType": self.data_type,
            "enabled": self.enabled,
            "description": self.description,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }


# ══════════════════════════════════════════════
# Import Collector Model (오프라인 데이터 가져오기)
# ══════════════════════════════════════════════

class ImportCollector(Base):
    __tablename__ = "import_collector"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(String(500), default="")
    # 가져오기 설정
    import_type = Column(String(20), nullable=False, default="csv")  # csv / json / sql_dump
    target_type = Column(String(20), nullable=False, default="tsdb")  # tsdb / rdbms / file
    target_id = Column(Integer, nullable=True)                        # TSDB/RDBMS config ID
    target_table = Column(String(200), default="")                    # 대상 테이블명 (RDBMS)
    target_measurement = Column(String(200), default="")              # 대상 measurement (TSDB)
    target_bucket = Column(String(100), default="sdl-files")          # 대상 버킷 (file)
    # 파일 정보
    file_name = Column(String(500), default="")
    file_path = Column(String(1000), default="")  # MinIO 업로드 경로
    file_size = Column(Integer, default=0)
    # 컬럼 매핑
    column_mapping = Column(JSON, default={})  # {"sourceCol": "targetCol", ...}
    timestamp_column = Column(String(200), default="")  # 시계열용 타임스탬프 컬럼
    tag_column = Column(String(200), default="")         # 시계열용 태그 컬럼
    value_columns = Column(JSON, default=[])             # 값 컬럼 목록
    # 실행 설정
    batch_size = Column(Integer, default=1000)
    encoding = Column(String(20), default="utf-8")
    delimiter = Column(String(5), default=",")
    skip_header = Column(Boolean, default=True)
    publish_mqtt = Column(Boolean, default=True)  # MQTT 발행 여부 (파이프라인 연계)
    # xlsx 옵션 (import_type='xlsx' 일 때만 사용)
    sheet_name = Column(String(200), default="")  # 빈 값이면 첫 시트
    header_row = Column(Integer, default=1)        # 1-base, 헤더가 있는 행 번호
    # 소스 모드
    source_mode = Column(String(20), default="upload")  # upload / local_path
    local_path = Column(String(1000), default="")        # 서버 로컬 경로
    file_patterns = Column(JSON, default=["*"])           # 파일 패턴 ["*.csv", "*.jpg"]
    recursive = Column(Boolean, default=True)             # 하위 디렉토리 포함
    # 상태
    status = Column(String(20), default="ready")  # ready / running / completed / error
    enabled = Column(Boolean, default=True)
    config = Column(JSON, default={})
    # 실행 통계
    total_rows = Column(Integer, default=0)
    imported_rows = Column(Integer, default=0)
    error_rows = Column(Integer, default=0)
    progress = Column(Integer, default=0)  # 0~100
    error_count = Column(Integer, default=0)
    last_error = Column(Text, default="")
    last_imported_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "importType": self.import_type,
            "targetType": self.target_type,
            "targetId": self.target_id,
            "targetTable": self.target_table,
            "targetMeasurement": self.target_measurement,
            "targetBucket": self.target_bucket,
            "fileName": self.file_name,
            "filePath": self.file_path,
            "fileSize": self.file_size,
            "columnMapping": self.column_mapping or {},
            "timestampColumn": self.timestamp_column,
            "tagColumn": self.tag_column,
            "valueColumns": self.value_columns or [],
            "batchSize": self.batch_size,
            "encoding": self.encoding,
            "delimiter": self.delimiter,
            "skipHeader": self.skip_header,
            "publishMqtt": self.publish_mqtt,
            "sheetName": self.sheet_name or "",
            "headerRow": self.header_row if self.header_row else 1,
            "sourceMode": self.source_mode or "upload",
            "localPath": self.local_path or "",
            "filePatterns": self.file_patterns or ["*"],
            "recursive": self.recursive if self.recursive is not None else True,
            "status": self.status,
            "enabled": self.enabled,
            "config": self.config or {},
            "totalRows": self.total_rows,
            "importedRows": self.imported_rows,
            "errorRows": self.error_rows,
            "progress": self.progress,
            "errorCount": self.error_count,
            "lastError": self.last_error,
            "lastImportedAt": self.last_imported_at.isoformat() if self.last_imported_at else None,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }