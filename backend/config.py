import os

# Flask Session
SECRET_KEY = os.getenv("SECRET_KEY", "sdl-secret-key-2025-change-in-production")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://sdl_user:sdl_password_2025@localhost:5432/sdl"
)

# MinIO S3-compatible Object Storage
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "sdladmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "sdladmin1234")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKETS = ["sdl-files", "sdl-archive", "sdl-backup"]
# MinIO 이벤트 기반 객체 인덱스 (claudedocs/minio-event-index-design.md)
MINIO_WEBHOOK_TOKEN = os.getenv("MINIO_WEBHOOK_TOKEN", "")   # webhook 수신 공유 비밀

# Benthos (Redpanda Connect) Streams API
BENTHOS_BIN = os.getenv("BENTHOS_BIN", os.path.expanduser("~/.local/bin/redpanda-connect"))
BENTHOS_API = os.getenv("BENTHOS_API", "http://localhost:4195")

# MQTT default broker (for development)
MQTT_DEFAULT_HOST = os.getenv("MQTT_DEFAULT_HOST", "localhost")
MQTT_DEFAULT_PORT = int(os.getenv("MQTT_DEFAULT_PORT", "1883"))
