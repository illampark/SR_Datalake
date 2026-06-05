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

# ── Multi-tenant Feature Flag ─────────────────────────────────────────────
# claudedocs/multitenant-design-v1.md § 2 D11 — 단일테넌트 SKU와 멀티테넌트
# SKU를 한 코드베이스에서 분기. 기본값 off (현 단일테넌트 동작 100% 유지).
#
# off 일 때:
#   - 모든 쿼리는 암묵적으로 tenant_id=1 으로 동작
#   - 로그인 UX 변경 없음
#   - 신규 멀티테넌트 라우트(/api/sys/*, /api/tenant/me/*)는 동작하지만 진입 0
#
# on 일 때 (Phase 4 이후 스테이징부터):
#   - before_request 단계에서 g.tenant_id 주입
#   - 모든 도메인 쿼리에 tenant 필터 적용
#   - RBAC가 4-role + 경로 4분류 정책 사용
MULTITENANT_MODE = os.getenv("MULTITENANT_MODE", "off").lower() == "on"
