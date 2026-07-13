import os
from urllib.parse import urlsplit, unquote

# Flask Session
SECRET_KEY = os.getenv("SECRET_KEY", "sdl-secret-key-2025-change-in-production")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://sdl_user:sdl_password_2025@localhost:5432/sdl"
)


def _db_parts(url):
    """DATABASE_URL → (host, port, database, user, password).

    내부 PostgreSQL 에 psycopg2 로 직접 붙는 경로(TSDB/RDBMS 인스턴스, 보관정책)가
    자격증명을 각자 하드코딩하지 않고 여기서만 읽도록 하는 단일 출처.
    """
    s = urlsplit(url)
    return (
        s.hostname or "localhost",
        s.port or 5432,
        (s.path or "/").lstrip("/") or "postgres",
        unquote(s.username or ""),
        unquote(s.password or ""),
    )


DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = _db_parts(DATABASE_URL)

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


# ── Phase 8 — import collector local_path 화이트리스트 ────────────────────
# tenant_id 별 허용 base prefix. 운영 인프라 정렬(Phase 6) 시 조정.
# tenant 1 (legacy): 컨테이너 기본 + 임시 영역 + 운영 NFS 후보.
# 다른 tenant: DEFAULT_TEMPLATE 적용 (`/app/static/uploads/import/t-{N}/`).
LOCAL_PATH_BASES_BY_TENANT = {
    1: [
        "/app/static/uploads/import/",   # 컨테이너 내부 default
        "/data/sdl-imports/",            # legacy host bind (계획)
        "/tmp/",                         # 임시 — 운영 정렬 후 제거 권장
    ],
}
LOCAL_PATH_DEFAULT_TEMPLATE = "/app/static/uploads/import/t-{tid}/"
