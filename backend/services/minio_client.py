"""MinIO 클라이언트 중앙 관리 모듈.

DB(admin_setting)에 저장된 설정을 우선 사용하고,
없으면 config.py(환경변수) 값으로 폴백한다.
UI에서 설정 변경 시 재시작 없이 즉시 반영된다.
"""
from minio import Minio
from backend.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
)


def _get_setting(db, key, fallback):
    """admin_setting 테이블에서 값을 조회한다. 없으면 fallback 반환."""
    from backend.models.user import AdminSetting
    row = db.query(AdminSetting).filter_by(key=key).first()
    return row.value if row else fallback


def get_minio_config(db=None):
    """MinIO 접속 정보를 dict로 반환한다.

    db 세션이 주어지면 admin_setting에서 읽고, 없으면 config.py 기본값 사용.
    """
    if db is None:
        return {
            "endpoint": MINIO_ENDPOINT,
            "access_key": MINIO_ACCESS_KEY,
            "secret_key": MINIO_SECRET_KEY,
            "secure": MINIO_SECURE,
        }

    return {
        "endpoint": _get_setting(db, "minio.endpoint", MINIO_ENDPOINT),
        "access_key": _get_setting(db, "minio.access_key", MINIO_ACCESS_KEY),
        "secret_key": _get_setting(db, "minio.secret_key", MINIO_SECRET_KEY),
        "secure": _get_setting(db, "minio.secure", str(MINIO_SECURE).lower()) == "true",
    }


def get_minio_client(db=None):
    """MinIO 클라이언트를 생성하여 반환한다.

    DB 세션이 주어지면 admin_setting 설정을 사용하고,
    없으면 config.py 기본값으로 연결한다.
    """
    cfg = get_minio_config(db)
    return Minio(
        cfg["endpoint"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=cfg["secure"],
    )
