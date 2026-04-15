"""API 키 인증 모듈.

X-API-Key 헤더를 검증해 외부 시스템의 API 호출을 허용한다.
세션 인증(UI 로그인)과 공존하며, 세션이 없는 /api/* 요청에 대해
require_login 미들웨어가 호출한다.
"""

import fnmatch
import logging
from datetime import datetime

from flask import g, request

from backend.database import SessionLocal
from backend.models.gateway import ApiKey

logger = logging.getLogger(__name__)


def _match_allowed_paths(pattern, path):
    """allowed_paths 패턴과 요청 경로 매칭 (P1: 단일 glob 패턴)."""
    if not pattern:
        return True
    p = pattern.strip()
    if p in ("", "*"):
        return True
    return fnmatch.fnmatch(path, p)


def authenticate_api_key():
    """X-API-Key 헤더 검증.

    Returns:
        (True, None): 인증 성공, g.api_key_id 설정됨.
        (False, (code, message, status)): 인증 실패.
        (None, None): 헤더 없음 (호출자가 다른 인증 수단 시도).
    """
    raw = request.headers.get("X-API-Key")
    if raw is None:
        return None, None
    key_value = raw.strip()
    if not key_value:
        return False, ("INVALID_KEY", "API 키가 비어 있습니다.", 401)

    db = SessionLocal()
    try:
        key = db.query(ApiKey).filter_by(key_value=key_value).first()
        if not key:
            return False, ("INVALID_KEY", "유효하지 않은 API 키입니다.", 401)
        if not key.is_active:
            return False, ("KEY_DISABLED", "비활성화된 API 키입니다.", 403)
        if key.expires_at and key.expires_at < datetime.utcnow():
            return False, ("KEY_EXPIRED", "만료된 API 키입니다.", 403)
        if not _match_allowed_paths(key.allowed_paths, request.path):
            return False, ("FORBIDDEN_PATH",
                           "이 API 키로 접근할 수 없는 경로입니다.", 403)

        g.api_key_id = key.id

        try:
            key.last_used_at = datetime.utcnow()
            key.request_count = (key.request_count or 0) + 1
            db.commit()
        except Exception:
            db.rollback()
            logger.debug("API 키 사용 통계 업데이트 실패", exc_info=True)

        return True, None
    except Exception:
        logger.exception("API 키 검증 중 오류")
        return False, ("AUTH_ERROR", "API 키 검증 중 오류가 발생했습니다.", 500)
    finally:
        db.close()
