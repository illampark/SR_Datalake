"""시스템 전반에서 공유하는 admin_setting 헬퍼.

지금은 페이지 크기(default page size)만 노출하지만, 향후 다른 전역 설정도
같은 캐시 패턴으로 추가할 수 있다.
"""

import logging
import time

from backend.database import SessionLocal
from backend.models.user import AdminSetting

logger = logging.getLogger(__name__)

_DEFAULT_PAGE_SIZE = 20
_PAGE_SIZE_MIN = 5
_PAGE_SIZE_MAX = 200
_PAGE_SIZE_KEY = "system.page_size"
_TTL_SECONDS = 60.0

# 모듈 수준 캐시 — 멀티 워커 환경에서 워커별로 1번씩 DB 조회.
# TTL 1분이면 변경 후 최대 1분 내 반영. 즉시 반영이 필요한 경로(저장 직후) 는
# invalidate_page_size_cache() 를 호출해서 즉시 무효화한다.
_cache = {"value": None, "ts": 0.0}


def get_default_page_size():
    """admin_setting 'system.page_size' 값을 정규화해 반환. 60초 캐시."""
    now = time.time()
    if _cache["value"] is not None and (now - _cache["ts"]) < _TTL_SECONDS:
        return _cache["value"]

    db = SessionLocal()
    try:
        row = db.query(AdminSetting).filter_by(key=_PAGE_SIZE_KEY).first()
        try:
            v = int(row.value) if row else _DEFAULT_PAGE_SIZE
        except (ValueError, TypeError):
            v = _DEFAULT_PAGE_SIZE
        v = max(_PAGE_SIZE_MIN, min(_PAGE_SIZE_MAX, v))
    except Exception:
        logger.exception("page_size 조회 실패 — 기본값 사용")
        v = _DEFAULT_PAGE_SIZE
    finally:
        db.close()

    _cache["value"] = v
    _cache["ts"] = now
    return v


def invalidate_page_size_cache():
    """설정 저장 직후 호출해 즉시 새 값을 반영시킨다."""
    _cache["value"] = None
    _cache["ts"] = 0.0


def clamp_page_size(value):
    """사용자 입력값 정규화. 잘못된 값이면 기본값 반환."""
    try:
        v = int(value)
    except (ValueError, TypeError):
        return _DEFAULT_PAGE_SIZE
    return max(_PAGE_SIZE_MIN, min(_PAGE_SIZE_MAX, v))


def page_size_bounds():
    return _PAGE_SIZE_MIN, _PAGE_SIZE_MAX
