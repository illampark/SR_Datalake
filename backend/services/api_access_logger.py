"""API 접근 로깅 미들웨어 — Flask before/after_request 훅"""

import logging
import time
from datetime import datetime, timedelta

from flask import g, request
from backend.database import SessionLocal
from backend.models.gateway import ApiAccessLog

logger = logging.getLogger(__name__)

# 로깅 제외 경로 (자기 자신 호출 무한루프 방지)
_EXCLUDE_PATHS = frozenset({
    "/api/gateway/access-log",
    "/api/gateway/stats",
})

_EXCLUDE_PREFIXES = ("/static/",)


def setup_access_logging(app):
    """Flask 앱에 API 접근 로그 미들웨어를 등록한다."""

    @app.before_request
    def _mark_start():
        g._req_start = time.time()

    @app.after_request
    def _log_access(response):
        # /api/* 경로만 기록
        if not request.path.startswith("/api/"):
            return response

        if request.path in _EXCLUDE_PATHS:
            return response
        for pfx in _EXCLUDE_PREFIXES:
            if request.path.startswith(pfx):
                return response

        try:
            elapsed = (time.time() - getattr(g, "_req_start", time.time())) * 1000
            db = SessionLocal()
            try:
                entry = ApiAccessLog(
                    timestamp=datetime.utcnow(),
                    method=request.method,
                    path=request.path,
                    status_code=response.status_code,
                    response_time_ms=round(elapsed, 2),
                    remote_addr=request.remote_addr or "",
                    user_agent=(request.user_agent.string or "")[:500],
                    request_size=request.content_length or 0,
                    response_size=response.content_length or 0,
                    api_key_id=getattr(g, "api_key_id", None),
                )
                db.add(entry)
                db.commit()
            except Exception:
                db.rollback()
                logger.debug("API 접근 로그 기록 실패", exc_info=True)
            finally:
                db.close()
        except Exception:
            pass  # 로깅 실패로 요청 처리를 중단하지 않음

        return response


def cleanup_old_logs(retention_days=7):
    """오래된 접근 로그를 삭제한다."""
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    db = SessionLocal()
    try:
        deleted = db.query(ApiAccessLog).filter(
            ApiAccessLog.timestamp < cutoff,
        ).delete(synchronize_session=False)
        db.commit()
        if deleted:
            logger.info("오래된 API 접근 로그 %d건 삭제 (보관 %d일)", deleted, retention_days)
        return deleted
    except Exception:
        db.rollback()
        logger.error("접근 로그 정리 실패", exc_info=True)
        return 0
    finally:
        db.close()
