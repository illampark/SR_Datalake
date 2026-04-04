"""감사 로그 기록 헬퍼 — 각 라우트에서 호출"""

import logging

logger = logging.getLogger(__name__)


def log_audit(action_type, action, target_type="", target_name="",
              result="success", detail=None, username=None):
    """감사 이벤트를 DB에 기록한다. 실패해도 예외를 전파하지 않는다."""
    try:
        from flask import request, session
        from backend.database import SessionLocal
        from backend.models.audit import AuditLog

        db = SessionLocal()
        try:
            db.add(AuditLog(
                username=username or session.get("username", "system"),
                action_type=action_type,
                action=action,
                target_type=target_type,
                target_name=str(target_name),
                ip_address=getattr(request, "remote_addr", "") or "",
                user_agent=(request.user_agent.string or "")[:500]
                if hasattr(request, "user_agent") else "",
                result=result,
                detail=detail or {},
            ))
            db.commit()
        except Exception:
            db.rollback()
            logger.debug("감사 로그 기록 실패", exc_info=True)
        finally:
            db.close()
    except Exception:
        pass  # Flask context 밖에서 호출되어도 안전
