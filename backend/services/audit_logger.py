"""감사 로그 기록 헬퍼 — 각 라우트에서 호출.

두 가지 사용 방식:
1. `log_audit(...)` 인라인 호출 — 변경 필드 추적 등 풍부한 detail이 필요할 때
2. `@audit_route(...)` 데코레이터 — 단순 CRUD/start/stop. 응답 상태로 success/failure 자동 판정
"""

import functools
import logging

logger = logging.getLogger(__name__)


# detail/JSON 본문에서 마스킹할 민감 키 (대소문자 무시, snake/camel 모두)
_SENSITIVE_KEYS = {
    "password", "password_hash", "passwd", "pwd",
    "secret", "secretkey", "secret_key",
    "apikey", "api_key", "access_key", "accesskey",
    "privatekey", "private_key",
    "token", "auth_token", "bearer", "session",
    "passphrase", "credential", "credentials",
}


def _sanitize(value, max_depth=4):
    """dict/list 안의 민감 키를 '***' 로 마스킹한 사본 반환."""
    if max_depth <= 0:
        return value
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if isinstance(k, str) and k.lower().replace("-", "_") in _SENSITIVE_KEYS:
                out[k] = "***"
            else:
                out[k] = _sanitize(v, max_depth - 1)
        return out
    if isinstance(value, list):
        return [_sanitize(x, max_depth - 1) for x in value[:50]]
    return value


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
                detail=_sanitize(detail or {}),
            ))
            db.commit()
        except Exception:
            db.rollback()
            logger.debug("감사 로그 기록 실패", exc_info=True)
        finally:
            db.close()
    except Exception:
        pass  # Flask context 밖에서 호출되어도 안전


def audit_route(action_type, action, target_type="",
                target_name_kwarg=None,
                target_name_from_response="name",
                detail_keys=None,
                detail_extra=None):
    """라우트 데코레이터 — 핸들러 실행 후 자동으로 log_audit 호출.

    인자:
      action_type: audit_log.action_type 값. (예: "connector", "pipeline")
      action: audit_log.action 값. (예: "connector.mqtt.delete")
      target_type: audit_log.target_type 값.
      target_name_kwarg: URL kwarg 이름. 있으면 그 값으로 target_name 설정.
      target_name_from_response: response.data 에서 가져올 키 (기본 "name").
        URL kwarg 가 없거나 None 일 때 사용. 응답 JSON 의 data.{이 키} 또는
        data.id 를 시도.
      detail_keys: request body(JSON) 에서 detail 로 포함할 키 화이트리스트.
        (민감 키는 자동 마스킹).
      detail_extra: dict 또는 (request, response) -> dict 콜백. 추가 detail.

    결과 판정:
      Response 의 status_code 가 2xx 면 success, 아니면 failure.
      response body 가 {"success": false} 면 failure 로 덮어쓰기.

    예시:
      @bp.route("/<int:cid>", methods=["DELETE"])
      @audit_route("connector", "connector.mqtt.delete",
                   target_type="mqtt_connector", target_name_kwarg="cid")
      def delete_mqtt_connector(cid):
          ...
    """
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            from flask import request

            response = fn(*args, **kwargs)

            try:
                _audit_after(response, request, kwargs,
                             action_type, action, target_type,
                             target_name_kwarg, target_name_from_response,
                             detail_keys, detail_extra)
            except Exception:
                logger.debug("audit_route 후처리 실패", exc_info=True)

            return response
        return wrapper
    return deco


def _audit_after(response, request, kwargs,
                 action_type, action, target_type,
                 target_name_kwarg, target_name_from_response,
                 detail_keys, detail_extra):
    """audit_route 데코레이터 내부 로직 — 응답을 분석해 log_audit 호출."""

    # 1) Response 객체 분리
    if isinstance(response, tuple):
        resp_obj = response[0]
        status = response[1] if len(response) > 1 and isinstance(response[1], int) else 200
    else:
        resp_obj = response
        status = getattr(response, "status_code", 200)

    # 2) result 판정 (HTTP status + body success 플래그)
    result = "success" if 200 <= int(status) < 300 else "failure"
    body_json = None
    try:
        if hasattr(resp_obj, "get_json"):
            body_json = resp_obj.get_json(silent=True)
            if isinstance(body_json, dict) and body_json.get("success") is False:
                result = "failure"
    except Exception:
        body_json = None

    # 3) target_name 결정
    target_name = ""
    if target_name_kwarg and target_name_kwarg in kwargs:
        target_name = str(kwargs[target_name_kwarg])
    elif body_json and isinstance(body_json.get("data"), dict):
        d = body_json["data"]
        target_name = str(
            d.get(target_name_from_response)
            or d.get("name")
            or d.get("id")
            or ""
        )

    # 4) detail 조립
    detail = {}
    if detail_keys:
        try:
            body = request.get_json(silent=True) or {}
            for k in detail_keys:
                if k in body:
                    detail[k] = body[k]
        except Exception:
            pass

    if callable(detail_extra):
        try:
            extra = detail_extra(request, resp_obj)
            if isinstance(extra, dict):
                detail.update(extra)
        except Exception:
            pass
    elif isinstance(detail_extra, dict):
        detail.update(detail_extra)

    # 5) 실패 시 에러 메시지 detail 에 포함
    if result == "failure" and isinstance(body_json, dict):
        err = body_json.get("error")
        if isinstance(err, dict):
            detail.setdefault("error", {
                "code": err.get("code"),
                "message": err.get("message"),
            })

    log_audit(action_type=action_type, action=action,
              target_type=target_type, target_name=target_name,
              result=result, detail=detail)
