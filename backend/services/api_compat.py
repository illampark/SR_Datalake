"""API request body 의 camelCase → snake_case 호환 헬퍼.

표준 컨벤션: JSON request/response 는 camelCase.
일부 레거시 라우트는 내부적으로 body.get("snake_key") 로 꺼냄.
점진 마이그레이션을 위해 본 헬퍼로 body 를 정규화하면 기존 코드 변경 없이
camelCase / snake_case 둘 다 수용된다.

사용 예:
    from backend.services.api_compat import normalize_camel_to_snake

    body = normalize_camel_to_snake(request.get_json(force=True))
    # 이후 기존 코드 (body.get("tenant_id") 등) 그대로 동작.
"""

import re

_CAMEL_BOUNDARY = re.compile(r"(?<!^)(?=[A-Z])")


def _to_snake(s: str) -> str:
    """단순 camelCase → snake_case. 연속 대문자(XML 등)는 대상 아님 — API 키는
    'tenantId', 'pipelineId' 같은 일반 camel 만 다루므로 단순 패턴으로 충분."""
    return _CAMEL_BOUNDARY.sub("_", s).lower()


def normalize_camel_to_snake(body):
    """body 의 camelCase 키 각각에 snake_case 별칭을 추가 (원본 키 보존).

    - 입력이 dict 가 아니면 그대로 반환 (None / list 등).
    - snake_case 가 이미 body 에 존재하면 덮어쓰지 않음 (snake 우선).
    - 1 단계 평탄 변환만 수행 (중첩 dict 는 손대지 않음).
    """
    if not isinstance(body, dict):
        return body
    out = dict(body)
    for k, v in body.items():
        if not isinstance(k, str):
            continue
        sk = _to_snake(k)
        if sk != k and sk not in body:
            out[sk] = v
    return out
