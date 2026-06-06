"""Phase 8 보안 — import collector 의 local_path 화이트리스트 가드.

Phase 1 (T-1): 새 입력만 검증. 기존 collector 의 path 가 정책 위반이어도
PUT 시 값 변경이 없으면 통과 (운영 영향 최소화).

정책:
- tenant_id 별 허용 base prefix 집합 안에 있어야 함 (legacy 호환 포함)
- super_admin 도 working tenant 의 base 안에서만 허용 — cross-tenant base 는
  impersonate 흐름으로 진입 후에야 가능

backend/config.py 의 LOCAL_PATH_BASES_BY_TENANT / LOCAL_PATH_DEFAULT_TEMPLATE 로
운영 환경별 조정.
"""

import os
import re
from typing import Optional, Tuple

from backend.services.tenant_filter import _current_tenant_id


# 다른 tenant 의 template 경로 침범 차단용 패턴
# `/t-{N}/` 가 path 안에 있고 그 N 이 자기 tenant id 가 아니면 cross-tenant
_TENANT_TEMPLATE_RE = re.compile(r"/t-(\d+)/")


def allowed_bases_for(tid: int) -> list[str]:
    """tid 의 허용 base prefix 리스트. 운영 base 가 추가되면 config 만 수정."""
    from backend.config import (
        LOCAL_PATH_BASES_BY_TENANT,
        LOCAL_PATH_DEFAULT_TEMPLATE,
    )
    if tid in LOCAL_PATH_BASES_BY_TENANT:
        return list(LOCAL_PATH_BASES_BY_TENANT[tid])
    return [LOCAL_PATH_DEFAULT_TEMPLATE.format(tid=tid)]


def _path_under_base(path: str, base: str) -> bool:
    """path 가 base 안에 머무는지 (realpath 비교, traversal 방지)."""
    base_real = os.path.realpath(base).rstrip(os.sep)
    target = os.path.realpath(path)
    return target == base_real or target.startswith(base_real + os.sep)


def assert_local_path_allowed(local_path: str) -> Optional[Tuple[str, str, int]]:
    """입력 path 가 현 tenant 의 허용 base 안에 있는지 검증.

    위반 시 (message, code, status) 튜플 반환. 통과 시 None.
    빈 문자열은 통과 (upload 모드 등 path 미사용).

    super_admin 도 working tenant 의 base 안에서만 허용. cross-tenant base 는
    impersonate 흐름으로 진입 후 호출 권장.
    """
    if not local_path:
        return None
    tid = _current_tenant_id()
    bases = allowed_bases_for(tid)
    in_base = any(_path_under_base(local_path, b) for b in bases)
    if not in_base:
        return (
            f"허용된 base 밖의 경로입니다. 허용 base 예: {bases[0]}",
            "INVALID_PATH",
            400,
        )
    # 다른 tenant 의 template 영역 침범 차단
    # 예: T1 base 의 `/app/static/uploads/import/` 안에 `/t-2/abc` 가 들어오면 거부
    m = _TENANT_TEMPLATE_RE.search(os.path.realpath(local_path))
    if m and int(m.group(1)) != tid:
        return (
            f"다른 tenant 의 영역입니다 (/t-{m.group(1)}/). "
            "impersonate 로 진입 후 호출하세요.",
            "INVALID_PATH",
            400,
        )
    return None


def assert_path_change_allowed(new_path: str, current_path: str) -> Optional[Tuple[str, str, int]]:
    """PUT/scan-path 의 변경 시 검증. new_path 와 current_path 가 같으면 통과
    (값 미변경은 운영 호환 — 기존 정책 위반 path 도 그대로 유지 가능)."""
    if (new_path or "") == (current_path or ""):
        return None
    return assert_local_path_allowed(new_path)


def validate_archive_subdir(subdir: str) -> Optional[Tuple[str, str, int]]:
    """archive_subdir 검증. traversal·절대경로·구분자·빈 문자열 거부."""
    if not subdir:
        return ("archive_subdir 는 비어 있을 수 없습니다.", "INVALID_PATH", 400)
    if len(subdir) > 100:
        return ("archive_subdir 길이는 100자 이하여야 합니다.", "INVALID_PATH", 400)
    if any(ch in subdir for ch in ("/", "\\", "..")):
        return (
            "archive_subdir 는 / · \\ · .. 를 포함할 수 없습니다.",
            "INVALID_PATH",
            400,
        )
    return None
