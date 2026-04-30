"""RBAC (Role-Based Access Control) — 단순 2-role 정책

현재 정책: admin(전체 권한) / viewer(조회·다운로드 전용).
레거시 enum 값(engineer/operator)은 admin으로 해석한다(기존 데이터 보호 + 호환성).

원칙:
- GET / HEAD / OPTIONS: 인증된 모든 사용자 허용 (단, ADMIN_ONLY_GET_PATHS 예외)
- POST / PUT / PATCH / DELETE: admin 만 (단, RBAC_ALLOWED_FOR_ALL_PATHS 예외)
- API 키 인증: admin 동등으로 간주(현재 키별 권한 미구현)

향후 3-role 이상으로 확장 시: ROLE_RANK 에 새 값 추가 + ADMIN_ONLY_GET_PATHS / 매핑 보강.
"""
from functools import wraps
from flask import session, jsonify, g, request, redirect

# 권한 등급(높을수록 강함)
ROLE_RANK = {
    "viewer": 0,
    "admin":  1,
}

# 변경 메서드여도 인증된 모든 사용자에게 허용할 경로(prefix 비교).
# 로그인/로그아웃/언어 등 본인 세션 관련.
RBAC_ALLOWED_FOR_ALL_PATHS = (
    "/api/admin/auth/login",
    "/api/admin/auth/logout",
    "/api/admin/auth/me",
    "/api/admin/lang",
)

# 조회(GET)여도 admin 만 허용할 경로(prefix 비교).
# 감사 로그·사용자 관리 등 보안/관리 정보 + 관련 HTML 페이지.
ADMIN_ONLY_GET_PATHS = (
    "/api/monitoring/logs/audit",
    "/monitoring/logs/audit",
    "/api/admin/login-history",
    "/api/admin/users",
    "/api/admin/login-policy",
    # HTML 관리 페이지 (사이드바 노출 외에 직접 URL 접근도 차단)
    "/admin/users",
    "/admin/infra",
    "/admin/gateway",
    "/admin/settings",
)


def normalize_role(raw):
    """레거시 값(engineer/operator)을 현 정책에 맞춰 admin 으로 흡수."""
    if not raw:
        return "viewer"
    raw = str(raw).strip().lower()
    if raw == "viewer":
        return "viewer"
    return "admin"


def current_role():
    """현재 요청의 정규화된 role 반환. API 키 인증은 admin 으로 간주."""
    if getattr(g, "api_key_authenticated", False):
        return "admin"
    return normalize_role(session.get("role"))


def is_admin():
    return current_role() == "admin"


def _forbidden():
    """API 호출엔 JSON 403, 페이지 네비게이션엔 홈으로 리다이렉트."""
    if request.path.startswith("/api/"):
        return jsonify({
            "success": False,
            "data": None,
            "error": {"code": "FORBIDDEN", "message": "권한이 부족합니다."},
        }), 403
    return redirect("/")


def require_role(min_role):
    """라우트 데코레이터 — 명시적 권한 강제.

    미들웨어가 일괄 적용되긴 하지만, 화이트리스트 경로 안에서 일부 액션을
    admin 한정으로 좁히고 싶을 때 추가 가드로 사용.
    """
    if min_role not in ROLE_RANK:
        raise ValueError(f"unknown role: {min_role}")
    needed = ROLE_RANK[min_role]

    def deco(fn):
        @wraps(fn)
        def w(*a, **kw):
            r = current_role()
            if ROLE_RANK.get(r, 0) < needed:
                return _forbidden()
            return fn(*a, **kw)
        return w
    return deco


def enforce_request_rbac():
    """app.before_request 단계에서 호출. 인증은 이미 통과한 상태라 가정.

    반환값:
      - None  : 통과
      - tuple : Flask 응답(403)
    """
    method = request.method
    path = request.path

    # 인증 면제 경로(static/login)는 require_login 단계에서 이미 None 반환.
    # 여기 도달했다는 것은 인증 통과 = role 결정 가능.

    role = current_role()

    # 1) GET/HEAD/OPTIONS: 기본 viewer 이상 허용. 단, admin-only GET 경로는 admin.
    if method in ("GET", "HEAD", "OPTIONS"):
        for prefix in ADMIN_ONLY_GET_PATHS:
            if path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "?"):
                if role != "admin":
                    return _forbidden()
                break
        return None

    # 2) 변경 메서드 (POST/PUT/PATCH/DELETE 등):
    #    화이트리스트(self-service) → 모든 로그인 사용자 허용
    for prefix in RBAC_ALLOWED_FOR_ALL_PATHS:
        if path == prefix or path.startswith(prefix + "/"):
            return None

    #    그 외 변경은 admin 만
    if role != "admin":
        return _forbidden()
    return None
