"""RBAC (Role-Based Access Control) — 2-role 정책 + 4-role 확장 준비 (Phase 1)

현재 정책 (Phase 1 시점, MULTITENANT_MODE=off):
- admin(전체 권한) / viewer(조회·다운로드 전용)
- 레거시 enum 값(engineer/operator)은 admin으로 해석 (기존 데이터 보호 + 호환성)

원칙:
- GET / HEAD / OPTIONS: 인증된 모든 사용자 허용 (단, ADMIN_ONLY_GET_PATHS 예외)
- POST / PUT / PATCH / DELETE: admin 만 (단, RBAC_ALLOWED_FOR_ALL_PATHS 예외)
- API 키 인증: admin 동등으로 간주(현재 키별 권한 미구현)

Phase 1 신규 (claudedocs/rbac-target-v1.md):
- 4-role 등급(TENANT_ROLE_RANK) 정의를 준비 — 현재는 미사용 (Phase 2 이후 활용)
- 레거시 → 4-role 매핑(map_legacy_role_to_tenant_role)
- 이 두 가지는 단일테넌트 동작에 영향이 없도록 add-only 변경
"""
from functools import wraps
from flask import session, jsonify, g, request, redirect

# ────────────────────────────────────────────────────────────────────────
# Phase 0 (현 운영) — 2-role 정책. 변경 금지.
# ────────────────────────────────────────────────────────────────────────

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
# 주의: API GET 엔드포인트는 메인 대시보드/카탈로그 등에서 폭넓게 재사용되므로
# 시스템 모니터링·스토리지의 경우 HTML 페이지만 차단하고 API GET 은 그대로 둔다.
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
    # 시스템 모니터링·스토리지 페이지 자체는 admin 전용
    # (단, 메인 대시보드·카탈로그 등이 의존하는 /api/monitoring/*·/api/storage/* GET 은 viewer 도 사용)
    "/monitoring",
    "/storage",
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
    """현재 요청의 정규화된 role 반환.

    API 키 인증은 viewer(읽기 전용) 로 고정한다.
    외부에서 임베드되는 API 키 특성상 노출 위험이 높아 최소 권한 원칙 적용.
    필요 시 키별 role 컬럼을 추가해 admin 키를 별도 발급하는 방식으로 확장 가능.
    """
    if getattr(g, "api_key_authenticated", False):
        return "viewer"
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


# ────────────────────────────────────────────────────────────────────────
# Phase 1 (add-only) — 4-role 등급 정의. 아직 어디서도 호출되지 않는다.
# 활용은 Phase 2 (세션 컨텍스트) 이후. 정의만 두는 이유: 모델·마이그레이션에서
# 동일 문자열을 참조해야 하므로 단일 진실의 원천이 필요.
#
# 참조: claudedocs/rbac-target-v1.md § 2 - 권한 매트릭스
# ────────────────────────────────────────────────────────────────────────

TENANT_ROLE_RANK = {
    "tenant_viewer":  0,
    "tenant_editor":  1,
    "tenant_admin":   2,
    "super_admin":    3,
}


def map_legacy_role_to_tenant_role(raw) -> str:
    """레거시 user.role → 4-role 매핑 (Phase 1 마이그레이션·시드 시 사용).

    매핑 규칙 (rbac-target-v1.md § 1 R5):
      admin    → tenant_admin
      viewer   → tenant_viewer
      engineer → tenant_editor   ← 원래 의도 살림
      operator → tenant_editor   ← 원래 의도 살림
      그 외/없음 → tenant_viewer (안전 기본값)

    super_admin 은 user.is_super 컬럼으로 별도 표현하므로 여기서 매핑하지 않는다.
    """
    if not raw:
        return "tenant_viewer"
    v = str(raw).strip().lower()
    if v == "admin":
        return "tenant_admin"
    if v in ("engineer", "operator"):
        return "tenant_editor"
    if v == "viewer":
        return "tenant_viewer"
    return "tenant_viewer"


# ────────────────────────────────────────────────────────────────────────
# Phase 2 (add-only) — tenant 컨텍스트 조회 헬퍼.
#
# 모두 read-only 함수. 라우트·서비스 코드가 g / session 에서 안전하게 값을 얻기
# 위한 단일 접근 지점. before_request 의 inject_tenant_context (app.py) 가
# g 에 값을 주입한 뒤 이 함수들로 접근.
#
# MULTITENANT_MODE=off 일 때도 안전한 기본값(tenant=1) 반환 → 단일테넌트
# 동작 호환.
# ────────────────────────────────────────────────────────────────────────

def current_tenant_id():
    """현재 요청의 tenant_id.

    우선순위:
      1) g.tenant_id (before_request 가 세션 또는 API 키에서 채움)
      2) 세션의 tenant_id
      3) 기본값 1 (legacy default tenant)
    """
    tid = getattr(g, "tenant_id", None)
    if tid is not None:
        return tid
    sid = session.get("tenant_id") if session else None
    if sid is not None:
        return sid
    return 1


def current_tenant_role():
    """현재 요청의 4-role 값 (tenant_admin / tenant_editor / tenant_viewer).

    super_admin 여부는 is_super() 로 별도 확인. 본 함수는 tenant 컨텍스트 안에서의
    역할만 다룬다.
    """
    if getattr(g, "api_key_authenticated", False):
        # Phase 6 에서 키 record 의 role 을 g.tenant_role 로 세팅. 그 전엔 viewer.
        return getattr(g, "tenant_role", "tenant_viewer")
    return session.get("tenant_role", "tenant_viewer")


def is_super():
    """super_admin 여부 (cross-tenant 권한자).

    API 키 인증은 super 가 될 수 없음 (rbac-target-v1.md D10/R4).
    """
    if getattr(g, "api_key_authenticated", False):
        return False
    return bool(session.get("is_super", False))


def is_impersonating():
    """super_admin 이 특정 tenant 로 impersonate 중인지 (Phase 7 에서 실제 사용)."""
    return bool(session.get("impersonate"))


def tenant_role_at_least(min_role):
    """4-role 등급 비교. super_admin 은 항상 통과."""
    if is_super():
        return True
    if min_role not in TENANT_ROLE_RANK:
        raise ValueError(f"unknown tenant role: {min_role}")
    cur = current_tenant_role()
    return TENANT_ROLE_RANK.get(cur, -1) >= TENANT_ROLE_RANK[min_role]



# ────────────────────────────────────────────────────────────────────────
# Phase 7 - 경로 4분류 (rbac-target-v1.md § 6)
# ────────────────────────────────────────────────────────────────────────

# SYSTEM_ONLY: super_admin 만
_SYSTEM_ONLY_PREFIXES = (
    "/api/sys/",
)

# TENANT_ADMIN_ONLY: tenant_admin 이상 (super_admin 도 통과)
# 단, /api/tenant/me GET 은 모든 멤버 접근 가능 — 함수별로 _require_tenant_admin 처리
_TENANT_ADMIN_ONLY_PREFIXES = (
)


def classify_path(path: str) -> str:
    """경로를 PUBLIC / TENANT_SCOPED / TENANT_ADMIN_ONLY / SYSTEM_ONLY 로 분류."""
    for p in _SYSTEM_ONLY_PREFIXES:
        if path.startswith(p):
            return "SYSTEM_ONLY"
    for p in _TENANT_ADMIN_ONLY_PREFIXES:
        if path.startswith(p):
            return "TENANT_ADMIN_ONLY"
    # /api/admin/auth/* 는 기존 PUBLIC 화이트리스트 분류로
    return "TENANT_SCOPED"


def enforce_class_gate():
    """before_request 단계 — 경로 클래스별 강제 가드.

    enforce_request_rbac 보다 상위에서 호출. SYSTEM_ONLY 는 super_admin 만,
    TENANT_ADMIN_ONLY 는 tenant_admin 이상만 통과.

    인증 면제 경로(login/static)는 호출 전에 require_login 에서 이미 차단됨.
    """
    from flask import request as _req
    cls = classify_path(_req.path)
    if cls == "SYSTEM_ONLY":
        if not is_super():
            return _forbidden()
    elif cls == "TENANT_ADMIN_ONLY":
        # super 도 통과
        if not (is_super() or tenant_role_at_least("tenant_admin")):
            return _forbidden()
    return None
