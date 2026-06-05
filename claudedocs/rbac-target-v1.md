# RBAC 목표 모델 v1

**Phase 0 산출물 / 작성일: 2026-06-05 / 부모 문서: `multitenant-design-v1.md` D7~D10**

이 문서는 4-role 모델로의 전환에 필요한 모든 데이터·인터페이스·정책을 정의한다. Phase 1~7 RBAC 작업의 단일 진실이다.

---

## 1. 잠긴 결정

| # | 항목 | 결정 | 비고 |
|---|---|---|---|
| R1 | 역할 수 | **4-role** | super_admin / tenant_admin / tenant_editor / tenant_viewer |
| R2 | `tenant_editor` 도입 | **Yes** | 산업 운영자 페르소나 (파이프라인 작성 가능, 사용자/정책 불가) |
| R3 | 정책 표현 | **코드 하드코딩** (경로 분류 기반) | DB-정책 테이블은 Phase 8에서 재검토 |
| R4 | API 키 스코프 | **키별 1 tenant 고정** + 키별 role | |
| R5 | 레거시 매핑 | `admin` → `tenant_admin`, `viewer` → `tenant_viewer`, `engineer`/`operator` → `tenant_editor` | 원래 의도 살림 |

## 2. 역할 매트릭스

### 2.1 정의

| 역할 | 스코프 | 핵심 능력 | 핵심 제약 |
|---|---|---|---|
| `super_admin` | 전 시스템 (cross-tenant) | tenant CRUD, impersonate, 시스템 설정, 모든 데이터 조회 | 일상 운영은 impersonate로 수행 (감사 트레일 강화) |
| `tenant_admin` | 자기 tenant | 사용자 초대·역할 변경, tenant 설정, 모든 데이터 변경 | 다른 tenant 불가 |
| `tenant_editor` | 자기 tenant | 파이프라인·컬렉터·카탈로그 작성/수정/실행, 데이터 변경 | 사용자 관리·tenant 설정·통합 키 발급 불가 |
| `tenant_viewer` | 자기 tenant | 조회·다운로드 + 일부 self-service (로그인/언어) | 모든 변경 불가 |

### 2.2 능력 매트릭스 (요약)

`R` = read, `W` = write, `−` = 불가

| 영역 | super_admin | tenant_admin | tenant_editor | tenant_viewer |
|---|---|---|---|---|
| **시스템 관리** (`/api/sys/*`) | RW | − | − | − |
| Tenant CRUD | RW | − | − | − |
| 시스템 감사 로그 | R | − | − | − |
| Impersonate | RW | − | − | − |
| **테넌트 관리** (`/api/tenant/me/*`) | RW (impersonate 중) | RW | R (자기 프로필만) | R (자기 프로필만) |
| 멤버 초대·역할 변경 | RW | RW | − | − |
| Tenant 설정·브랜딩 | RW | RW | − | − |
| API 키 발급·폐기 | RW | RW | − | − |
| 사용량·청구 | R | R | − | − |
| **도메인 데이터** (`/api/pipeline`, `/api/import`, `/api/catalog`, `/api/storage`, `/api/collector_*`) | RW (impersonate 중) | RW | RW | R |
| **모니터링** (`/api/monitoring/*` 일반) | R | R | R | R |
| 감사 로그 (`/api/monitoring/logs/audit`) | R | R | − | − |
| **본인 self-service** (login/logout/me/lang) | OK | OK | OK | OK |

### 2.3 역할 등급 (코드용)

```python
ROLE_RANK = {
    "tenant_viewer":  0,
    "tenant_editor":  1,
    "tenant_admin":   2,
    "super_admin":    3,
}
```

## 3. 데이터 모델 변경

### 3.1 신규 테이블

```sql
-- 테넌트 entity
CREATE TABLE tenant (
    id          BIGSERIAL PRIMARY KEY,
    slug        VARCHAR(50) NOT NULL UNIQUE,
    name        VARCHAR(200) NOT NULL,
    status      VARCHAR(20) NOT NULL DEFAULT 'active',  -- active/suspended/archived
    plan        VARCHAR(20) NOT NULL DEFAULT 'default',
    settings    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_tenant_slug ON tenant(slug);
CREATE INDEX ix_tenant_status ON tenant(status);

-- 사용자 ↔ 테넌트 멤버십 + 역할
CREATE TABLE tenant_membership (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    tenant_id   BIGINT  NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    role        VARCHAR(30) NOT NULL,    -- tenant_admin/tenant_editor/tenant_viewer
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, tenant_id),
    CHECK (role IN ('tenant_admin','tenant_editor','tenant_viewer'))
);
CREATE INDEX ix_membership_user ON tenant_membership(user_id);
CREATE INDEX ix_membership_tenant ON tenant_membership(tenant_id);
```

### 3.2 기존 테이블 변경

```sql
-- super_admin 표식 (cross-tenant 권한자)
ALTER TABLE "user" ADD COLUMN is_super BOOLEAN NOT NULL DEFAULT FALSE;

-- 기존 user.role은 한시적 유지 (Phase 4까지 호환), Phase 5에 deprecate, Phase 8에 제거
-- Phase 1 백필 후 멤버십이 진실, role 컬럼은 캐시처럼 동작
```

### 3.3 초기 데이터 마이그레이션 (Phase 1)

```sql
-- 시스템 테넌트 (super_admin 소속용, 도메인 데이터 없음)
INSERT INTO tenant(id, slug, name, status) VALUES (0, 'system', 'System', 'active');

-- 현 운영 고객
INSERT INTO tenant(id, slug, name, status) VALUES (1, 'default', '기본 테넌트', 'active');

-- 시퀀스 보정 (다음 신규 tenant는 id=2부터)
SELECT setval('tenant_id_seq', 2);

-- 기존 사용자 전부 → tenant 1 멤버 + 역할 매핑
INSERT INTO tenant_membership(user_id, tenant_id, role)
SELECT id, 1,
       CASE LOWER(role)
         WHEN 'viewer'   THEN 'tenant_viewer'
         WHEN 'engineer' THEN 'tenant_editor'
         WHEN 'operator' THEN 'tenant_editor'
         ELSE 'tenant_admin'
       END
FROM "user";

-- super_admin 지정은 운영자가 별도 명령으로 (자동 매핑 금지)
-- UPDATE "user" SET is_super = TRUE WHERE username = 'admin' AND ...;  -- 수동 실행
```

## 4. API 키 모델 v2 (Phase 6)

### 4.1 스키마

```sql
-- 신규 api_key 테이블 (현재 미흡한 부분 확장)
CREATE TABLE api_key (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    name        VARCHAR(100) NOT NULL,
    key_hash    VARCHAR(128) NOT NULL UNIQUE,        -- bcrypt/argon2
    key_prefix  VARCHAR(16)  NOT NULL,               -- 'sdl_pk_XXXX' 식별자 표시용
    role        VARCHAR(30)  NOT NULL DEFAULT 'tenant_viewer',
    scopes      JSONB        NOT NULL DEFAULT '[]'::jsonb,  -- e.g., ["read:pipeline","write:catalog"]
    created_by  INTEGER REFERENCES "user"(id),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMP,
    expires_at  TIMESTAMP,
    revoked_at  TIMESTAMP,
    CHECK (role IN ('tenant_viewer','tenant_editor','tenant_admin'))
);
CREATE INDEX ix_apikey_tenant ON api_key(tenant_id);
CREATE INDEX ix_apikey_hash ON api_key(key_hash);
```

### 4.2 인증 흐름

```python
# backend/services/api_auth.py (확장)
def authenticate_api_key():
    raw = request.headers.get("X-API-Key")
    if not raw: return False, "missing"
    record = lookup_by_hash(hash(raw))
    if not record or record.revoked_at or (record.expires_at and now > record.expires_at):
        return False, "invalid_or_expired"
    g.api_key_authenticated = True
    g.tenant_id = record.tenant_id     # 필수
    g.role      = record.role
    g.scopes    = set(record.scopes)
    update_last_used_at(record.id)
    return True, None
```

### 4.3 스코프 (선택적 — 미세 권한)

`scopes`가 비어 있으면 `role`이 부여하는 전체 권한. 채워져 있으면 그 집합으로 제한.

| Scope | 의미 |
|---|---|
| `read:pipeline` / `write:pipeline` | 파이프라인 조회/변경 |
| `read:catalog` / `write:catalog` | 카탈로그 |
| `read:storage` / `write:storage` | 스토리지 |
| `read:monitoring` | 모니터링 (write 불가) |
| `*` | 전부 (기본값) |

→ Phase 6 1차는 `role`만 적용, `scopes`는 Phase 8에서 본격화.

## 5. 세션 스키마

### 5.1 일반 세션 (로그인 사용자)

```python
session = {
  "user_id":   123,
  "username":  "alice",
  "tenant_id": 1,                # 현재 작업 중인 tenant
  "role":      "tenant_admin",   # tenant_id에서의 역할
  "is_super":  False,            # super_admin이면 True
}
```

### 5.2 Impersonate 세션 (Phase 7)

```python
session = {
  "user_id":   123,
  "username":  "alice",
  "tenant_id": 2,                # impersonate 대상
  "role":      "tenant_admin",   # impersonate 시 부여 역할 (보통 tenant_admin)
  "is_super":  True,
  "impersonate": {
    "real_user_id":   123,
    "real_tenant_id": 0,
    "started_at":     "2026-06-05T12:34:56Z",
    "expires_at":     "2026-06-05T13:34:56Z",  # 1h 만료 권장
    "reason":         "support ticket #4521",
  }
}
# 모든 행위는 audit log에 impersonate.* 메타와 함께 기록
```

### 5.3 API 키 세션 (stateless)

```python
g = {
  "api_key_authenticated": True,
  "tenant_id": 1,
  "role":      "tenant_viewer",
  "scopes":    {"read:pipeline","read:catalog"},
}
```

## 6. 경로 분류 (4-class)

`enforce_request_rbac()`가 사용할 분류. 라우트 PR 시 분류 명시 필수.

| 클래스 | 정의 | 권한 결정 | 예시 |
|---|---|---|---|
| **PUBLIC** | 인증 면제 또는 self-service | 통과 또는 본인만 | `/api/admin/auth/login`, `/api/admin/auth/logout`, `/api/admin/auth/me`, `/api/admin/lang`, `/api/health` |
| **TENANT_SCOPED** | 자기 tenant 컨텍스트 필요 | GET: viewer 이상 / 변경: editor 이상 / 일부 변경: admin (각 라우트 명시) | `/api/pipeline/*`, `/api/import/*`, `/api/storage/*`, `/api/catalog/*`, `/api/monitoring/*` (일반), `/api/collector_*` |
| **TENANT_ADMIN_ONLY** | tenant_admin 전용 | tenant_admin 또는 impersonate 중 super_admin | `/api/tenant/me/members/*`, `/api/tenant/me/settings`, `/api/tenant/me/api-keys/*`, `/api/tenant/me/billing` |
| **SYSTEM_ONLY** | super_admin 전용 | super_admin | `/api/sys/tenants/*`, `/api/sys/users/*`, `/api/sys/audit/*`, `/api/sys/impersonate/*` |

### 6.1 코드 표현

```python
# backend/services/rbac.py
PATH_CLASSES = {
    "PUBLIC": (
        "/api/admin/auth/login", "/api/admin/auth/logout", "/api/admin/auth/me",
        "/api/admin/lang", "/api/health",
    ),
    "TENANT_ADMIN_ONLY": (
        "/api/tenant/me/members", "/api/tenant/me/settings",
        "/api/tenant/me/api-keys", "/api/tenant/me/billing",
    ),
    "SYSTEM_ONLY": (
        "/api/sys/", "/admin/sys/",          # /admin/sys/* HTML 페이지도 포함
    ),
    # TENANT_SCOPED는 기본값 (위 셋에 해당하지 않으면)
}

def classify(path):
    for cls in ("PUBLIC", "TENANT_ADMIN_ONLY", "SYSTEM_ONLY"):
        for p in PATH_CLASSES[cls]:
            if path == p or path.startswith(p + "/") or path.startswith(p):
                return cls
    return "TENANT_SCOPED"
```

### 6.2 GET 가드 (변경 메서드와 별도)

| GET path | 추가 가드 |
|---|---|
| `/api/monitoring/logs/audit` | tenant_admin 이상 (조회만이지만 민감) |
| `/api/sys/*` | SYSTEM_ONLY로 이미 차단됨 |
| HTML `/admin/users`, `/admin/sys/*` | 페이지 진입 단계 차단 |

### 6.3 변경 메서드 가드

기본 정책: TENANT_SCOPED 변경은 `tenant_editor` 이상. 단, 특정 액션(파이프라인 활성화/비활성화, 컬렉터 등록·삭제 등 운영 영향 큰 것)은 `tenant_admin` 이상으로 제한 가능 → 라우트별 `require_role("tenant_admin")` 추가.

## 7. 헬퍼 API (rbac.py 확장)

```python
# 컨텍스트 조회
def current_user_id():    return session.get("user_id") or g.get("api_user_id")
def current_tenant_id():  return g.tenant_id  # before_request에서 주입
def current_role():       return g.role        # before_request에서 주입
def is_super():           return session.get("is_super", False) or g.get("is_super_key", False)
def is_impersonating():   return bool(session.get("impersonate"))

# 등급 비교
def role_at_least(r):     return ROLE_RANK.get(current_role(), -1) >= ROLE_RANK[r]
def is_tenant_admin():    return role_at_least("tenant_admin")
def is_tenant_editor():   return role_at_least("tenant_editor")

# 객체 검증 (cross-tenant 방지)
def assert_tenant(obj):
    if not hasattr(obj, "tenant_id"): return
    if obj.tenant_id != current_tenant_id() and not is_super():
        raise CrossTenantAccess()

# 데코레이터
def require_role(min_role):  ...   # 기존 그대로
def require_tenant_admin(fn):     return require_role("tenant_admin")(fn)
def require_super(fn):            ...   # is_super() 체크

# 미들웨어
def enforce_request_rbac():
    cls = classify(request.path)
    if cls == "PUBLIC": return None
    # 이 시점에 g.tenant_id, g.role 이미 주입 (before_request 앞단)
    if cls == "SYSTEM_ONLY":
        return None if is_super() else _forbidden()
    if cls == "TENANT_ADMIN_ONLY":
        return None if is_tenant_admin() else _forbidden()
    # TENANT_SCOPED
    if request.method in ("GET","HEAD","OPTIONS"):
        return None  # role_at_least(viewer)은 자명
    # 변경 메서드
    return None if is_tenant_editor() else _forbidden()
```

## 8. 코드 마이그레이션 항목 (Phase별)

### Phase 1
- `backend/models/tenant.py` 신설 (Tenant, TenantMembership)
- `user` 테이블 `is_super` 컬럼 추가
- 시드 + 백필 SQL 실행
- `rbac.py`에 새 `ROLE_RANK`만 추가(미사용)

### Phase 2
- `rbac.py`에 `current_tenant_id`, `is_super`, `is_impersonating` 헬퍼 추가
- `app.py before_request`에서 tenant 컨텍스트 주입 (flag off면 tenant_id=1 강제)
- 로그인 라우트(admin.py) 확장 — 멤버십에서 tenant_id/role 결정
- 화이트리스트 임시 호환 (PATH_CLASSES 1차 도입, TENANT_SCOPED 분류만 도입해도 동작)

### Phase 4 (모듈별)
- 라우트에서 `session.get("role")` / `_is_admin()` 직접 호출 제거 → 헬퍼로 통일
  - 영향 파일 (현 코드 검색 기준):
    - `notice.py:_is_admin()` → `rbac.is_tenant_admin()` 또는 `role_at_least(...)`
    - `admin.py` user CRUD → `is_tenant_admin()` 가드
- 모든 변경 라우트가 `require_role()` 명시 (특히 admin 전용 액션)
- 객체 참조 시 `assert_tenant(obj)` 가드

### Phase 5~6
- MinIO 접근 헬퍼 `bucket_for(tenant_id, role)` 도입
- API 키 v2 스키마로 마이그레이션, `api_auth.authenticate_api_key` 확장

### Phase 7
- `/api/sys/tenants/*` 라우트 신설 (SYSTEM_ONLY)
- `/api/tenant/me/members/*`, `/api/tenant/me/settings` 신설 (TENANT_ADMIN_ONLY)
- impersonate 라우트 — `POST /api/sys/impersonate/start`, `POST /api/sys/impersonate/stop`
- 모든 행위 감사 (`audit_log.actor_user_id`, `actor_tenant_id`, `impersonate_meta`)

## 9. Impersonate 흐름 상세 (Phase 7)

```
super_admin alice (user_id=1, is_super=True)
  ↓ POST /api/sys/impersonate/start { tenant_id: 5, reason: "ticket #X" }
세션 변경:
  tenant_id     = 5
  role          = "tenant_admin"   (impersonate 기본값)
  impersonate   = { real_user_id:1, real_tenant_id:0, expires_at: now+1h, reason:... }
  ↓ 모든 행위:
    audit_log.actor_user_id    = 1   (실제 사용자)
    audit_log.acting_as_tenant = 5   (어느 tenant로 행동했는지)
    audit_log.impersonate_meta = { reason, started_at }
  ↓ POST /api/sys/impersonate/stop
세션 복귀:
  tenant_id  = 0 (또는 직전 본인 tenant)
  role       = "super_admin"
  impersonate 제거
```

추가 보강 (O4에 따라 결정):
- MFA 필수 (impersonate start 시 한 번 더 인증)
- 사전 승인 (다른 super_admin의 승인 필요)
- 시간 제한 (기본 1h, 최대 24h)
- Read-only impersonate 옵션 (변경 불가)

## 10. 레거시 코드 정리 항목 (Phase 4~5 일괄)

| 위치 | 패턴 | 변경 |
|---|---|---|
| `notice.py:_is_admin()` | `session.get("role") == "admin"` | `rbac.is_tenant_admin()` 호출 |
| `admin.py:764` (login) | `session["role"] = user.role` | 멤버십에서 결정해 주입 |
| `app.py:236` (context_processor) | `is_admin/current_role` 노출 | `is_tenant_admin/current_role/current_tenant/is_super` 노출 |
| `rbac.py:ADMIN_ONLY_GET_PATHS` | 단일 list | `PATH_CLASSES` 구조화 |
| `rbac.py:RBAC_ALLOWED_FOR_ALL_PATHS` | 단일 list | `PATH_CLASSES["PUBLIC"]`로 흡수 |
| `rbac.py:normalize_role` | 2-role 흡수 | 4-role 매핑 (legacy → editor/admin/viewer) |
| `api_auth.py:authenticate_api_key` | viewer 고정 | 키 record의 tenant_id/role 사용 |

## 11. 테스트 (구체는 `multitenant-test-policy.md`)

각 RBAC 변경 PR은 최소:
- 4-role 각각으로 권한 매트릭스 § 2.2 항목 검증
- Cross-tenant: tenant 2 사용자가 tenant 1 데이터 시도 → 403/404
- Impersonate: 권한 부여 + audit 기록 정확성

## 12. UI 영향 요약

- 좌측 네비게이션이 `current_role`에 따라 메뉴 가시성 변경 (현 `is_admin` 분기 확장)
- 우상단 사용자 메뉴에 **현재 tenant 표시** + 멀티 tenant 멤버의 경우 **tenant 전환** (Phase 7)
- super_admin 전용 햄버거 메뉴: "System 콘솔" 진입 + impersonate 시 화면 상단 띠 ("⚠ Impersonating <tenant>")
- `tenant_editor`의 메뉴: 사용자 관리·tenant 설정 항목 숨김

## 13. 잠긴 결정 갱신 (2026-06-05)

부모 문서 `multitenant-design-v1.md` § 2.2 D16에 따라 impersonate 정책 잠금:

- **super_admin은 항상 impersonate 가능** — 별도 MFA·승인 절차 없음
- 시간 제한도 강제 안 함 (1h 만료는 보안 권장 사항으로만 유지)
- 단, **모든 impersonate 행위는 audit log에 의무 기록** — `actor_user_id`, `acting_as_tenant`, `started_at`/`stopped_at`, `reason`(optional)
- 안정화 이후(2027 Q1+) MFA·승인 강화 재검토

→ § 9.4 추가 보강 항목(MFA/사전 승인/Read-only 옵션)은 안정화 이후 시점으로 deferred.

### 보류 항목 (실제 코딩 시 결정)

- API 키 발급 권한: tenant_admin만? tenant_editor도 자기 키 발급 가능? → **tenant_admin만** 1차 권장 (운영 단순)
- `tenant_owner` 같은 추가 역할: 안정화 이후 비즈니스 요구 발생 시 재검토
