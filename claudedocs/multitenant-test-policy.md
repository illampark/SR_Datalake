# 멀티테넌트 격리 테스트 정책 v1

**Phase 0 산출물 / 작성일: 2026-06-05 / 부모 문서: `multitenant-design-v1.md`**

이 문서는 멀티테넌트 전환의 핵심 안전망인 **테넌트 격리 보증**을 어떻게 코드로 강제하는지를 정의한다. Phase 4부터 모든 PR이 이 정책을 만족해야 한다.

---

## 1. 원칙

| 원칙 | 의미 |
|---|---|
| **누출 = 치명 결함** | Cross-tenant 데이터 누출은 보안 사고. 발견 시 즉시 핫픽스 + 사후 분석 |
| **CI 게이트 우선** | 사람의 코드 리뷰는 보완책. CI가 1차 방어 |
| **3중 가드** | 서비스 계층 + ORM 이벤트 + 테스트로 막음 |
| **Belt-and-suspenders** | 한 계층 실수해도 다른 계층이 잡음 |
| **신규 라우트 = 신규 테스트** | "라우트 추가됐는데 테스트 없음" PR 차단 |

## 2. 격리 가드 (L2 schema + 3계층 belt-and-suspenders)

부모 문서 D2 결정 — L2 schema-per-tenant + bucket-per-tenant를 **처음부터** 적용. 그 위에 추가로 코드 3계층 가드를 두는 belt-and-suspenders 구성. 한 계층의 실수가 격리를 깨지 않게 한다.

| 가드 | 어떤 사고를 막는가 |
|---|---|
| **PG schema 분리** (`SET search_path`) | 잘못된 쿼리도 잘못된 schema에서 빈 결과 반환, 누출 가능성 자체 차단 |
| **tenant_id 컬럼** (모든 도메인 테이블) | search_path 실수 시 row 가드로 한 번 더 잡음 |
| **서비스 계층 명시 필터** | 쿼리 작성 단계에서 명시적 표현, 코드 리뷰 가능 |
| **ORM before_flush 이벤트** | 신규/수정 객체의 tenant_id 자동 주입·검증 |
| **CI 격리 테스트** | 사람 실수 자동 검출 |

### 2.1 계층 1 — 서비스 계층 (필수, 항상)

모든 서비스 함수는 `tenant_id`를 매개변수로 받고 쿼리에 명시 적용.

```python
# Good
def get_pipelines(tenant_id: int, ...):
    return db.query(Pipeline).filter(Pipeline.tenant_id == tenant_id).all()

# Bad — tenant 컨텍스트가 암묵적
def get_pipelines():
    return db.query(Pipeline).all()
```

### 2.2 계층 2 — ORM 이벤트 (안전망)

SQLAlchemy `before_flush` 이벤트에서 `tenant_id` 자동 주입·검증.

```python
@event.listens_for(Session, "before_flush")
def enforce_tenant_on_flush(session, flush_context, instances):
    if not current_app.config.get("MULTITENANT_MODE"): return
    tid = g.get("tenant_id")
    if tid is None: return  # PUBLIC 라우트 등
    for obj in session.new:
        if hasattr(obj, "tenant_id"):
            if obj.tenant_id is None:
                obj.tenant_id = tid                    # 자동 주입
            elif obj.tenant_id != tid and not g.get("is_super"):
                raise CrossTenantWrite(type(obj).__name__)
    for obj in session.dirty:
        if hasattr(obj, "tenant_id") and obj.tenant_id != tid and not g.get("is_super"):
            raise CrossTenantWrite(type(obj).__name__)
```

### 2.3 계층 3 — 테스트 (검증)

CI가 모든 라우트에 대해 격리 시나리오 자동 실행. § 4 참조.

## 3. 테스트 카테고리

| 카테고리 | 목적 | 빈도 |
|---|---|---|
| **회귀 테스트** (`tenant=1` 단독) | `MULTITENANT_MODE=off` 또는 단일 tenant 시 현 동작 보장 | 모든 PR |
| **격리 테스트** (2 tenant 평행) | tenant 2 데이터가 tenant 1에 안 보임 | 모든 도메인 PR |
| **누출 fuzz** (랜덤 ID) | cross-tenant ID로 조작 시도 → 403/404 | 야간 / 주간 |
| **권한 매트릭스** (4-role × 경로 클래스) | 모든 조합 검증 | RBAC 변경 PR |
| **Impersonate 감사** | impersonate 행위가 audit log에 정확히 기록 | Phase 7 PR |

## 4. pytest 인프라 설계

### 4.1 Fixture 골격 (`tests/conftest.py`)

```python
import pytest

@pytest.fixture
def two_tenants(db_session):
    """tenant 1, 2 + 각 admin/editor/viewer 사용자 6명 생성"""
    t1 = Tenant(slug='t1', name='Tenant 1')
    t2 = Tenant(slug='t2', name='Tenant 2')
    db_session.add_all([t1, t2]); db_session.commit()
    users = {}
    for tid, slug in [(t1.id, 't1'), (t2.id, 't2')]:
        for role in ('tenant_admin', 'tenant_editor', 'tenant_viewer'):
            u = User(username=f"{slug}_{role}", password_hash="...")
            db_session.add(u); db_session.flush()
            db_session.add(TenantMembership(user_id=u.id, tenant_id=tid, role=role))
            users[f"{slug}_{role}"] = u
    db_session.commit()
    return {"t1": t1, "t2": t2, "users": users}

@pytest.fixture
def super_admin(db_session):
    u = User(username='alice_super', is_super=True, password_hash="...")
    db_session.add(u); db_session.commit()
    return u

@pytest.fixture
def login_as(client):
    def _login(user, tenant_id=None):
        with client.session_transaction() as s:
            s["user_id"] = user.id
            if tenant_id:
                m = TenantMembership.q.filter_by(user_id=user.id, tenant_id=tenant_id).first()
                s["tenant_id"] = m.tenant_id
                s["role"] = m.role
            s["is_super"] = user.is_super
        return client
    return _login
```

### 4.2 격리 테스트 패턴 (필수 패턴 — 모든 도메인 모듈)

```python
def test_pipeline_isolation(client, two_tenants, login_as):
    t1, t2 = two_tenants["t1"], two_tenants["t2"]
    u_t1_admin = two_tenants["users"]["t1_tenant_admin"]
    u_t2_admin = two_tenants["users"]["t2_tenant_admin"]

    # t1 admin이 파이프라인 생성
    c = login_as(u_t1_admin, t1.id)
    p1 = c.post("/api/pipeline", json={"name":"P1"}).get_json()["data"]["id"]

    # t2 admin이 list → P1 안 보임
    c2 = login_as(u_t2_admin, t2.id)
    res = c2.get("/api/pipeline").get_json()["data"]
    assert all(p["id"] != p1 for p in res)

    # t2 admin이 P1 직접 조회 → 404
    assert c2.get(f"/api/pipeline/{p1}").status_code == 404

    # t2 admin이 P1 수정 시도 → 404
    assert c2.put(f"/api/pipeline/{p1}", json={"name":"X"}).status_code == 404

    # t2 admin이 P1 삭제 시도 → 404
    assert c2.delete(f"/api/pipeline/{p1}").status_code == 404
```

### 4.3 권한 매트릭스 테스트 (RBAC 변경 PR)

```python
@pytest.mark.parametrize("role,method,expected", [
    ("tenant_viewer", "GET",  200),
    ("tenant_viewer", "POST", 403),
    ("tenant_editor", "POST", 200),
    ("tenant_editor", "DELETE", 200),   # editor도 자기 tenant 변경 가능
    ("tenant_admin",  "POST", 200),
])
def test_pipeline_role_matrix(client, two_tenants, login_as, role, method, expected):
    user = two_tenants["users"][f"t1_{role}"]
    c = login_as(user, two_tenants["t1"].id)
    if method == "GET":
        assert c.get("/api/pipeline").status_code == expected
    elif method == "POST":
        assert c.post("/api/pipeline", json={"name":"X"}).status_code == expected
    # ...
```

추가: `tenant_admin` 전용 액션 (사용자 초대, settings 변경)이 `tenant_editor`에서 403 나오는지 명시 케이스.

### 4.4 누출 fuzz (야간 CI)

```python
# tests/fuzz/test_cross_tenant_fuzz.py
import random

def test_random_cross_tenant_ids(client, two_tenants, login_as):
    """t2 사용자가 t1 ID 범위로 무작위 접근 시도 → 모두 404"""
    t1_data = create_bulk(tenant=t1, n=100)         # t1에 100개 객체 생성
    c2 = login_as(two_tenants["users"]["t2_tenant_admin"], two_tenants["t2"].id)
    for _ in range(200):
        target_id = random.choice(t1_data).id
        for endpoint in ["/api/pipeline", "/api/import", "/api/catalog"]:
            r = c2.get(f"{endpoint}/{target_id}")
            assert r.status_code in (403, 404), f"LEAK: {endpoint}/{target_id} → {r.status_code}"
```

## 5. CI 게이트

### 5.1 PR 머지 차단 조건

| 게이트 | 조건 | 우회 |
|---|---|---|
| `pytest tests/unit -m isolation` | 모든 격리 테스트 통과 | 불가 |
| 신규 라우트 → 격리 테스트 추가 | grep으로 신규 `@*_bp.route` vs 테스트 추가 비교 | reviewer override (PR comment `BYPASS_ISOLATION_TEST: <이유>`) |
| `tenant_id` 컬럼 누락 검사 | Phase 3 이후 모델에 tenant_id 없으면 fail | 글로벌 테이블 화이트리스트 |
| 직접 `session.get("role")` 사용 | grep 으로 RBAC 헬퍼 우회 검출 | 의도된 사용은 `# rbac-allow-raw-session` 주석 |

### 5.2 GitHub Actions 워크플로 (예시)

```yaml
# .github/workflows/multitenant-gate.yml (Phase 4 시점에 활성화)
name: multitenant gate
on: [pull_request]
jobs:
  isolation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: python -m pytest tests/ -m isolation --maxfail=1 -q
      - run: tools/check_new_routes_have_tests.py
      - run: tools/check_tenant_id_columns.py
```

### 5.3 야간 fuzz 워크플로

```yaml
on:
  schedule: [{ cron: "0 18 * * *" }]   # 매일 03:00 KST
jobs:
  fuzz:
    steps:
      - run: pytest tests/fuzz --count=10 --hypothesis-profile=ci
```

## 6. 코드 리뷰 체크리스트

PR 작성자·리뷰어가 사용. 라우트/서비스 변경 PR에 의무 포함.

```markdown
## Multitenant Review Checklist
- [ ] 신규/수정 서비스 함수는 `tenant_id`를 매개로 받는다
- [ ] 신규/수정 쿼리는 `tenant_filter()` 또는 `WHERE tenant_id = ?` 명시
- [ ] 신규 모델은 `tenant_id BIGINT NOT NULL` 컬럼 + 인덱스 (글로벌 테이블 예외 명시)
- [ ] 다른 도메인 객체 ID 참조 시 `assert_tenant(obj)` 또는 동등 가드
- [ ] 신규 라우트는 PATH_CLASSES 4분류 중 하나로 명시 (PR 설명에 기록)
- [ ] 신규 변경 라우트는 `tenant_editor` 또는 더 높은 가드
- [ ] `tenant_admin` 전용 액션은 명시적 `require_role("tenant_admin")`
- [ ] `session.get("role")` 직접 사용 0 (헬퍼 사용)
- [ ] 격리 테스트 추가 (또는 BYPASS 사유 명시)
- [ ] 권한 매트릭스 테스트 (RBAC 변경 PR만)
```

## 7. 누출 의심 발생 시 대응 절차

1. **확인** — 의심 케이스 재현 (재현 안 되면 hypothesis-driven test로 자동화)
2. **격리** — 해당 라우트를 `@require_role('super_admin')`로 임시 잠금
3. **핫픽스** — 가드 추가 PR (격리 테스트 동반 필수)
4. **후속** — affected tenant 알림, audit log 점검, retroactive 권한 확인
5. **회귀** — 같은 패턴 다른 라우트 spotcheck (`grep` + 리뷰)

## 8. 단계별 적용 범위 (2026-12-31 안정화 일정 반영)

| Phase | 적용 |
|---|---|
| 1~2 | 인프라 준비 (fixture, CI 설정), 테스트 미실행 (라우트 미변경) |
| 3 | `tenant_id` 컬럼 검사 게이트 + **schema 분리 검증** (tenant 2 schema 자동 생성 후 cross-schema 접근 차단 테스트) |
| 4 | 모듈 마이그레이션 PR마다 격리 테스트 동반 필수 + CI 게이트 활성 |
| 5 | MinIO 격리 테스트 추가 (bucket cross-access 시도) |
| 6 | API 키 v2 권한 테스트 (MQTT 토픽 격리는 deferred) |
| 7 | impersonate 감사 테스트 — D16에 따라 MFA·승인 없이 진행되므로 audit 정확성이 1차 방어 |
| 8 | 야간 fuzz 워크플로 활성. quota/rate-limit 테스트는 안정화 이후 |

## 9. 메트릭

격리 효과성을 정량 추적.

| 메트릭 | 목표 |
|---|---|
| 격리 테스트 통과율 | 100% |
| 신규 라우트 중 격리 테스트 추가 비율 | ≥ 95% |
| 누출 fuzz 발견 incidents | 분기당 0 |
| Cross-tenant 가드 우회 (BYPASS 주석) 수 | 분기당 ≤ 5 |
| 평균 격리 테스트 실행 시간 | < 3분 |

→ Phase 8 강화 단계에서 대시보드화.
