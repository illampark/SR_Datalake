# SDL 멀티테넌트 전환 설계 v1

**Phase 0 산출물 / 작성일: 2026-06-05 / 대상: Option A (In-place 점진 진화)**

이 문서는 Phase 1 이후 모든 구현의 단일 진실(Single Source of Truth)이다. 결정 사항은 잠겨 있으며, 변경 시 v2로 발행한다.

---

## 1. 목표

- 현재 단일 회사용 SDL을 **다수 회사를 격리 수용하는 멀티테넌트 SaaS**로 진화
- **1년 내 30~50 tenant 수용** + **2026-12-31까지 안정화 완료** (최소 인원 운영 가정)
- 현 단일테넌트 SKU는 **계속 지원**(온프렘 케이스). `MULTITENANT_MODE=off` 플래그로 같은 코드베이스 분기
- 현 운영 고객을 **`tenant_id = 1`로 그대로 수용**. 마이그레이션 비용 최소화
- 격리 강도 **L2 (schema-per-tenant + bucket-per-tenant) 부터 시작** — 데이터 주권/리전 요구 + 30~50 규모에서 row-level의 누출 위험 회피
- MVP 출시 ~2026-10, 안정화 ~2026-12 (약 6~7개월)

## 2. 잠긴 결정 사항 (Locked Decisions)

### 2.1 아키텍처·역할

| # | 항목 | 결정 | 이유 |
|---|---|---|---|
| D1 | 진화 경로 | **Option A** (In-place 점진 진화) | 코드베이스 정돈도 양호, fork의 병행 유지 비용 회피 |
| D2 | 격리 강도 | **L2 — schema-per-tenant + bucket-per-tenant** (처음부터) | 30~50 tenant 규모에서 row-level 누출 위험 + 데이터 주권 요건. L1→L2 후행 마이그가 더 큼 |
| D3 | 격리 보강 | tenant_id 컬럼도 함께 유지 (Belt-and-suspenders) | 잘못된 schema 접근도 row 가드로 한 번 더 막음 |
| D4 | 테넌트 식별 (1단계) | **세션 기반** — `session["tenant_id"]` | DNS·라우팅 변경 없이 진입 |
| D5 | 테넌트 식별 (장기) | **subdomain** — `<slug>.sdl.example.com` (Phase 7+) | 멀티테넌트 SaaS UX 표준 |
| D6 | 사용자-테넌트 관계 (1단계) | **1:1** (1 user = 1 tenant) | 단순. N:M(파트너)은 Phase 7에서 |
| D7 | 역할 모델 | **4-role** — `super_admin` / `tenant_admin` / `tenant_editor` / `tenant_viewer` | 위임 모델 깔끔, 자세한 정의는 `rbac-target-v1.md` |
| D8 | `tenant_editor` 도입 | **Yes** | 파이프라인 작성·수정은 가능, 사용자/정책은 불가능 — 산업 현장 운영자 페르소나 정확히 표현 |
| D9 | 권한 정책 표현 | **코드 하드코딩** (1차) — 경로 분류 기반 | DB-정책 테이블은 추후 재검토 |
| D10 | API 키 — tenant 스코프 | **키별 1 tenant 고정** + 키별 role | 격리 명확, 키 노출 시 영향 범위 최소 |
| D11 | 단일테넌트 SKU 유지 | **유지** — `MULTITENANT_MODE=off` 인스턴스 영구 지원 | 온프렘 고객 보호 |
| D12 | Tenant ID 타입 | `BIGINT` + `slug VARCHAR(50) UNIQUE` 동반 | 외부 URL에는 slug, 내부에는 id |

### 2.2 비즈니스·운영 (구 O1~O6 잠금)

| # | 항목 | 결정 | 영향 |
|---|---|---|---|
| D13 | 1년 내 목표 tenant 수 | **30~50** | L2 격리 필연, 자동 onboarding(Phase 7) 필수 |
| D14 | 데이터 주권/리전 | **각 tenant L2 수준 격리** (schema + bucket 분리) | D2와 일치. 다중 리전은 후속 검토 |
| D15 | 청구 모델 | **1차: 사용량 미터링만, 빌링 후속** | Phase 7의 빌링 모듈은 제외, 사용량 수집·노출만 |
| D16 | Impersonate 정책 | **super_admin은 항상 가능** (별도 MFA·승인 미적용) | 운영 단순성 우선. 모든 행위는 audit log에 기록 |
| D17 | 안정화 데드라인 | **2026-12-31** | Phase 1~7을 ~6~7개월로 압축. MQTT 격리 등 deferred |
| D18 | 운영 인력 | **최소 인원** | 범위 컷, 자동화 우선. 사람 검증 의존 작업은 회피 |
| D19 | super_admin 인증 강화 | **현 수준 유지** (별도 IDP·WebAuthn 미도입) | 안정화 후 재검토 |

### 2.3 보류 결정 (없음)

Phase 0 시점 모든 결정 사항이 잠금. 이후 신규 결정 필요 시 v2로 발행한다.

## 3. 아키텍처 개요

```
┌────────────────────────────────────────────────────────────┐
│                  Client (web / API / SFTP)                 │
└────────────────────────────────────────────────────────────┘
       │ session.cookie (tenant_id, user_id, role)
       │ X-API-Key (tenant 스코프 + role)
       ▼
┌────────────────────────────────────────────────────────────┐
│   sdl-app (Flask)  ── 단일 인스턴스, 멀티테넌트 컨텍스트 ──   │
│ ┌──────────────────────────────────────────────────────┐  │
│ │ before_request: 인증 → tenant 컨텍스트(g.tenant_id)   │  │
│ │                  → enforce_request_rbac()             │  │
│ └──────────────────────────────────────────────────────┘  │
│   routes/* → services/* (tenant_id 매개)                   │
└────────────────────────────────────────────────────────────┘
       │ tenant 필터링된 쿼리                ┌──── webhook ←──┐
       ▼                                    ▼                 │
┌────────────────────┐  ┌──────────────────────┐  ┌──────────────┐
│  Postgres (단일 DB)  │  │  MinIO (단일 인스턴스)  │  │  Mosquitto    │
│  - 글로벌: tenant,   │  │  - t-{id}-files       │  │  - t/{id}/... │
│    user, system_log │  │  - t-{id}-archive     │  │    토픽 + ACL  │
│  - 도메인: 각 테이블에│  │  - t-{id}-backup      │  └──────────────┘
│    tenant_id 컬럼   │  │  - t-{id}-exports     │
└────────────────────┘  └──────────────────────┘
```

**핵심 변화**:
- sdl-app은 단일 인스턴스 유지 (옵션 D 같은 풀스택 복제 X)
- PG는 단일 schema에서 시작 (Phase 9에서 schema 분리 옵션)
- MinIO는 **버킷-per-tenant** (Phase 5에서 격리)
- MQTT는 **토픽 prefix-per-tenant** (Phase 6)

## 4. 데이터 모델 변경 요약 (단계별)

**Phase 1** — 신규 글로벌 테이블 + Alembic 베이스라인:
```sql
tenant            (id BIGINT, slug, name, status, plan, created_at, settings JSONB)
tenant_membership (id, user_id, tenant_id, role, created_at)  -- UNIQUE(user_id, tenant_id)
ALTER TABLE "user" ADD COLUMN is_super BOOLEAN NOT NULL DEFAULT FALSE;
```
초기 데이터:
- `tenant(id=0, slug='system')` — super_admin 소속용
- `tenant(id=1, slug='default')` — 현 운영 고객
- 기존 모든 user → tenant 1의 멤버

**Phase 3** — L2 스키마 인프라 + 도메인 테이블 이전 (L2 처음부터 적용):
- 현 도메인 테이블은 `public` 유지 (= tenant 1의 schema)
- 신규 tenant는 `t_<id>` schema 자동 생성 + 도메인 테이블 복제
- `tenant_id` 컬럼은 belt-and-suspenders로 모든 도메인 테이블에 함께 추가 (잘못된 schema 접근도 row 가드로 잡음)
- 자세한 마이그·인프라는 `migration-guide.md` § 3, § 9 참조

```sql
ALTER TABLE pipeline           ADD COLUMN tenant_id BIGINT NOT NULL REFERENCES tenant(id);
ALTER TABLE import_collector   ADD COLUMN tenant_id BIGINT NOT NULL REFERENCES tenant(id);
ALTER TABLE data_catalog       ADD COLUMN tenant_id BIGINT NOT NULL REFERENCES tenant(id);
-- ... (대상 전체는 §6 참조)
-- 새 tenant 추가 시: CREATE SCHEMA t_<id>; CREATE TABLE t_<id>.pipeline (LIKE public.pipeline INCLUDING ALL); ...
```
백필: 모두 `tenant_id = 1` (public schema 그대로 = tenant 1 schema).

**Phase 6** — API 키 모델 확장:
```sql
ALTER TABLE api_key
  ADD COLUMN tenant_id BIGINT REFERENCES tenant(id),
  ADD COLUMN role VARCHAR(20) NOT NULL DEFAULT 'tenant_viewer',
  ADD COLUMN scopes JSONB DEFAULT '[]';
```

> **L2 적용 범위**: 도메인 테이블만 schema 분리. 글로벌 테이블(`tenant`, `user`, `tenant_membership`, `api_key`, sys-level `system_log`)은 `public` 유지.

## 5. 도메인 테이블 분류

### 6.1 tenant_id 컬럼 추가 대상 (Phase 3)
- 파이프라인: `pipeline`, `pipeline_step`, `pipeline_binding`, `pipeline_run`
- 가공 규칙: `normalize_rule`, `unit_conversion`, `filter_rule`, `aggregate_config`, `enrich_config`, `script_config`, `anomaly_config`
- 컬렉터: `import_collector`, `import_run`, `file_collector`, `db_connector`, `mqtt_connector`, `opcua_connector`, `modbus_connector`, `api_connector`, `db_tag`
- 카탈로그: `data_catalog`, `dataset`, `metadata_*`
- 파일 인덱스: `file_index`, `file_index_state`, `minio_object`
- 알림/감사 (스코프 필요한 부분): `alarm`, `notice`, `backup`
- 통합/게이트웨이: `integration`, `gateway`

### 6.2 글로벌 유지 (tenant_id 미적용)
- `tenant`, `tenant_membership`
- `user` (멤버십으로 N개 tenant 소속 가능)
- `system_log` 시스템 레벨 행 (tenant_id NULL 허용)
- `api_key` — Phase 6에서 tenant_id 컬럼 추가하되 글로벌 테이블 위치 유지
- `admin_setting` 일부 (로그인 정책 등 시스템 전반은 글로벌)

### 6.3 신중하게 분리 필요 (Phase 4 모듈 작업 시 결정)
- `system_settings` — 일부 시스템 전역, 일부 tenant별 설정 → 키 prefix로 분기 (`sys.*` vs `tenant.*`)
- `audit` — tenant 행위는 tenant 스코프, 시스템 행위는 글로벌 → row 분리

## 6. 테넌트 식별 흐름

### 7.1 1단계 — 세션 기반 (Phase 1~6)

```python
# 로그인 시 (admin.py)
user = authenticate(username, password)
memberships = get_memberships(user.id)
if len(memberships) == 1:
    session["tenant_id"] = memberships[0].tenant_id
    session["role"]      = memberships[0].role
elif len(memberships) > 1:
    redirect("/select-tenant")            # Phase 7에서 본격 구현
else:
    return 403  # no tenant
if user.is_super:
    session["is_super"] = True            # super_admin 표식 (별도 권한)
```

### 7.2 2단계 — Subdomain (Phase 7+)

`acme.sdl.example.com` → 미들웨어가 slug 추출 → 세션의 tenant_id와 일치 검증.

### 7.3 API 키 흐름 (Phase 6 확장)

```python
# api_auth.py
def authenticate_api_key():
    key = request.headers.get("X-API-Key")
    record = lookup_key_hash(key)
    if not record or record.revoked_at: return False
    g.api_key_authenticated = True
    g.tenant_id = record.tenant_id        # 신규
    g.role      = record.role             # 신규 (현 hardcoded viewer 제거)
    return True
```

## 7. 모듈 마이그레이션 순서 (Phase 4)

복잡도·의존도 기준 추정 순서. 한 모듈은 1개 PR.

1. `pipeline` (1주) — 가장 빈번 사용, 패턴 확립
2. `import_collector` (1주)
3. `data_catalog` (1주)
4. `monitoring` / `system_log` (1주 — NULL tenant_id 처리 포함)
5. `storage_file` (1주 — MinIO 격리는 Phase 5에서 별도)
6. `storage_rdbms` / `storage_tsdb` / `storage_retention` (1주)
7. `collector_*` 6종 (1.5주)
8. `alarm`, `notice`, `backup`, `audit` (1주)
9. `admin` — tenant_admin/super_admin 분기 (1주)

## 8. 점진 출시 전략

### 9.1 Feature flag `MULTITENANT_MODE`

```python
# config.py
MULTITENANT_MODE = os.getenv("MULTITENANT_MODE", "off").lower() == "on"
```

- `off`: 현 동작 100% (회귀 0). `tenant_id` 컬럼은 데이터에 있지만 라우트가 무시
- `on`: 멀티테넌트 흐름. 모든 라우트가 `g.tenant_id` 필터링 적용

### 9.2 단일테넌트 SKU 호환

- 온프렘 배포: `MULTITENANT_MODE=off`로 빌드
- SaaS 배포 (스테이징/프로덕션): `MULTITENANT_MODE=on`
- 같은 코드, 같은 이미지, 환경변수만 분기

### 9.3 Tenant #1 = 현 운영 고객

- Phase 1~3은 현 동작에 영향 0 (`MULTITENANT_MODE=off`)
- Phase 4부터 스테이징을 `on`으로 전환해 회귀 시험
- Tenant 2를 시험용으로 생성, 격리 회귀 테스트 + 사람 시연
- 프로덕션은 Phase 4 모듈 단위가 검증된 후 `on` 전환

## 9. 호환성 및 백워드 계약

| 인터페이스 | 1단계 | 2단계 (subdomain) | 비고 |
|---|---|---|---|
| Web UI (`/dashboard`, `/storage`, ...) | session 기반 | subdomain 기반 | path/route 자체는 유지 |
| API (`/api/...`) | session 또는 X-API-Key | + subdomain 옵션 | 키는 tenant 스코프 |
| 기존 API 키 | role=viewer 호환 모드 | 마이그레이션 후 명시 role | Phase 6에서 변환 |
| MinIO bucket 이름 | `sdl-files` 등 글로벌 | `t-{id}-files` | Phase 5에서 마이그레이션 |
| MQTT topic | 글로벌 | `t/{id}/...` prefix | Phase 6에서 마이그레이션 |
| SFTP 사용자 | MinIO root 1개 | 테넌트별 IAM 사용자 | Phase 5 |

## 10. 일정 압축 (2026-12-31 안정화 목표 반영)

기존 8~10개월 계획을 6~7개월로 압축. 압축 방법:

| 항목 | 압축 전 | 압축 후 | 방법 |
|---|---|---|---|
| Phase 1 | 2~3주 | 3~4주 | L2 schema 인프라까지 포함 (자동 schema 생성) |
| Phase 3 | 2~3주 | 4~5주 | tenant_id 컬럼 + schema 분리 동시 (L2 처음부터) |
| Phase 5 (MinIO) | 3~4주 | 3주 | 신규 tenant는 처음부터 bucket-per-tenant, 기존 tenant 1만 미러 |
| Phase 6 (MQTT) | 3~4주 | **deferred** | MQTT 쓰는 tenant 등장 시점에 별도 진행 |
| Phase 7 (콘솔) | 4~6주 | 3~4주 | 빌링 모듈 제외 (D15). 사용량 미터링만 수집. impersonate는 MFA 없이 단순 (D16) |
| Phase 8 (강화) | 4주+ | 2주 + 지속 | 격리 회귀 테스트·기본 감사만 |
| Phase 9 (schema) | +4~6주 | **흡수** | Phase 3에 통합 |

**압축 후 누적 일정** (1.5 FTE 기준):
- Phase 0: 완료 (이 문서)
- Phase 1: 2026-06-30
- Phase 2: 2026-07-15
- Phase 3: 2026-08-20
- Phase 4: 2026-10-15 (모듈별 6주, 우선순위 모듈 위주)
- Phase 5: 2026-11-05
- Phase 7: 2026-12-05 (Phase 6 deferred 우회)
- Phase 8 1차 + 안정화: **2026-12-31**

MQTT 격리 / N:M 멤버십 / SSO 등은 안정화 이후 단계로 미룬다.

## 11. 리스크 등록부 (요약)

| 리스크 | 영향 | 단계 | 완화 |
|---|---|---|---|
| Cross-tenant 데이터 누출 | 치명 | 4~8 | 3중 가드 (서비스+ORM+CI) + L2 schema 격리 — `multitenant-test-policy.md` 참조 |
| Tenant=1 회귀 | 중 | 4~5 | Feature flag, 모듈 단위 출시, 회귀 슈트 강화 |
| MinIO 마이그레이션 정합성 | 중 | 5 | 점검창 + ETag 검증 |
| Super_admin impersonate 남용 | 중 | 7 | (D16) MFA·승인 미적용 — audit log 강화로 사후 추적 |
| 큰 테이블 ALTER 락 | 중 | 3 | 점진 ALTER, 새벽 창 |
| L2 schema 자동 생성 실패 | 상 | 1·3 | 트랜잭션 + Alembic 모든 schema 적용 검증 |
| 일정 압축으로 인한 품질 저하 | 상 | 4~7 | 범위 컷 우선, 자동화 우선, 사람 검증 의존 항목 회피 |

## 12. 성공 기준 (Definition of Done — 2026-12-31)

- [ ] 2개 이상 실고객 tenant가 운영 중 (+ tenant 1 현 고객)
- [ ] L2 격리 동작 — 각 tenant가 별도 schema + 별도 MinIO 버킷
- [ ] 격리 회귀 테스트 슈트 통과율 100%, CI 게이트 활성
- [ ] super_admin 콘솔로 tenant CRUD + impersonate + 사용량 조회 가능 (빌링 모듈 미포함)
- [ ] 단일테넌트 SKU 인스턴스(온프렘 케이스) 회귀 0
- [ ] 평균 응답 시간 회귀 < 10% (성능 가드)
- [ ] 운영자 가이드 문서 1.0 발행
- [ ] 30~50 tenant 수용을 위한 자동 onboarding 흐름 동작

> **명시 deferred** (안정화 이후): MQTT 토픽 격리 / N:M 멤버십 / 빌링 모듈 / subdomain 라우팅 / SSO·WebAuthn / Phase 8의 quota·rate-limit 고도화

## 13. 참고 문서

- `rbac-target-v1.md` — RBAC 상세 (역할/경로/API 키)
- `migration-guide.md` — 마이그레이션 표준 패턴
- `multitenant-test-policy.md` — 격리 회귀 테스트 정책
- 기존: `claudedocs/minio-event-index-design.md` (MinIO 이벤트 인덱스 — 멀티테넌트 webhook 분기는 Phase 5에서 참조)
