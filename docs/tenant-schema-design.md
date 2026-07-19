# 테넌트별 스키마 설계 — 시계열 DB(TSDB) / RDB

> 내부 스토리지의 멀티테넌트 격리 설계 정리. 코드 기준: `tenant_pg.py`, `tenant_storage.py`,
> `models/storage.py`, `pipeline_modules.py`. 최종 확인 2026-07-19 (프로덕션 실측 포함).

## 공통 전제: 2단계 격리 모델

물리 DB는 **`sdl` 하나**뿐이다. 테이블은 성격에 따라 두 가지 방식으로 격리된다.

| 구분 | 격리 방식 | 위치 |
|---|---|---|
| **앱·메타 테이블** (pipeline, data_catalog, tag_metadata, tsdb_config, rdbms_config 등 ~69개) | **행(row) 격리** — `tenant_id` 컬럼(`TenantScopedMixin`) + 쿼리 시 `filter_by_tenant` | `public` 단일본 (모든 테넌트 공유) |
| **데이터 테이블** (time_series_data, RDBMS sink/import 테이블) | **물리(schema) 격리** — 테넌트 전용 PG 스키마 + 전용 role 권한 | tenant 1 → `public`, tenant N → `tenant_N` |

설정·카탈로그는 공용 테이블에서 `tenant_id`로 걸러내고, 실데이터는 물리적으로 다른 스키마에 분리된다.

## 명명 규칙
- 스키마: tenant 1 = `public` (legacy), tenant N(N>1) = `tenant_N`
- PG role: tenant 1 = `sdl_user` (공용), tenant N = `t_N_user` (전용, 랜덤 password)
- **`database_name` 은 모든 테넌트·TSDB·RDB 에서 항상 `sdl`** — 격리는 `schema_name` 으로만.
  (폐기된 별도 DB `sdl_tsdb` 는 어떤 설정에도 넣지 않는다. 2026-07-19 커밋 0b48714 로 자동 교정·차단)

---

## Part A — 시계열 DB (TSDB)

### 스키마 배치
| 테넌트 | 스키마 | 테이블 | 소유자 |
|---|---|---|---|
| tenant 1 | `public` | `public.time_series_data` | pg_database_owner |
| tenant N | `tenant_N` | `tenant_N.time_series_data` | `t_N_user` |

tenant N 테이블은 `CREATE TABLE tenant_N.time_series_data (LIKE public.time_series_data INCLUDING ALL)`
후 owner 를 `t_N_user` 로 이전 → 모든 테넌트가 동일 구조.

### `time_series_data` 컬럼 (모든 테넌트 공통)
| 컬럼 | 타입 | 의미 |
|---|---|---|
| id | bigint PK | 자동 증가 |
| tsdb_id | int NOT NULL | TsdbConfig.id |
| measurement | varchar(200) NOT NULL | 측정 이름 |
| tag_name | varchar(200) NOT NULL | 태그 이름 |
| connector_type / connector_id / pipeline_id | varchar/int | 출처 |
| value | double | 숫자 값 |
| value_str | text | 문자열/JSON 값 |
| data_type / unit / quality | varchar/int | 메타 |
| tags | json | 추가 key-value |
| timestamp | timestamp NOT NULL | 측정 시각 |
| created_at | timestamp | 적재 시각 |
| tenant_id | bigint NOT NULL default 1 | mixin, 이중 안전장치 |

**롱 포맷**: 태그 1개 관측 = 1행. 와이드(태그=컬럼) 조회는 카탈로그 레시피의
`MAX(CASE WHEN tag_name=... THEN value END)` 피벗 SQL 로 처리. (파이프라인 엔진에 cross-tag pivot 모듈 없음)

### 설정 (TsdbConfig)
`database_name=sdl` (항상) · `schema_name` 만 테넌트별(`public`/`tenant_N`) · `username`=테넌트 role.

### 조회/쓰기 경로
- 쓰기: 파이프라인 TSDB sink → `"schema".time_series_data` raw SQL INSERT (앱 엔진)
- 조회(앱 세션): `catalog.py` 가 `tenant_table()` 로 스키마 지정 → 자기 스키마만
- 조회(직접 psycopg2): `storage_tsdb` 가 TsdbConfig 의 username/schema 로 접속 → PG 권한으로 격리

---

## Part B — RDB (RDBMS sink / import)

### 스키마 배치
| 테넌트 | 스키마 | 테이블 | 소유자 |
|---|---|---|---|
| tenant 1 | `public` | 사용자 지정 테이블명 | sdl_user |
| tenant N | `tenant_N` | 사용자 지정 테이블명 | `t_N_user` |

TSDB 와 달리 고정 스키마가 아니라 파이프라인 sink / import 가 **런타임에 테이블 생성**.

### columnMapping 두 가지
**auto** (고정 스키마, 롱 포맷):
```
id(PK) | pipeline_id | connector_type | connector_id | asset_id | tag_name
       | value_num | value_str | data_type | unit | quality | collected_at
```
모든 값 컬럼 TEXT. 태그 1개 = 1행.

**flatten** (와이드): value 가 dict 일 때만, dict key 마다 컬럼 + `_pipeline_id`/`_connector_type`/`_tag_name`/`_collected_at` 메타. 새 key 등장 시 `ALTER TABLE ADD COLUMN` 자동.
(주의: OPC-UA/Modbus 워커는 태그당 스칼라 발행 → value 가 dict 가 아니므로 flatten 은 auto 로 폴백. 와이드가 필요하면 pivot 모듈 신규 구현 필요 — 현재 없음)

### 설정 (RdbmsConfig)
`database_name=sdl` (항상) · `schema_name` 만 테넌트별 · `username`=테넌트 role.
import_parser/sink 가 이 role 로 접속하면 search_path 가 자기 스키마로 고정 → 테이블이 올바른 스키마에 자동 생성·조회.

---

## PG 권한 격리 (TSDB·RDB 공통 기반)

`tenant_pg.py` 가 신규 테넌트 생성 시 자동 수행:
- `t_N_user` 전용 role 생성 (랜덤 password)
- `tenant_N` 스키마 생성 (AUTHORIZATION t_N_user)
- 자기 스키마 ALL PRIVILEGES + DEFAULT PRIVILEGES (미래 객체 자동 grant)
- `search_path = tenant_N` 고정
- **`REVOKE CREATE ON SCHEMA public`** — public 실수 생성 차단 (USAGE 만 유지)

→ `t_N_user` 는 물리적으로 `tenant_N` 밖 데이터 접근 불가. 앱 계층(`filter_by_tenant` +
`_validate_*_ownership`)과 함께 애플리케이션·PG 권한 이중 격리.

---

## 요약: TSDB vs RDB

| | TSDB | RDB |
|---|---|---|
| 테이블 | 고정 `time_series_data` (LIKE 로 사전 생성) | 동적 (사용자 테이블명, sink/import 생성) |
| 스키마 | 항상 동일 롱 스키마 | auto(고정 롱) / flatten(동적 와이드) 선택 |
| 컬럼 타입 | value=double, value_str=text 등 정형 | 전부 TEXT |
| 격리 | 스키마(public/tenant_N) + role | 동일 |
| database_name | 항상 `sdl` | 항상 `sdl` |
