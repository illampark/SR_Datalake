# 멀티테넌트 마이그레이션 가이드 v1

**Phase 0 산출물 / 작성일: 2026-06-05 / 부모 문서: `multitenant-design-v1.md`**

스키마/데이터/MinIO/MQTT 마이그레이션의 표준 패턴. Phase 3, 5, 6에서 반복 사용한다.

---

## 1. 원칙

| 원칙 | 의미 |
|---|---|
| **단방향 마이그레이션 금지** | 모든 변경은 롤백 스크립트 동반 |
| **백필 우선, 강제 NOT NULL은 마지막** | 큰 테이블 안전 |
| **점진 적용 가능** | 한 테이블 → 검증 → 다음 |
| **Tenant=1 무영향** | 모든 마이그레이션 후 단일테넌트 인스턴스 회귀 0 |
| **Alembic 단일 진실** | ad-hoc SQL 금지, 모든 변경은 Alembic revision |

## 2. Alembic 도입 (Phase 1 즉시)

현재 SDL은 Alembic 미적용 추정. Phase 1 첫 단계로 베이스라인 적용.

```bash
pip install alembic
alembic init backend/migrations
```

`alembic.ini` 설정:
- `script_location = backend/migrations`
- `sqlalchemy.url = <DATABASE_URL>` (env 주입)

`backend/migrations/env.py`:
- `target_metadata = Base.metadata` 매핑
- `compare_type = True` (컬럼 타입 변경도 감지)

**베이스라인 생성**:
```bash
alembic revision --autogenerate -m "baseline_pre_multitenant"
alembic stamp head     # 이미 운영 중인 DB에 stamp만 (실제 ALTER 안 함)
```

이후 모든 스키마 변경은 `alembic revision -m "..."`.

## 3. 표준 패턴 — 도메인 테이블에 `tenant_id` 추가 (Phase 3)

### 3.1 마이그레이션 단계 (3-step)

각 도메인 테이블에 대해:

```python
# backend/migrations/versions/0010_add_tenant_id_pipeline.py
def upgrade():
    # 1) 컬럼 추가 (NULL 허용)
    op.add_column('pipeline',
        sa.Column('tenant_id', sa.BigInteger(),
                  sa.ForeignKey('tenant.id'), nullable=True))

    # 2) 백필 (현 데이터 = tenant 1)
    op.execute("UPDATE pipeline SET tenant_id = 1 WHERE tenant_id IS NULL")

    # 3) NOT NULL + 인덱스
    op.alter_column('pipeline', 'tenant_id', nullable=False)
    op.create_index('ix_pipeline_tenant', 'pipeline', ['tenant_id'])

def downgrade():
    op.drop_index('ix_pipeline_tenant', table_name='pipeline')
    op.drop_column('pipeline', 'tenant_id')
```

### 3.2 큰 테이블 점진 ALTER (락 회피)

`pipeline_run`, `system_log` 등 큰 테이블은 ALTER 시 락 시간이 길 수 있음.

**옵션 A — pg_repack** (가용 시 권장)
- 운영 중 락 최소화, 컬럼 추가도 빠름

**옵션 B — 수동 점진 (DEFAULT 활용)**
```sql
-- PG 11+ : NOT NULL DEFAULT는 즉시(메타데이터만 변경, 락 짧음)
ALTER TABLE system_log ADD COLUMN tenant_id BIGINT DEFAULT 1;
ALTER TABLE system_log ALTER COLUMN tenant_id SET NOT NULL;
-- 이후 인덱스는 CONCURRENTLY로
CREATE INDEX CONCURRENTLY ix_system_log_tenant ON system_log(tenant_id);
```

**옵션 C — 새 테이블 + 트리거 + 스왑** (마지막 수단)
- 큰 다운타임 회피, 운영 복잡도 큼

→ 1차 권장: 옵션 B + 새벽 배포창.

### 3.3 NULL 허용 도메인 (예외)

`system_log`: 시스템 레벨 행은 `tenant_id NULL` 유지.

```python
op.add_column('system_log',
    sa.Column('tenant_id', sa.BigInteger(),
              sa.ForeignKey('tenant.id'),
              nullable=True))    # NULL 허용
op.execute("UPDATE system_log SET tenant_id = 1 WHERE component IN ('pipeline','import','catalog','storage')")
# NOT NULL 생략
```

## 4. 검증 게이트 (각 마이그레이션 후)

```sql
-- 1) 모든 행에 tenant_id 또는 NULL(허용 테이블) 채워졌나
SELECT COUNT(*) FROM pipeline WHERE tenant_id IS NULL;  -- 0이어야 함

-- 2) 기존 데이터 무결성
SELECT COUNT(*) FROM pipeline;  -- 마이그 전후 일치

-- 3) FK 무결성
SELECT COUNT(*) FROM pipeline p LEFT JOIN tenant t ON p.tenant_id = t.id WHERE t.id IS NULL;
-- 0이어야 함

-- 4) 인덱스 효과 확인 (큰 테이블)
EXPLAIN ANALYZE SELECT * FROM pipeline WHERE tenant_id = 1 LIMIT 100;
-- Index Scan 사용해야 함
```

## 5. 롤백 패턴

### 5.1 정상 롤백 (downgrade)

```bash
alembic downgrade -1
```

### 5.2 데이터 손상 시

각 마이그 직전 PG 백업:
```bash
docker exec sdl-postgres pg_dump -U sdl_user -d sdl -f /var/lib/postgresql/data/pre_<rev>.sql
```

복구:
```bash
docker exec sdl-postgres psql -U sdl_user -d sdl -f /var/lib/postgresql/data/pre_<rev>.sql
```

### 5.3 부분 실패 시

- 한 테이블 ALTER 도중 실패 → 그 테이블만 downgrade
- Alembic의 `op.batch_alter_table` 사용 시 트랜잭션 보장

## 6. MinIO 격리 마이그레이션 (Phase 5)

### 6.1 흐름

```
[현재]          sdl-files/import/<collector>/<date>/<file>
                sdl-archive/...
                sdl-backup/...
                sdl-exports/catalog_<id>/...

[목표]          t-1-files/import/<collector>/<date>/<file>
                t-1-archive/...
                t-1-backup/...
                t-1-exports/...

[방법]          1. 새 버킷 생성
                2. mc mirror로 데이터 복사
                3. sdl-app 코드 전환 (bucket_for 함수)
                4. 검증
                5. 옛 버킷 readonly → 일정 기간 후 삭제
```

### 6.2 스크립트 (스테이징 / 프로덕션 동일)

```bash
# 0) 게이트 — 워크로드 0 확인 (별도 호출)
# pipeline_running=0, import_running=0, file_indexer_running=0 확인 후 진행

# 1) 새 버킷
mc mb local/t-1-files local/t-1-archive local/t-1-backup local/t-1-exports

# 2) 미러 (--remove 옵션은 dry-run 후에만)
mc mirror --overwrite local/sdl-files/    local/t-1-files/
mc mirror --overwrite local/sdl-archive/  local/t-1-archive/
mc mirror --overwrite local/sdl-backup/   local/t-1-backup/
mc mirror --overwrite local/sdl-exports/  local/t-1-exports/

# 3) 검증 — 객체 수 + ETag 일치
for b in files archive backup exports; do
  src=$(mc ls --recursive local/sdl-$b | wc -l)
  dst=$(mc ls --recursive local/t-1-$b | wc -l)
  echo "$b: src=$src dst=$dst $([ $src = $dst ] && echo OK || echo MISMATCH)"
done

# 4) sdl-app 전환 (env 또는 코드 배포 — bucket_for(tenant_id, 'files') = 't-{id}-files')

# 5) 옛 버킷 readonly 정책
mc policy set download local/sdl-files
# (소비자가 더 이상 쓰지 않는지 확인 후, 후일 mc rb 삭제)
```

### 6.3 SFTP 사용자 격리 (선택)

현재 SFTP는 MinIO root 사용 — 전 버킷 접근. tenant 격리를 SFTP 단에서도 보장하려면:

```bash
# tenant 1 IAM 사용자 생성
mc admin user add local t1user <strong-secret>

# tenant 1 버킷만 읽기·쓰기 가능 정책
cat > /tmp/t1-policy.json <<EOF
{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:*"],
    "Resource":["arn:aws:s3:::t-1-*","arn:aws:s3:::t-1-*/*"]
  }]
}
EOF
mc admin policy create local t1-policy /tmp/t1-policy.json
mc admin policy attach local t1-policy --user t1user
```

WinSCP는 `t1user`/시크릿으로 접속 — 다른 tenant 버킷 안 보임.

## 7. MQTT 토픽 격리 (Phase 6)

### 7.1 흐름

```
[현재]          plant/etching/temp
                plant/etching/pressure
                ...

[목표]          t/1/plant/etching/temp
                ...

[과도기]        publisher가 양쪽으로 publish, subscriber도 양쪽 구독 (2주 정도)
[전환]          새 prefix만 사용, 옛 토픽 deprecated
```

### 7.2 Mosquitto ACL

`config/mosquitto.acl`:
```
# 글로벌 sdl-app (Webhook 등) — 모든 토픽
user sdl-app
topic readwrite #

# tenant 1 디바이스 — t/1/* 만
user t1device
topic readwrite t/1/#
```

`config/mosquitto.conf`:
```
acl_file /mosquitto/config/mosquitto.acl
allow_anonymous false
password_file /mosquitto/config/passwd
```

### 7.3 디바이스 측 변경 (운영팀 협의)

- 펌웨어 OTA로 토픽 prefix 변경 (`t/1/` 추가)
- 또는 GW(엣지)에서 prefix 자동 추가
- 이중 발행 기간(2주) 동안 호환 유지

## 8. API 키 v2 마이그레이션 (Phase 6)

```python
# alembic
def upgrade():
    op.add_column('api_key',
        sa.Column('tenant_id', sa.BigInteger(), sa.ForeignKey('tenant.id'), nullable=True))
    op.add_column('api_key',
        sa.Column('role', sa.String(30), nullable=False, server_default='tenant_viewer'))
    op.add_column('api_key',
        sa.Column('scopes', postgresql.JSONB(), nullable=False, server_default='[]'))
    op.add_column('api_key',
        sa.Column('key_prefix', sa.String(16), nullable=True))

    # 기존 키 → tenant 1, viewer 유지 (호환)
    op.execute("UPDATE api_key SET tenant_id = 1, role = 'tenant_viewer' WHERE tenant_id IS NULL")
    op.alter_column('api_key', 'tenant_id', nullable=False)

def downgrade():
    op.drop_column('api_key', 'key_prefix')
    op.drop_column('api_key', 'scopes')
    op.drop_column('api_key', 'role')
    op.drop_column('api_key', 'tenant_id')
```

## 9. Schema-per-tenant (Phase 1·3 — L2 처음부터 적용)

부모 문서 § 2.1 D2에 따라 L2는 옵션이 아니라 **필수 시작점**.
Tenant 1은 `public` schema를 그대로 사용. 신규 tenant는 `t_<id>` schema 자동 생성.

### 9.1 신규 Tenant 프로비저닝 흐름 (Phase 7 콘솔에서 호출)

```sql
-- super_admin이 새 tenant 생성 시 자동 실행
BEGIN;
  -- 1) tenant entity 생성
  INSERT INTO tenant(slug, name, status) VALUES ('acme', 'ACME Corp', 'active') RETURNING id;
  -- :new_id

  -- 2) schema 생성
  CREATE SCHEMA t_:new_id;

  -- 3) 모든 도메인 테이블 복제 (구조 + 인덱스 + 제약)
  CREATE TABLE t_:new_id.pipeline           (LIKE public.pipeline INCLUDING ALL);
  CREATE TABLE t_:new_id.pipeline_step      (LIKE public.pipeline_step INCLUDING ALL);
  CREATE TABLE t_:new_id.import_collector   (LIKE public.import_collector INCLUDING ALL);
  CREATE TABLE t_:new_id.data_catalog       (LIKE public.data_catalog INCLUDING ALL);
  -- ... (자동화: tenant_provisioning.sql 템플릿)

  -- 4) FK는 글로벌 테이블로 (예: user, tenant 자체)
  ALTER TABLE t_:new_id.pipeline
    ADD CONSTRAINT fk_pipeline_tenant FOREIGN KEY (tenant_id) REFERENCES public.tenant(id);
COMMIT;

-- 5) MinIO 버킷 동시 생성 (Phase 5)
-- mc mb local/t-:new_id-files local/t-:new_id-archive local/t-:new_id-backup local/t-:new_id-exports
```

→ 전체 자동화 스크립트: `backend/services/tenant_provisioning.py` (Phase 1·3 산출물)

### 9.2 sdl-app — tenant-aware SessionLocal

```python
def get_session(tenant_id):
    s = SessionLocal()
    schema = f"t_{tenant_id}" if tenant_id != 1 else "public"
    s.execute(text(f"SET search_path TO {schema}, public"))
    return s
```

- tenant 1 = `public` (legacy 그대로)
- tenant 2+ = `t_<id>`
- 글로벌 테이블(`tenant`, `user`, `tenant_membership`, `api_key`, `system_log` sys-level)은 `public` 검색 경로에 항상 포함

### 9.3 Alembic 다중 스키마 마이그레이션

새 schema 추가가 잦으므로 `env.py`가 모든 tenant schema 순회:

```python
# env.py
def run_migrations_online():
    for tid in get_all_active_tenant_ids():
        schema = f"t_{tid}" if tid != 1 else "public"
        with engine.begin() as conn:
            conn.execute(text(f"SET search_path TO {schema}, public"))
            context.configure(connection=conn, target_metadata=target_metadata, version_table_schema=schema)
            with context.begin_transaction():
                context.run_migrations()
```

- 각 schema에 alembic_version 테이블 따로 둠 (`version_table_schema=schema`)
- 새 schema가 추가되면 자동으로 다음 마이그에 포함됨
- 마이그 실패 시 부분 적용 방지 (트랜잭션 단위로 schema별 분리)

## 10. 마이그레이션 체크리스트 (PR 머지 전)

```markdown
## Migration PR Checklist
- [ ] Alembic revision 1개만 (여러 마이그를 한 PR에 안 섞음)
- [ ] `upgrade()`와 `downgrade()` 양쪽 동작 검증
- [ ] 로컬에서 `alembic upgrade head` → `alembic downgrade -1` → `alembic upgrade head` 사이클 성공
- [ ] 큰 테이블이면 EXPLAIN으로 ALTER 시간 추정
- [ ] 백필 SQL이 멱등 (재실행 안전)
- [ ] 기존 tenant=1 데이터 무결성 SQL 확인
- [ ] 운영 적용 절차 PR 설명에 기재 (점검창 필요 여부, 백업 명령)
- [ ] 롤백 절차 PR 설명에 기재
```

## 11. 운영 적용 절차 표준

```bash
# 0) 게이트 체크 — 단독 호출, 결과 확인 후 다음 단계
docker exec sdl-postgres psql -U sdl_user -d sdl -c "
  SELECT 'pipeline'  k, COUNT(*) v FROM pipeline WHERE status='running'
  UNION ALL SELECT 'import', COUNT(*) FROM import_collector WHERE status='running'
  UNION ALL SELECT 'indexer', COUNT(*) FROM file_index_state WHERE is_running='t';
"
# 모두 0 확인 후 진행

# 1) 백업
docker exec sdl-postgres pg_dump -U sdl_user -d sdl > backup_pre_<rev>.sql

# 2) 마이그
docker exec sdl-app alembic upgrade head

# 3) 검증
docker exec sdl-postgres psql -U sdl_user -d sdl -f /migrations/post_check_<rev>.sql

# 4) 실패 시 롤백
# docker exec sdl-app alembic downgrade -1
# (또는 백업 복구)
```

## 12. 주의 사항

- **PG vacuum/analyze**: 큰 ALTER 후 ANALYZE 실행 (`VACUUM ANALYZE pipeline;`)
- **인덱스 누락**: tenant_id 컬럼에 인덱스 빠뜨리면 모든 쿼리 느려짐. PR 체크리스트 항목
- **FK cascade 방향**: `tenant.id` 삭제 시 도메인 행 어떻게 할지 결정 (`ON DELETE RESTRICT` 권장 — soft-delete 정책 일관)
- **Alembic 트랜잭션**: PG는 DDL이 트랜잭션 안에서 안전 (MySQL과 다름). 안심하고 한 트랜잭션에 묶어도 됨
- **컨테이너 redeploy 가드**: 마이그가 sdl-app 부팅 시 자동 실행되도록 만들지 말 것 (점검창 통제 우선). 별도 명령으로 분리

## 13. 다음 단계

- Phase 1 첫 작업: Alembic 베이스라인 + `tenant`/`tenant_membership` 테이블 마이그레이션 작성
- 본 가이드 갱신 시점:
  - Phase 3 직전: 도메인 테이블 별 ALTER 예측 락 시간 보강 + schema 자동 프로비저닝 스크립트 확정
  - Phase 5 직전: MinIO 마이그레이션 dry-run 결과 반영
  - 마이그 후행 정리(2027 Q1): 운영 데이터 기반 최적화·재발 패턴 정리
