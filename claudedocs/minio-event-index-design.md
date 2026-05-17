# MinIO 이벤트 기반 객체 인덱스 전환 설계

작성일: 2026-05-17 · 대상: SR DataLake
배경 문서: [minio-canonical-design.md](minio-canonical-design.md)

## 1. 목표

MinIO 객체 목록 조회를 **"주기적 전체 LIST + 파일 캐시"** 에서
**"MinIO 이벤트 알림 → PostgreSQL 인덱스 테이블"** 로 전환한다.

오브젝트 스토리지의 본질적 이점(대용량 파일을 메타데이터로 관리)을 실제로 활용 —
앱이 직접 LIST 를 돌리지 않고, MinIO 가 변경분(생성/삭제)을 push 하면 인덱스
테이블이 실시간으로 갱신되고 UI 는 그 테이블을 SQL 로 조회한다.

## 2. 현재 구조의 문제

| 항목 | 현재 |
|------|------|
| 데이터 출처 | 30분마다 `list_objects` 전체 LIST (NFS 위 5~10분, collector 8 = 13만+ 객체) |
| 저장 | `/tmp/sdl_minio_cache.json` — 현재 **65MB**, 매 `_cache_put` 마다 전체 재기록 |
| 신선도 | 최대 30분 stale |
| 워밍 | leader 워커 백그라운드 워머 필수 — cold 윈도우, 우선순위 튜닝(0746fb1/ad7b9d4/0a221bd) |
| 조회 | 전체 목록을 메모리 로드 후 Python 에서 필터/정렬/페이지네이션 |
| 컨테이너 재생성 | `/tmp` 볼륨 아님 → 배포마다 cold start |

→ LIST 는 본질적으로 O(전체 객체수). 캐시·워밍은 그 비용을 가린 우회책일 뿐
근본적으로 객체가 늘수록 악화된다.

## 3. 목표 아키텍처

```
객체 생성/삭제 ──> MinIO ──(Bucket Notification)──> POST /api/internal/minio-events
                                                          │
                                          이벤트 핸들러 (upsert/delete)
                                                          ▼
                                                  minio_object 테이블 (PG)
                                                          ▲
   /browse · /status · /stats · /local/minio-status · /local/cleanup-migrated
                          (전부 SQL 조회)
```

- **실시간**: 이벤트 도착 즉시 인덱스 반영 (stale 없음)
- **LIST 제거**: 핫패스에서 MinIO LIST 없음. LIST 는 ①초기 백필 ②주기 reconcile 에만
- **SQL 네이티브**: 필터·정렬·검색·페이지네이션을 인덱스된 테이블에서 처리

## 4. 설계 결정

### D1. 알림 타깃 — Webhook (권장) vs PostgreSQL namespace 직결
MinIO 알림 타깃 후보: PostgreSQL(namespace 포맷, `(key,value)` 직결) / Webhook / MQTT.

**선택: Webhook.** MinIO 가 앱의 엔드포인트로 이벤트를 POST → 앱이 **정규화된
테이블을 직접 제어**한다.
- 근거: `name`·`parent_path`·`ftype`·`extension` 등 파생 컬럼을 앱이 계산 →
  `/browse` 가 인덱스를 그대로 활용. namespace 포맷의 `(key, JSONB value)` 직결은
  JSONB 추출·생성컬럼이 필요해 부자연스럽다.
- 근거: 기존 `file_index`(local_path inbox 인덱스, file_indexer 서비스)와 **동일
  패턴** — "앱이 소유하는 인덱스 테이블". `minio_object` 는 그 MinIO 판.
- MQTT 타깃도 가능하나(파이프라인이 MQTT 사용) webhook 이 전달 보장(`queue_dir`)·
  디버깅이 단순.

### D2. 단일 진실원천 — `minio_object` 테이블
`/status`·`/stats` 합계도 Prometheus 메트릭 대신 **테이블 집계**(`GROUP BY bucket`,
`GROUP BY ftype`)로 통일 — 소스가 하나여서 정합성 보장.

### D3. 점진 전환 + 폴백 플래그
전환 중에는 `MINIO_INDEX_MODE`(`cache`|`table`) 플래그로 신/구 경로를 토글 →
테이블이 정상 채워짐을 검증한 뒤 컷오버, 문제 시 즉시 롤백.

## 5. 스키마 — `minio_object`

`file_index` 와 의도적으로 동일한 형태:

```
minio_object
  id            BIGSERIAL   PK
  bucket        VARCHAR(63)   NOT NULL
  object_name   VARCHAR(1024) NOT NULL          -- 전체 키
  name          VARCHAR(512)  NOT NULL          -- basename
  parent_path   VARCHAR(1024) NOT NULL DEFAULT '' -- 디렉토리 prefix (끝 '/' 없음)
  ftype         VARCHAR(20)   DEFAULT ''         -- _classify() 결과
  extension     VARCHAR(20)   DEFAULT ''
  size          BIGINT        DEFAULT 0
  etag          VARCHAR(64)   DEFAULT ''
  content_type  VARCHAR(160)  DEFAULT ''
  last_modified TIMESTAMP                        -- 이벤트의 객체 시각
  event_seq     VARCHAR(40)   DEFAULT ''         -- MinIO sequencer (순서 가드)
  indexed_at    TIMESTAMP     DEFAULT now()
  UNIQUE (bucket, object_name)                   -- upsert 충돌 키
  INDEX  (bucket, parent_path)                   -- 디렉토리 레벨 browse
  INDEX  (bucket, ftype)                         -- 유형 필터/통계
  INDEX  (bucket, last_modified DESC)            -- 정렬
  INDEX  (bucket, object_name varchar_pattern_ops) -- prefix LIKE (import/{cid}/%)
```

- 모델: `backend/models/minio_object.py`, `database.py` 자동 생성 등록
- 거대 캐시 파일 제거 — 16만 행은 PG 에서 인덱스로 ms 단위 조회

## 6. 신규 컴포넌트

### 6.1 이벤트 수신 엔드포인트 — `POST /api/internal/minio-events`
- `backend/routes/internal.py` (신규 blueprint) 또는 `storage_file.py`
- **인증**: 세션 없음(MinIO 호출) → `require_login` 화이트리스트에 추가 +
  `Authorization: Bearer <token>` 검증 (MinIO `auth_token` 과 공유 비밀, env
  `MINIO_WEBHOOK_TOKEN`). 토큰 불일치 → 401.
- 본문: S3 이벤트 JSON `{"Records": [...]}`. 레코드별:
  - `eventName` `s3:ObjectCreated:*` → upsert (`ON CONFLICT (bucket,object_name)`)
  - `eventName` `s3:ObjectRemoved:*` → delete
  - 키는 URL 인코딩 → `urllib.parse.unquote_plus`
  - `name`/`parent_path`/`ftype`/`extension` 는 키에서 파생 (`file_indexer._classify` 재사용)
- **응답은 빠르게 200** — 무거운 작업 금지. `Records` 배열을 한 트랜잭션에 일괄 처리.
- 순서 가드: 같은 `object_name` 의 기존 행 `event_seq` 가 더 크면 skip (out-of-order
  방어). 불확실분은 reconcile 가 정정.

### 6.2 초기 백필 — `minio_indexer.backfill()`
- 1회성: 각 버킷 `list_objects(recursive=True)` → `minio_object` 일괄 INSERT
- 현재의 느린 LIST 와 동일 비용이지만 **딱 한 번** 실행 (운영 명령 또는 관리 라우트)
- 웹훅이 켜진 **후** 실행 — 백필 도중 도착한 이벤트와 겹쳐도 upsert 라 안전

### 6.3 주기 reconcile — `minio_indexer.reconcile()`
- 누락 이벤트(앱 다운 중 발생 등) 대비 안전망
- 저빈도(예: 1일 1회 야간) 전체 LIST → 테이블과 diff → 추가/삭제 정정
- file_indexer 스케줄러에 합류하거나 별도 저빈도 스케줄

## 7. MinIO 설정 (인프라)

`deploy/docker-compose.yml` 의 minio 서비스 env (IDENTIFIER=`PRIMARY`):

```yaml
environment:
  MINIO_NOTIFY_WEBHOOK_ENABLE_PRIMARY: "on"
  MINIO_NOTIFY_WEBHOOK_ENDPOINT_PRIMARY: "http://sdl-app:5001/api/internal/minio-events"
  MINIO_NOTIFY_WEBHOOK_AUTH_TOKEN_PRIMARY: "Bearer ${MINIO_WEBHOOK_TOKEN}"
  MINIO_NOTIFY_WEBHOOK_QUEUE_DIR_PRIMARY: "/data/.minio-notify-queue"
  MINIO_NOTIFY_WEBHOOK_QUEUE_LIMIT_PRIMARY: "100000"
```
- `QUEUE_DIR` — 앱이 일시 불가(배포·재시작)일 때 이벤트를 디스크에 적재 후 재전송.
  MinIO 데이터 볼륨 내 경로로 지정 → MinIO 재시작에도 보존.
- 버킷별 이벤트 구독 (1회, `mc`):
  ```
  mc event add sdl/sdl-files  arn:minio:sqs::PRIMARY:webhook --event put,delete
  mc event add sdl/sdl-archive arn:minio:sqs::PRIMARY:webhook --event put,delete
  mc event add sdl/sdl-backup arn:minio:sqs::PRIMARY:webhook --event put,delete
  ```
  (`put` = `s3:ObjectCreated:*`, `delete` = `s3:ObjectRemoved:*` 포함)

## 8. 소비처 전환

| 엔드포인트 | 현재 | 전환 후 (SQL) |
|-----------|------|--------------|
| `/browse` | `_minio_browse_cached` 메모리 필터/정렬 | `SELECT … WHERE bucket=:b [AND ftype][AND name ILIKE] ORDER BY last_modified DESC LIMIT/OFFSET` |
| `/status` 합계 | `_minio_bucket_summary_cached` | `SELECT bucket, COUNT(*), SUM(size) GROUP BY bucket` |
| `/stats` | browse 캐시 type 집계 | `SELECT ftype, COUNT(*), SUM(size) GROUP BY ftype` |
| `/local/minio-status` | `_minio_import_objects` LIST | `SELECT object_name,size WHERE bucket=:b AND object_name LIKE 'import/'||:cid||'/%'` |
| `/local/cleanup-migrated` | 동상 | 동상 — `_import_match_*` 헬퍼는 SQL 결과로 그대로 구동 |

`warming` 상태·재시도·배지 지연 표시 등 프론트 우회 로직도 함께 정리(테이블은 항상 즉답).

## 9. 제거 대상

전환 완료 후 삭제:
- `storage_file.py`: `_cache_get`/`_cache_put`, `_minio_browse_cached`,
  `_minio_bucket_summary_cached`, `_minio_import_objects`/`_warm_import_objects`,
  `/tmp/sdl_minio_cache.json` 일습, `IMPORT_WARM_REFRESH_AGE`
- `file_indexer.py`: `_warmup_minio_cache`/`_warmup_browse_summary`/
  `_warmup_import_objects` (file_indexer 의 local_path inbox 인덱싱 역할은 유지)

## 10. 마이그레이션 단계

| Phase | 작업 | 비고 |
|-------|------|------|
| 1 | `minio_object` 모델 + 마이그레이션 | 무중단 |
| 2 | 웹훅 엔드포인트 + 이벤트 핸들러 + `MINIO_INDEX_MODE=cache` 배포 | 아직 미사용 |
| 3 | MinIO 웹훅 설정 + `mc event add` | 이벤트가 테이블로 흐르기 시작 |
| 4 | `backfill()` 1회 실행 | 기존 16만+ 객체 적재. **3 이후** 실행 |
| 5 | 테이블 검증 후 `MINIO_INDEX_MODE=table` 컷오버 | 소비처가 테이블 조회 |
| 6 | `reconcile()` 스케줄 추가 | 드리프트 안전망 |
| 7 | 캐시·워머 코드 제거 | 안정 확인 후 |

**순서 주의**: 웹훅(3)을 백필(4)보다 먼저 — 백필 도중 이벤트 누락 방지.
컷오버(5)는 백필 완료 + 행 수 검증 후.

## 11. 엣지 케이스 / 리스크

- **이벤트 누락**: 앱 다운 중 발생분 → MinIO `queue_dir` 가 재전송으로 1차 완화,
  `reconcile()` 가 2차 보정. at-least-once 전제.
- **버스트**: 대량 import(수천 파일) 시 이벤트 폭주 → 핸들러는 `Records` 배열
  일괄 upsert 로 가볍게, `queue_dir` 가 평활화. 필요 시 큐잉 도입.
- **순서 역전**: `event_seq` 가드 + reconcile.
- **버킷 버저닝**: 현재 미사용 가정 — 사용 시 `DeleteMarkerCreated` 등 추가 처리 필요(확인 요).
- **멀티파트 업로드**: 완료 시 `CompleteMultipartUpload` 이벤트 1건 — `put` 구독에 포함.
- **앱→MinIO 자체 쓰기**(import 적재)도 이벤트 발생 → 인덱스 자동 정합.
- **웹훅 미도달 배포 윈도우**: `queue_dir` 필수. 미설정 시 배포 중 이벤트 영구 손실.

## 12. 이점 요약

- LIST 가 핫패스에서 사라짐 — 객체가 늘어도 조회 성능 불변(인덱스 조회)
- 65MB 캐시 파일·워머·cold 윈도우·30분 stale 전부 제거
- 실시간 정합 — 배지/정리/리스트가 즉시 최신
- `/browse` 가 서버측 필터·정렬·검색·페이지네이션 (메모리 풀로드 제거)
- `file_index`(local) 와 `minio_object`(MinIO) 가 동일 "DB 인덱스" 패턴으로 통일

## 13. 작업 규모 (개략)

소규모 config 변경이 아닌 **다중 단계 기능 작업**:
- 백엔드: 모델 1, 라우트/핸들러 1, 백필·reconcile 서비스 1, 소비처 5곳 전환
- 인프라: docker-compose env + `mc event add` (운영/스테이징 각각)
- 검증: 백필 행수 대조, 이벤트 정합, reconcile diff
권장: Phase 1~4 를 먼저 배포해 테이블을 그림자(shadow)로 채우며 정합을 관찰한 뒤
Phase 5 컷오버.

## 14. 구현 상태 (2026-05-17)

Phase 1~7 모두 staging·KETI prod 배포·검증 완료. 커밋 `6120768` 기준 구 캐시/워머
코드 제거까지 끝나, MinIO 객체 조회는 전적으로 `minio_object` 이벤트 인덱스로 동작.

| Phase | 내용 | 커밋 |
|-------|------|------|
| 1·2 | minio_object 모델 + 웹훅 수신·handle_events | f3eef67 |
| 3 | MinIO Bucket Notification 설정 (webhook 타깃 + 버킷 구독) | 인프라 |
| 4 | backfill — 기존 163,525 객체 적재 | 183a5db |
| 5 | 소비처 컷오버 (/browse·/status·/stats·이관여부·정리) | 01c226d |
| 6 | reconcile 드리프트 안전망 (file_indexer 스케줄, ~1일) | 887b15b |
| 7 | 구 캐시·워머 코드 제거, MINIO_INDEX_MODE 플래그 제거 | 6120768 |

## 15. 운영 안정성 강화 방안

> **상태 (2026-05-17, 커밋 05d278e)**: **P1·P2 적용 완료** (staging·KETI prod 배포·검증).
> P3 미적용. 적용 내역 — reconcile 대량삭제 가드, `GET /minio-index-status`,
> `POST /minio-reconcile`, `run_reconcile()` 상태기록, reconcile 주기 6h,
> queue_dir 영속 볼륨(`minio-notify`).

### 현재 안전장치
- **웹훅 누락**: MinIO `queue_dir` 스테이징·재전송 → 1차, `reconcile()` 약 1일 1회 → 2차
- **이벤트 순서 역전**: `event_seq`(sequencer) 가드
- **핸들러 견고성**: 레코드별 try/except, 배치 단위 트랜잭션 (실패 시 전체 롤백 →
  MinIO 가 재시도, upsert/delete 라 멱등)
- **인덱스 무결성**: `UNIQUE(bucket, object_name)`

### P1 — 우선 보완 권장

**(a) reconcile 대량 삭제 가드** ⚠️
`reconcile()` 는 "MinIO LIST 에 없으면 테이블에서 삭제"한다. `list_objects` 가
예외를 던지면 버킷이 skip 되지만, **예외 없이 빈 결과**가 오면 그 버킷의 인덱스
전체가 삭제될 수 있다. → live 객체 수가 0 이거나 테이블 대비 큰 비율(예: 50%↑)
감소 시 삭제를 중단하고 WARN — transient LIST 이상으로부터 인덱스 보호.

**(b) 인덱스 상태 가시화**
웹훅이 조용히 끊겨도 reconcile(최대 23h) 전까지 드러나지 않는다. →
`GET /api/storage/file/minio-index-status`: 테이블 행수, 마지막 이벤트 수신 시각,
마지막 reconcile 시각·결과(added/removed/updated). reconcile 가 매번 큰 드리프트를
보고하면 = 웹훅이 새는 신호.

### P2 — 권장

**(c) queue_dir 영속화**: 현재 `/tmp/minio-notify` 는 컨테이너 휘발성 — MinIO 재시작
시 대기 이벤트 유실(reconcile 가 보정). 영속 볼륨으로 옮기면 무손실.

**(d) reconcile 경량화·단축**: 23h 는 길다. 버킷별 `COUNT(*)` 만 비교(MinIO Prometheus
`minio_bucket_usage_object_total` vs 테이블)하는 경량 체크를 짧은 주기로 → 차이 크면
전체 reconcile 트리거. 드리프트 감지 지연 단축.

**(e) 수동 reconcile 라우트**: 운영자가 관리 API 로 즉시 트리거 (현재 스케줄만).

### P3 — 문서화·장기
- 버킷 버저닝 활성화 시 `DeleteMarkerCreated` 등 추가 이벤트 처리 필요 (현재 미사용 전제)
- `handle_events`·`reconcile`·`backfill` 자동 테스트 추가
- `queue_limit`(10만) 초과 시 이벤트 드롭 — sdl-app 장기 다운 대비 알림
- `MINIO_WEBHOOK_TOKEN` 회전 절차: minio·sdl-app env 동시 갱신 후 양쪽 재기동
