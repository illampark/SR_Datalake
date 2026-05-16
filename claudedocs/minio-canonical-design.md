# MinIO 정본화 설계 — local_path 중복 저장 제거

작성일: 2026-05-16 · 대상: SR DataLake

## 1. 목표와 원칙

**원칙**: MinIO 가 데이터레이크의 유일한 정본(canonical) 저장소다.
`source_mode`(upload / local_path)는 **파일이 레이크로 들어오는 경로**만 결정하며,
import 가 끝난 뒤에는 어떤 모드로 들어왔든 파일의 위치·조회·집계가 동일해야 한다.

→ import 완료 후 `local_path` 디렉토리는 **정본이 아니라 "가져오기 대기함(inbox)"** 일 뿐이다.

## 2. 현재 local_path 접점 전수 인벤토리

### A. 수집 실행 (ingestion)
| 위치 | 역할 |
|------|------|
| `import_parser.scan_local_path()` :371 | NFS walk → 파일 목록 |
| `import_parser.start_import_from_path()` :443 | NFS 파일 읽어 import, **소스 미삭제** |
| `import_parser._execute_import_direct()` :750 | target=file → MinIO `import/{cid}/{YYYYMMDD}/{f}` (날짜키=재실행 시 중복) |
| `import_parser._execute_import_files()` :216 | files 타입 → MinIO `import/{cid}/{f}` (결정적 키 — 양호) |
| `collector_import.py` IMP-013 scan-path :659 / IMP-014 execute-path :700 | API |

### B. file_index 캐싱 (NFS browse 가속기)
| 위치 | 역할 |
|------|------|
| `file_indexer.py` | 5분 주기로 전체 local_path collector NFS walk → `file_index` upsert |
| `file_index` / `file_index_state` 테이블 | NFS 디렉토리 스냅샷 |
| `app.py:124-125` | 인덱서 기동 |

### C. 스토리지 UI — NFS 파일을 "저장된 데이터"로 표시 (중복 회계)
| 위치 | 문제 |
|------|------|
| `storage_file.py` `/status` :208 | `total = minio + localPath` → **2배 계상** |
| `storage_file.py` `/stats` :426 | type 차트에 `file_index` 합산 → **2배 계상** |
| `/local/collectors` :972, `/local/browse` :1023, `/local/index-state` :1166, `/local/reindex` :1182, `/local/download` :1211, `/local/preview` :1231, `/local/raw` :1276, `/local/delete-batch` :1301 | NFS 파일 별도 트리 |

### D. 카탈로그 browse — NFS 파일 표시
| 위치 | 문제 |
|------|------|
| `catalog.py:1974-1980` | import+local_path 면 강제로 `_list_local_path_files` 분기 |
| `catalog.py._list_local_path_files()` :2253 | `file_index`(NFS) 조회, `bucket:"(local)"`, `isLocal:true` |
| `import_parser._register_catalog()` | target=file → `access_url=s3://...import/{cid}/` (이미 MinIO 지정) → **메타데이터(MinIO)와 browse(NFS) 불일치** |

### E. 프론트엔드
`templates/storage/file_storage.html`, `templates/pipeline/catalog.html`,
`templates/collection/import_collector.html`, `static/i18n/{ko,en}.json`

## 3. 핵심 설계 결정

### D1. import 성공 후 소스 정리 — `post_import_action`
`ImportCollector` 에 컬럼 추가:
- `post_import_action VARCHAR(20) DEFAULT 'keep'` — `keep` / `archive` / `delete`
- `archive_subdir VARCHAR(200) DEFAULT '.imported'`

동작(파일 단위, **성공 시에만**):
- `keep` — 미삭제(레거시 호환). import 시 중복 경고 1회 로그.
- `archive` — `{local_path}/{archive_subdir}/{rel_path}` 로 이동(구조 보존). **권장 기본값(UI)**.
- `delete` — `os.remove()`.

마이그레이션 컬럼 기본값은 `keep`(기존 행 동작 불변), **신규 collector 생성 UI 는 `archive` 기본 + 안내문**.

### D2. 결정적 객체 키 — 재import 중복 제거
`_execute_import_direct` 의 키를 `import/{cid}/{YYYYMMDD-run}/{f}` →
`import/{cid}/{source_mtime_date}/{rel_path}` 로 변경.
- 같은 파일 재import → 같은 키 → 덮어쓰기(중복 아님)
- 날짜 구조 유지(소스 파일 mtime 기준 — 실행일 아님)
- 크래시(업로드 성공 후 정리 전 종료) 시에도 재실행이 덮어쓰기라 안전

### D3. 회계 분리 — local_path 는 스토리지 합계에서 제외
- `/status`: `localPath` 를 `totalUsedGB`/`totalObjects` 에서 제거. 별도 `inbox: {sizeBytes, objectCount}` 정보 필드로만 노출.
- `/stats`: `file_index` 집계 블록 제거. type 차트 = MinIO 전용.

### D4. 카탈로그 browse 통일
`catalog.py:1977` 의 `source_mode=='local_path'` 강제 분기 제거 →
local_path+file collector 도 upload collector 와 동일하게 MinIO `import/{cid}/` browse.
`_list_local_path_files` 는 제거하지 않고 **"가져오기 대기" 탭 전용**으로만 유지.

### D5. file_index 역할 재정의
인덱서는 유지하되 목적이 "스토리지 티어 캐시" → "**import inbox 인덱스**" 로 변경.
- `archive_subdir` 는 walk 에서 제외 (scan_local_path + file_indexer 양쪽).
- `file_index` 데이터는 inbox 뷰에만 사용, 스토리지 합계엔 절대 미반영.
- 소스가 import 후 사라지므로 inbox 는 소규모 → NFS 부하 자연 감소.

## 4. 스키마 변경

`database.py` `_migrate_add_columns` 에 추가:
```python
("import_collector", "post_import_action", "VARCHAR(20)", "'keep'"),
("import_collector", "archive_subdir",     "VARCHAR(200)", "'.imported'"),
```
`models/collector.py` `ImportCollector` 컬럼 + `to_dict()` 키(`postImportAction`, `archiveSubdir`) 추가.

## 5. 변경 작업 목록 (Phase 별)

### Phase 1 — 수집 라이프사이클 (물리 중복 제거 · 핵심)
1. `models/collector.py` — 컬럼 2개 + to_dict.
2. `database.py` — 마이그레이션 엔트리 2개.
3. `import_parser._execute_import_direct()` — D2 결정적 키.
4. `import_parser` 신규 헬퍼 `_apply_post_import_action(collector, full_path, rel_path)` — keep/archive/delete.
5. `import_parser.start_import_from_path()` — 루프 **재구성**: 파일 단위로 `import → MQTT publish → post_import_action` 순서. (현재 MQTT publish 가 루프 후 파일 재read 라 삭제와 충돌 → 루프 내 이동 필수.)
6. `import_parser.start_import()` files/structured 분기 — `files` 는 메모리 보유라 import+publish 후 일괄 정리, `local_path` 만 해당.
7. `import_parser._execute_import_files()` — 키는 양호, post_import_action 만 연결.
8. `import_parser.scan_local_path()` — `archive_subdir` walk 제외.
9. `collector_import.py` create/update API — `postImportAction`/`archiveSubdir` 수용.

### Phase 2 — 회계 정정
10. `storage_file.py` `/status` — localPath 합산 제거, `inbox` 필드로 분리.
11. `storage_file.py` `/stats` — `file_index` 블록 제거.

### Phase 3 — 카탈로그 통일
12. `catalog.py:1977` 강제 분기 제거 — local_path+file → MinIO browse.
13. `_list_local_path_files` — inbox 탭 전용 엔드포인트로 유지(또는 `/local/browse` 로 일원화).

### Phase 4 — file_index 재정의
14. `file_indexer.py` / `file_index.py` docstring 갱신, `archive_subdir` 제외.
15. `_warmup_minio_cache` 는 유지(MinIO browse 캐시는 정본 가속에 여전히 필요).

### Phase 5 — 프론트엔드
16. `import_collector.html` — `post_import_action` 셀렉터(keep/archive/delete) + 안내.
17. `file_storage.html` — "로컬 경로" 섹션을 "가져오기 대기(Inbox)" 로 라벨, 사용량 게이지 미합산.
18. `catalog.html` — local browse 를 inbox 탭으로 라벨.
19. `i18n/{ko,en}.json` — 신규 키.

### Phase 6 — 기존 중복 정리 (선택)
기존 `keep` collector 는 NFS·MinIO 양쪽에 이미 복사본 존재.
수동 액션 "이미 가져온 파일 정리": NFS 파일의 `basename+size` 가 `import/{cid}/` 하위
MinIO 객체와 매칭되면 archive/delete. 매칭 불확실분은 보존.

## 6. 엣지 케이스 / 안전성

- **부분 실패**: 파일 단위 처리 — 실패 파일 소스는 미정리(재시도 가능).
- **크래시 안전**: D2 결정적 키 → 업로드 후 정리 전 종료해도 재실행이 덮어쓰기.
- **MQTT publish 순서**: post_import_action 보다 **먼저** 발행(소스 read 필요).
- **archive 재import 방지**: `archive_subdir` 를 scan/index walk 모두에서 제외.
- **target=tsdb/rdbms + local_path**: MinIO 복사본 없음(행으로 변환). post_import_action 은 동일 적용(소비된 소스 정리). 회계상 이미 데이터는 TSDB/RDBMS — Phase 2 로 localPath 미합산되어 정합.
- **공유 디렉토리(타 팀 소유 MES/HMI export)**: `keep` 유지 가능 — UI 에 "중복 발생" 경고 명시.

## 7. 마이그레이션 / 롤아웃

1. 스키마 자동 마이그레이션(기본 `keep`) → 기존 동작 불변, 무중단.
2. Phase 1~4 배포 — 신규/수정 collector 부터 archive 효과.
3. Phase 5 UI 배포.
4. Phase 6 는 운영자가 collector 별 수동 실행.

## 9. 구현 상태 (2026-05-16)

Phase 1~5 구현·검증 완료. Phase 6(기존 중복 정리)은 미구현 — 운영자용 일회성 액션.

| Phase | 파일 | 상태 |
|-------|------|------|
| 1a | `models/collector.py` — post_import_action·archive_subdir + to_dict | ✅ |
| 1b | `database.py` — 마이그레이션 2엔트리 (기본 keep) | ✅ |
| 1c | `import_parser.py` — `_apply_post_import_action`, 결정적 키(소스 mtime), `scan_local_path(exclude_dirs)`, `start_import_from_path` 루프 재구성, `_execute_import_files` 성공목록 반환 | ✅ |
| 1d | `collector_import.py` — create/update/scan-path 신규 필드 (신규=archive 기본) | ✅ |
| 2 | `storage_file.py` — `/status` totals MinIO 전용 + `inbox` 필드, `/stats` file_index 제거 | ✅ |
| 3 | `catalog.py` — local browse 는 `?view=inbox` 시에만, 기본 MinIO | ✅ |
| 4 | `file_indexer.py`·`file_index.py` — archive_subdir 제외 + docstring 재정의 | ✅ |
| 5 | `import_collector.html`(정책 셀렉터), `file_storage.html`(inbox 라벨), i18n ko/en | ✅ |

부수 수정: 정형 import 의 MinIO 객체 키가 basename → 상대경로(`f["path"]`) 로 통일
— 하위 디렉토리 동명 파일 충돌 제거.

검증: 8개 .py `py_compile` OK, i18n JSON 유효, `_apply_post_import_action`·
`scan_local_path(exclude_dirs)` 기능 테스트(archive/delete/keep/재스캔) 통과.

**Phase 6 (미구현)** — 기존 `keep` collector 의 NFS·MinIO 중복분 정리.
`keep`→`archive`/`delete` 전환 시 이미 양쪽에 있는 파일은 다음 import 부터만
정리됨. 과거 적재분은 운영자가 수동 정리하거나 별도 reconcile 액션 필요.

### Phase 6 가시화 (2026-05-16) — 이관 여부 배지
Phase 6 본 정리에 앞서, 파일 스토리지 화면의 local 파일 목록에 'MinIO 이관 여부'
배지를 추가했다 (어떤 파일이 정리해도 안전한지 운영자가 식별).
- `storage_file.py` — `POST /local/minio-status`: `import/{cid}/` 1회 list(TTL 캐시
  `import_objs:` 키, 30분, miss 시 백그라운드 워밍 + `warming` 응답) 후 file_index
  파일을 `rel_path`(+`size`)로 매칭 → `migrated` / `changed`(경로 동일·크기 상이) /
  `not_migrated`. `target=file` collector 만 대상 (tsdb/rdbms 는 MinIO 사본 없음 →
  `applicable:false`). 매칭은 rel 정확 일치 + basename 폴백 — 신규(rel 보존)·
  레거시(basename 키) 양쪽 커버.
- `file_storage.html` — local 행 파일명 옆 배지, `/local/browse`(SQL, 빠름) 후
  현재 페이지 파일만 비동기 질의 + warming 재시도(2.5s ×4).
- i18n ko/en — `minio_*` 키.
### Phase 6 정리 액션 (2026-05-16) — 이관 완료 파일 일괄 정리
배지에 이어, 이관 완료(migrated) 파일을 일괄 정리하는 버튼을 추가했다.
- `storage_file.py` — `POST /local/cleanup-migrated {collectorId, action, dryRun}`:
  collector 전체 `file_index` 를 MinIO `import/{cid}/` 와 매칭해 `migrated` 만 정리.
  `archive`(`{local_path}/{archive_subdir}/` 로 이동) 또는 `delete`. `changed` /
  `not_migrated` 는 절대 건드리지 않음. `dryRun` 으로 확인 다이얼로그용 집계 제공.
  정리된 파일의 `file_index` 행도 삭제해 UI 즉시 정합. 감사 로그 기록
  (`storage.local.cleanup.{action}`).
- `file_storage.html` — 툴바 '이관 완료 정리' 버튼 → dryRun 집계 → 모달에서
  보관/삭제 선택 → 실행. i18n ko/en `cleanup_*` 키.

이로써 Phase 6 의 과거 NFS·MinIO 중복분 정리를 운영자가 collector 단위로 수행 가능.
유의: 매칭은 rel_path(+size) 휴리스틱 — 레거시 import 의 basename 충돌(동명이파일)
가능성은 size 일치로 완화하나 100% 보장은 아니므로 delete 보다 archive 권장.

## 8. 확정된 결정 (2026-05-16)

- **Q1 → archive.** 신규 collector 의 `post_import_action` 기본값 = `archive`(무손실).
  마이그레이션 컬럼 기본값은 `keep`(기존 행 불변), 생성 API 가 미지정 시 `archive` 적용.
- **Q2 → 별도 필드 유지.** `/status`·`/stats` 는 local_path 를 데이터레이크 합계에서
  제외하되 `inbox` 정보 필드로 대기 파일 수/용량을 계속 노출.
- **Q3 → inbox 전용 유지.** `/local/*` 엔드포인트군은 유지하되 의미를
  "import 대기함 관리"로 재정의. `.imported` archive 영역은 제외. 카탈로그 browse 는 MinIO 로 통일.
