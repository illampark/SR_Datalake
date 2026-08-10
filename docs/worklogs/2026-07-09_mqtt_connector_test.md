# MQTT 커넥터 테스트 워크로그 (2026-07-09)

## 목적
프로덕션 대덕 테넌트(tenant_id=4)에서 MQTT 커넥터 end-to-end 검증.
`sdl-mosquitto` 브로커에 mqtt-sim 이 publish → MQTT 커넥터가 subscribe → 파이프라인이 tenant_4 스키마 RDBMS 에 적재.

## 플랫폼 변경사항

### 1. `MqttTag.json_path` 컬럼 추가
- 마이그레이션: `backend/migrations/versions/20260709_0014_mqtt_tag_json_path.py`
- 다중 필드 JSON payload 를 태그별로 파싱하기 위한 dot-notation 경로 (`$.temperature` 등)
- 빈 값이면 payload 전체를 값으로 사용 (기존 동작 유지)

### 2. Benthos MQTT stream mapping 확장
- `backend/services/benthos_manager.py:build_mqtt_stream_config`
- 기존: `root = this` → payload 를 body 로 노출
- 신규: `root._meta`, `root._raw_str = content().string()`, `root._raw_json = content().parse_json().catch(null)` — payload 를 원본 문자열과 파싱된 JSON 둘 다로 노출

### 3. MQTT 콜백 라우트 리팩터
- `backend/routes/collector_mqtt.py:message_callback`
- 기존: message_count 갱신만
- 신규: `MqttTag` 순회 → 토픽 match (`#`, `+` wildcard) → `json_path` 파싱 → `mqtt_manager.publish_raw("mqtt", cid, tag_name, value)` 로 `sdl/raw/mqtt/{cid}/{tag}` 발행 → 파이프라인이 처리 가능

### 4. inject_tenant 누락 fix (추가 커밋)
- `create_tag` 에 `inject_tenant(tag)` 호출 누락으로 `MqttTag.tenant_id` 가 기본 1 로 저장되던 회귀 수정

## 커밋
- `c8eb25d` mqtt: MqttTag.json_path + callback fan-out (sdl/raw/mqtt publish per tag)
- `7416de2` mqtt: create_tag inject_tenant() 누락 fix

## 배포
- 이미지 태그: `sdl-app:mqtt-jsonpath` (프로덕션에 `sdl-app:latest` 로 재태그)
- 마이그레이션: `0013_uniq_membership_user` → `0014_mqtt_tag_json_path`
- sdl-app 재기동 후 헬스체크 통과

## mqtt-sim 컨테이너
- 이미지: `mqtt-sim:latest` (python:3.11-slim + paho-mqtt 1.6.1)
- 컨테이너: `mqtt-sim`, 네트워크 `sdl_keti_deploy_sdl-net`, restart unless-stopped
- 2초 간격 `sdl/sim/daeduck/A` 로 `{"temperature": ..., "humidity": ..., "pressure": ...}` publish

## 시나리오 결과

### S1 — dd3 로 커넥터·태그 등록 + 활성화
- 커넥터 id=1 `daeduck-mqtt-sim` (host=sdl-mosquitto, topic=sdl/sim/daeduck/#, tenant_id=4)
- 태그 3건: temperature/humidity/pressure — 각각 jsonPath `$.<name>`
- 활성화 후 `mosquitto_sub -t "sdl/raw/mqtt/1/#"` 로 3태그 발행 확인
  - 예: `sdl/raw/mqtt/1/temperature {"value": 21.92, "dataType": "float", ...}`

### S2 — 파이프라인 end-to-end
- 파이프라인 id=28 `daeduck-mqtt-to-rdbms`: `mqtt_source(connectorId=1)` → `internal_rdbms_sink(rdbmsId=4, tableName=mqtt_sim_data)`
- 실행 12초 후 `tenant_4.mqtt_sim_data` 에 3태그 × 6행 적재
  - temperature: 21.74–22.16
  - humidity: 57.3–58.47
  - pressure: 1015.8–1016.26

### S3 — 격리 검증
- tenant_id=1 로 cross-probe 커넥터 삽입
- dd3 세션 `GET /api/connectors/mqtt` → probe 미노출 (자기 tenant 것 1건만)
- dd3 세션 `GET /api/connectors/mqtt/2` → NOT_FOUND 로 정상 차단
- 정리 후 probe 삭제

### S4 — 잘못된 host
- `PUT` 로 host=`no-such-host` 변경 후 start
- Benthos stream 은 재시도 loop 로 active 유지 → 커넥터 status 도 running
- `lastError` 는 빈 값 — Benthos 내부 로그로만 남고 앱 레이어에 전파 안 됨
- **알려진 UX 한계**: 사용자가 Benthos 커넥트 실패를 알 수 없음. 향후 개선 대상.

## 정리 상태
- mqtt 커넥터 1: host 원상복구(sdl-mosquitto), status=stopped
- 파이프라인 28: stopped
- mqtt-sim 컨테이너: 지속 실행 유지 (사용자 요청)
- 대덕 OPC-UA pipeline id=26: **stopped 유지** (사용자가 수동 재시작 예정)

## 알려진 이슈
1. S4 lastError 미전파 — Benthos → 커넥터 상태 반영 파이프 부재.
2. `benthos_active` 필드 등 UI 는 이번 세션에서 별도로 제거하지 않았음(MQTT 는 실제 Benthos 사용 중이므로 유효한 상태값).

## 후속 작업 — asset_id + source timestamp 전파 (2026-07-09)

### 배경
초기 MQTT 검증에서 payload 는 `{"temperature", "humidity", "pressure"}` 뿐이었고
저장 시 asset 구분·설비 시각이 남지 않았음. 사용자가 D 옵션(플랫폼 스키마 확장)을 선택.

### 결정
| 항목 | 값 |
|---|---|
| asset_id 설정 단위 | 커넥터 레벨 (`MqttConnector.config.assetIdJsonPath`) |
| source timestamp | 커넥터 레벨 (`MqttConnector.config.timestampJsonPath`) |
| 적용 범위 | MQTT 만 (OPC-UA/Modbus/File/DB/API 등은 다음 안건) |
| 저장 위치 | RDBMS sink `asset_id` 컬럼(auto DDL) + TSDB `tags` JSON(자동 병합) |
| 마이그레이션 | **불필요** — sink 자동 스키마 진화 + TSDB tags 컬럼 재사용 |

### 코드 변경
1. `mqtt_manager.build_raw_message()` — `source_timestamp`, `asset_id` kwarg 추가.
   결과: `source.assetId` 필드 노출 + `timestamp` 필드가 source ts 우선.
2. `collector_mqtt.message_callback` — `connector.config.assetIdJsonPath` / `timestampJsonPath` 파싱.
3. `pipeline_modules.sink_internal_rdbms` 표준 스키마 row 에 `asset_id` 추가 (신규 컬럼 auto DDL 로 반영).
4. TSDB sink 는 이미 `source` 의 잔여 필드를 `tags` JSON 으로 병합 → assetId 자동 저장 (코드 변경 0).
5. `create_connector` / `update_connector` body 에 `assetIdJsonPath` / `timestampJsonPath` 필드 수용.

### 커밋
- `734c3b0` mqtt: source assetId + source timestamp 전파
- `90ee80b` mqtt: create/update route 에 assetIdJsonPath/timestampJsonPath 필드 지원

### mqtt-sim v2
```json
{"asset_id": "PRESS-01",
 "timestamp": "2026-07-09T03:47:40.367323Z",
 "temperature": 22.07,
 "humidity": 61.65,
 "pressure": 1013.82}
```
- 이미지: `mqtt-sim:v2` (기존 v1 재빌드)
- `ASSET_ID` env 로 asset 이름 변경 가능

### 검증 결과
- `sdl/raw/mqtt/1/temperature` 메시지: `source.assetId="PRESS-01"`, `timestamp=<sim 생성 시각>` (수집 시각이 아닌 설비 시각)
- `tenant_4.mqtt_sim_data` 스키마: **`asset_id` 컬럼 자동 추가**됨 (auto DDL 확인)
- 168행 적재, `asset_id=PRESS-01` 전부, `collected_at` = source timestamp
- 다른 계층 (OPC-UA/Modbus) 은 이전과 동일 동작 유지 (회귀 없음)

### 사용법 (API)
```json
POST /api/connectors/mqtt
{
  "name": "...",
  "host": "sdl-mosquitto",
  "port": 1883,
  "topics": ["sdl/sim/daeduck/#"],
  "assetIdJsonPath": "$.asset_id",
  "timestampJsonPath": "$.timestamp"
}
```
`assetIdJsonPath` / `timestampJsonPath` 가 비어있으면 기존 동작 (asset_id="", timestamp=수집 시각).

### 정리 상태
- pipeline 28 / connector 1 정지 유지
- mqtt-sim v2 컨테이너 지속 실행
- 대덕 OPC-UA pipeline 26 사용자 수동 재개 대상

### 미완 사항
- UI (커넥터 편집 화면) 에 두 필드 표시 — 다음 안건
- 다른 커넥터 (OPC-UA/Modbus/File/DB/API) 에도 asset_id 확장 — 별도 안건

## 후속 작업 — AASX (Asset Administration Shell) 파일 연동 (경량형)

### 배경
사용자가 AAS 표준 파일을 MQTT 커넥터에 붙여 asset_id·설비 메타·태그 후보를 자동으로
채우는 기능을 요청. 옵션 A(경량형) + basyx-python-sdk + V2/V3 둘 다 + 미리보기 후 선택 확정.

### 코드 변경
- `requirements.txt`: `basyx-python-sdk==2.0.1` 추가 (+ `pyecma376-2` 자동 dep)
  - 실제 이미지 증가는 약 200KB 수준 (lxml 은 이미 있음)
- `backend/services/aasx_parser.py` 신설
  - `parse_aasx(file_bytes) -> dict` — AASX ZIP 열기 → shells/submodels/properties/technical_data/digital_nameplate
  - `basyx.aas.adapter.aasx.AASXReader` 사용, V2 XML / V3 JSON 자동 감지
  - Property 재귀 추출 (SubmodelElementCollection 대응, path 는 slash 결합)
  - value_type, unit, semantic_id, description(첫 언어) 을 함께 반환
- `POST /api/connectors/mqtt/aasx-preview`
  - multipart file 업로드 → 파싱 결과 반환 (저장 없음, 커넥터 생성 전에 사용)
- `POST /api/connectors/mqtt/{id}/aasx-apply`
  - MinIO `t-{N}-files/aasx/{cid}.aasx` 저장 (`bucket_for("files", tenant_id=...)` 재사용)
  - `config.aasxObjectKey`, `config.aasMeta` (shells/technicalData/digitalNameplate) 병합
  - `selectedPropertyPaths` 폼 필드로 사용자가 고른 Property 만 `MqttTag` 로 등록
    - tag_name = property.idShort
    - json_path = `$.{path.replace('/', '.')}`
    - data_type = property.value_type (float/int/bool/string)
    - description = 원 description 첫 언어 (500자 clip)
  - `autoAssetId=true` / `autoTimestamp=true` (기본) 시 기존 값이 비어있을 때만
    `assetIdJsonPath = "$.asset_id"` / `timestampJsonPath = "$.timestamp"` 자동 세팅
- **버그 수정**: SQLAlchemy JSON 컬럼은 dict in-place 수정을 감지하지 못하므로
  `dict(c.config or {})` 얕은 복사 후 rebuild 하도록 변경 (첫 검증에서 발견·즉시 fix)

### 커밋
- `a269b2f` mqtt: AASX (Asset Administration Shell) 파일 연동
- `a10ca89` mqtt: aasx-apply config 병합 시 SQLAlchemy JSON mutation tracking 회피

### 검증
IDTA 표준 샘플이 basyx repo 에 pre-built 형태로 없어 **대덕 PRESS_01 미니멀 AASX 를
basyx AASXWriter 로 직접 생성** 후 검증:
- AAS: id_short=`PRESS_01`, global_asset_id=`urn:daeduck:asset:press-01`
- Submodel `OperationalData`: temperature/humidity/pressure (float)
- Submodel `TechnicalData`: Manufacturer=대덕전자, Model=SR-PRESS-01, SerialNumber=SN-0001
- 참고: AAS 표준 제약 (AASd-002) 상 idShort 에 hyphen 사용 불가 → underscore 로 우회

검증 결과:
- **aasx-preview**: shells/submodels/properties 모두 정확히 반환
- **aasx-apply**:
  - `t-4-files/aasx/{cid}.aasx` MinIO 업로드 확인
  - config 병합: `aasxObjectKey`, `aasMeta.shells/technicalData`, `assetIdJsonPath=$.asset_id`, `timestampJsonPath=$.timestamp`
  - MqttTag 3건 자동 등록 (temperature/humidity/pressure, jsonPath 자동 생성, tenant_id=4)
  - semanticId (`0173-1#02-BAA034#003`) 는 response 에는 노출되지만 DB 스키마 저장은 다음 단계

### 정리
- 검증용 커넥터·태그·MinIO 오브젝트 모두 삭제
- 남은 자산: `mqtt-sim v2` 컨테이너 (지속 실행), `daeduck-mqtt-sim` 커넥터 (id=1, stopped)
- 대덕 OPC-UA pipeline 26 stopped 유지 (사용자 수동 재시작)

### 사용 흐름 요약
```
1. 커넥터 준비: POST /api/connectors/mqtt {name, host, port, topics, qos}
2. 미리보기:    POST /api/connectors/mqtt/aasx-preview -F file=@x.aasx
                → shells/submodels/properties 확인
3. 적용:        POST /api/connectors/mqtt/{cid}/aasx-apply
                -F file=@x.aasx
                -F selectedPropertyPaths=["temperature","humidity",...]
                -F submodelIdShort=OperationalData
                -F autoAssetId=true -F autoTimestamp=true
                → config.aasMeta 저장 + MqttTag 자동 등록
```

### 미완 (다음 안건)
- UI (커넥터 편집 화면) 에서 AASX 파일 업로드 인터페이스
- semantic_id 를 MqttTag 컬럼으로 별도 저장 (중간형 스코프)
- 다른 커넥터 (OPC-UA/Modbus) 에도 AASX 연동 확장
- AAS-conformant 발행 형식 (완전형 스코프)
