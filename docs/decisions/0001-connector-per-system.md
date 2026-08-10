# 0001. 레거시 커넥터를 시스템 단위로 3개 유지한다

- **상태** 채택 (2026-08-10)

## 맥락

`sr_ai_agent/sim/README.md` 는 FDC / MES / VMS 를 각각 별도 커넥터로 등록하라고
규정한다. 세 시스템이 같은 대상을 다른 식별자로 부르는 사일로 상태를 그대로 유지해야
AI Agent 의 MDM 표준화 단계에 할 일이 생기기 때문이다.

그러나 실제 등록은 성격 기준 2개(이벤트 수집 / 참조 데이터)로 돼 있었고, 시스템 경계를
가로지르고 있었다. 왜 달라졌는지 기록이 없어 의도인지 실수인지 판단할 수 없었다.

## 결정

- 커넥터는 **시스템 단위 3개** — `FDC 설비계측`(schema fdc) / `MES 생산실행`(mes) /
  `VMS 비전검사`(vms).
- 파이프라인은 **태그별 8개**로 분리한다. 한 파이프라인의 sink 는 그 파이프라인에
  들어온 모든 태그를 받으므로, 적재 테이블을 소스 테이블 단위로 나누려면
  바인딩의 `tag_filter` 로 파이프라인을 쪼개는 방법뿐이다.
- 컬럼 매핑은 `flatten` 을 기본으로 하되, 소스에 `id` 컬럼이 있는 FDC 두 테이블은
  `auto` 를 쓰거나 customSql 에서 `id AS src_id` 로 별칭한다.
  sink 가 `CREATE TABLE (id SERIAL PRIMARY KEY, <소스컬럼>)` 을 만들기 때문에
  소스에 `id` 가 있으면 `column "id" specified more than once` 로 깨진다.

## 기각한 대안

**성격 기준 2개(이벤트 / 참조)로 묶기.** VMS 의 `inspection` 이 이벤트 커넥터로,
`defect_code` 가 참조 커넥터로 흩어져 시스템 경계가 무너진다. 사일로가 사라지면
MDM 표준화 데모의 전제 자체가 성립하지 않으므로 기각.

## 결과

데이터레이크에 8개 테이블 3,393행. 소스 행수와 정확히 일치한다.

    fdc_eqp_vibration 1344 · fdc_eqp_temperature 1344
    mes_lot 56 · mes_work_order 56 · mes_equipment 8 · mes_product 20
    vms_inspection 560 · vms_defect_code 5

README 의 인과 정답도 데이터레이크만으로 재현된다 — 후반기(07-05~07) EQ01 불량률
46.7% · VIB 12건(그 외 설비 0건), FDC 진동 임계 초과는 e00001 만 46회(최대 0.95).
