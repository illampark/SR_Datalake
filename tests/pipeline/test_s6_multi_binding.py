"""
시나리오 6: 다중 소스 바인딩 + 태그 필터링
[OPC-UA] + [Modbus] + [MQTT] → Normalize → Filter → [TSDB Sink]

목적: 서로 다른 커넥터 소스의 메시지를 하나의 파이프라인에서 처리 + 태그 필터 와일드카드 검증
"""
import re
import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


def match_tag_filter(tag_name, tag_filter):
    """태그 필터 와일드카드 매칭 (파이프라인 바인딩 시뮬레이션)
    * → 전체, prefix_* → prefix로 시작하는 태그
    """
    if tag_filter == "*":
        return True
    # 와일드카드를 정규식으로 변환
    pattern = "^" + re.escape(tag_filter).replace(r"\*", ".*").replace(r"\?", ".") + "$"
    return bool(re.match(pattern, tag_name))


class TestTagFilterMatching:
    """태그 필터 와일드카드 매칭 로직 테스트"""

    def test_wildcard_all(self):
        """* 필터: 모든 태그 매칭"""
        assert match_tag_filter("any_tag", "*") is True
        assert match_tag_filter("temperature_1", "*") is True

    def test_prefix_wildcard(self):
        """temperature_* 필터: temperature_로 시작하는 태그만"""
        assert match_tag_filter("temperature_zone1", "temperature_*") is True
        assert match_tag_filter("temperature_zone2", "temperature_*") is True
        assert match_tag_filter("pressure_main", "temperature_*") is False

    def test_motor_prefix(self):
        """motor_* 필터"""
        assert match_tag_filter("motor_current_01", "motor_*") is True
        assert match_tag_filter("motor_speed", "motor_*") is True
        assert match_tag_filter("valve_position", "motor_*") is False

    def test_exact_match(self):
        """와일드카드 없는 정확한 매칭"""
        assert match_tag_filter("sensor_1", "sensor_1") is True
        assert match_tag_filter("sensor_2", "sensor_1") is False


class TestS6MultiBinding:
    """시나리오 6: 다중 소스 바인딩 파이프라인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float"}},
        {"module_type": "filter", "config": {
            "filterType": "range", "minValue": -100, "maxValue": 500, "action": "drop"
        }},
    ]

    BINDINGS = [
        {"connector_type": "opcua", "connector_id": 1, "tag_filter": "temperature_*"},
        {"connector_type": "modbus", "connector_id": 2, "tag_filter": "motor_*"},
        {"connector_type": "mqtt", "connector_id": 3, "tag_filter": "*"},
    ]

    def _should_process(self, connector_type, connector_id, tag_name):
        """바인딩 기반 처리 대상 여부 판단"""
        for b in self.BINDINGS:
            if b["connector_type"] == connector_type and b["connector_id"] == connector_id:
                return match_tag_filter(tag_name, b["tag_filter"])
        return False

    def test_opcua_temperature_matched(self):
        """OPC-UA temperature_zone1 → 필터 매칭 → 처리"""
        assert self._should_process("opcua", 1, "temperature_zone1") is True
        msg = make_message(tag_name="temperature_zone1", value=85.3, connector_type="opcua", connector_id=1)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 85.3

    def test_opcua_pressure_not_matched(self):
        """OPC-UA pressure_main → 필터 불일치 → 파이프라인 진입 안함"""
        assert self._should_process("opcua", 1, "pressure_main") is False

    def test_modbus_motor_matched(self):
        """Modbus motor_current_01 → 필터 매칭 → 처리"""
        assert self._should_process("modbus", 2, "motor_current_01") is True
        msg = make_message(tag_name="motor_current_01", value=15.2, connector_type="modbus", connector_id=2)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 15.2

    def test_modbus_valve_not_matched(self):
        """Modbus valve_position → 필터 불일치 → 파이프라인 진입 안함"""
        assert self._should_process("modbus", 2, "valve_position") is False

    def test_mqtt_any_matched(self):
        """MQTT any_tag → * 필터 → 전부 매칭"""
        assert self._should_process("mqtt", 3, "any_tag_name") is True
        msg = make_message(tag_name="any_tag_name", value=42.0, connector_type="mqtt", connector_id=3)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 42.0

    def test_unified_tsdb_source_tracking(self):
        """통합 TSDB 저장 시 source 정보로 출처 구분 가능"""
        msgs = [
            make_message(tag_name="temperature_zone1", value=85.3, connector_type="opcua", connector_id=1),
            make_message(tag_name="motor_current_01", value=15.2, connector_type="modbus", connector_id=2),
            make_message(tag_name="humidity_1", value=65.0, connector_type="mqtt", connector_id=3),
        ]
        results = [run_pipeline_chain(m, self.STEPS) for m in msgs]
        sources = [r["source"]["connectorType"] for r in results if r]
        assert "opcua" in sources
        assert "modbus" in sources
        assert "mqtt" in sources

    def test_out_of_range_dropped(self):
        """범위 초과값은 어떤 소스든 drop"""
        msg = make_message(tag_name="temperature_zone1", value=999.0, connector_type="opcua", connector_id=1)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None  # filter drop

    def test_negative_out_of_range_dropped(self):
        """음의 범위 초과값 drop"""
        msg = make_message(tag_name="motor_current_01", value=-200, connector_type="modbus", connector_id=2)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None
