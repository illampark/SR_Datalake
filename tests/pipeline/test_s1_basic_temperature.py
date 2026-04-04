"""
시나리오 1: 기본 온도 수집 파이프라인
[MQTT Source] → Normalize → Unit Convert → [TSDB Sink]

목적: 가장 기본적인 E2E 데이터 흐름 검증
- Normalize: float 변환, null skip
- Unit Convert: celsius → fahrenheit 내장 변환
- TSDB Sink: 행 데이터 구성 검증
"""
import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS1BasicTemperature:
    """시나리오 1: 기본 온도 수집 파이프라인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float", "nullStrategy": "skip"}},
        {"module_type": "unit_convert", "config": {"sourceUnit": "celsius", "targetUnit": "fahrenheit"}},
    ]

    def test_normal_value_conversion(self):
        """정상 온도값 celsius→fahrenheit 변환"""
        msg = make_message(value=25.5, unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - 77.9) < 0.01
        assert result["unit"] == "fahrenheit"
        assert result["dataType"] == "float"
        assert result["quality"] == 100

    def test_zero_celsius(self):
        """0°C → 32°F"""
        msg = make_message(value=0, unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - 32.0) < 0.01

    def test_negative_celsius(self):
        """음수 온도 -40°C → -40°F (교차점)"""
        msg = make_message(value=-40, unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - (-40.0)) < 0.01

    def test_boiling_point(self):
        """100°C → 212°F"""
        msg = make_message(value=100, unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - 212.0) < 0.01

    def test_null_value_skip(self):
        """null 값 → skip(drop)"""
        msg = make_message(value=None, unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None

    def test_empty_string_skip(self):
        """빈 문자열 → skip(drop)"""
        msg = make_message(value="", unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None

    def test_string_number_conversion(self):
        """문자열 숫자 '25.5' → float 변환 후 단위 변환"""
        msg = make_message(value="25.5", unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - 77.9) < 0.01

    def test_source_info_preserved(self):
        """소스 정보 유지 확인"""
        msg = make_message(value=25.5, tag_name="temp_01", connector_type="mqtt", connector_id=3)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["source"]["tagName"] == "temp_01"
        assert result["source"]["connectorType"] == "mqtt"
        assert result["source"]["connectorId"] == 3

    def test_tsdb_sink_row_construction(self):
        """TSDB 싱크 행 데이터 구성 검증 (DB 연결 없이 로직만)"""
        from backend.services.pipeline_modules import _parse_quality
        msg = make_message(value=25.5, unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)

        # 싱크가 구성할 행 데이터 시뮬레이션
        assert result is not None
        assert isinstance(result["value"], float)
        assert _parse_quality(result["quality"]) == 100
        assert _parse_quality("good") == 100
        assert _parse_quality("bad") == 0
        assert _parse_quality("uncertain") == 50

    def test_whitespace_string_trimmed(self):
        """공백 포함 문자열 트림 후 변환"""
        msg = make_message(value="  25.5  ", unit="celsius")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - 77.9) < 0.01
