"""
시나리오 8: 단위 변환 체인 → Enrich → 외부 File
[Modbus Source] → Normalize → Unit Convert → Enrich → [External File Sink]

목적: 내장 단위 변환 테이블 전체 검증 + 커스텀 변환
"""
import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS8UnitConvert:
    """시나리오 8: 내장 단위 변환 + 커스텀 변환"""

    def _make_msg(self, value, tag="Tank.Level", unit="mm"):
        return make_message(
            tag_name=tag, value=value, unit=unit,
            connector_type="modbus", connector_id=1,
        )

    def _unit_steps(self, source, target, factor=None, offset=None):
        config = {"sourceUnit": source, "targetUnit": target}
        if factor is not None:
            config["factor"] = factor
        if offset is not None:
            config["offset"] = offset
        return [
            {"module_type": "normalize", "config": {"targetType": "float"}},
            {"module_type": "unit_convert", "config": config},
        ]

    # ── 온도 변환 ──
    def test_celsius_to_fahrenheit(self):
        msg = self._make_msg(100, unit="celsius")
        result = run_pipeline_chain(msg, self._unit_steps("celsius", "fahrenheit"))
        assert abs(result["value"] - 212.0) < 0.01

    def test_fahrenheit_to_celsius(self):
        msg = self._make_msg(212, unit="fahrenheit")
        result = run_pipeline_chain(msg, self._unit_steps("fahrenheit", "celsius"))
        assert abs(result["value"] - 100.0) < 0.01

    def test_celsius_to_kelvin(self):
        msg = self._make_msg(0, unit="celsius")
        result = run_pipeline_chain(msg, self._unit_steps("celsius", "kelvin"))
        assert abs(result["value"] - 273.15) < 0.01

    def test_kelvin_to_celsius(self):
        msg = self._make_msg(273.15, unit="kelvin")
        result = run_pipeline_chain(msg, self._unit_steps("kelvin", "celsius"))
        assert abs(result["value"] - 0.0) < 0.01

    # ── 압력 변환 ──
    def test_bar_to_psi(self):
        msg = self._make_msg(1.0, unit="bar")
        result = run_pipeline_chain(msg, self._unit_steps("bar", "psi"))
        assert abs(result["value"] - 14.5038) < 0.01

    def test_psi_to_bar(self):
        msg = self._make_msg(14.5038, unit="psi")
        result = run_pipeline_chain(msg, self._unit_steps("psi", "bar"))
        assert abs(result["value"] - 1.0) < 0.01

    def test_kpa_to_psi(self):
        msg = self._make_msg(100, unit="kpa")
        result = run_pipeline_chain(msg, self._unit_steps("kpa", "psi"))
        assert abs(result["value"] - 14.5038) < 0.01

    # ── 길이 변환 ──
    def test_mm_to_inch(self):
        msg = self._make_msg(25.4, unit="mm")
        result = run_pipeline_chain(msg, self._unit_steps("mm", "inch"))
        assert abs(result["value"] - 1.0) < 0.001

    def test_inch_to_mm(self):
        msg = self._make_msg(1.0, unit="inch")
        result = run_pipeline_chain(msg, self._unit_steps("inch", "mm"))
        assert abs(result["value"] - 25.4) < 0.01

    def test_m_to_ft(self):
        msg = self._make_msg(1.0, unit="m")
        result = run_pipeline_chain(msg, self._unit_steps("m", "ft"))
        assert abs(result["value"] - 3.28084) < 0.001

    def test_ft_to_m(self):
        msg = self._make_msg(3.28084, unit="ft")
        result = run_pipeline_chain(msg, self._unit_steps("ft", "m"))
        assert abs(result["value"] - 1.0) < 0.001

    # ── 무게 변환 ──
    def test_kg_to_lb(self):
        msg = self._make_msg(1.0, unit="kg")
        result = run_pipeline_chain(msg, self._unit_steps("kg", "lb"))
        assert abs(result["value"] - 2.20462) < 0.001

    def test_lb_to_kg(self):
        msg = self._make_msg(2.20462, unit="lb")
        result = run_pipeline_chain(msg, self._unit_steps("lb", "kg"))
        assert abs(result["value"] - 1.0) < 0.001

    # ── 유량 변환 ──
    def test_lpm_to_gpm(self):
        msg = self._make_msg(100, unit="lpm")
        result = run_pipeline_chain(msg, self._unit_steps("lpm", "gpm"))
        assert abs(result["value"] - 26.4172) < 0.01

    def test_gpm_to_lpm(self):
        msg = self._make_msg(26.4172, unit="gpm")
        result = run_pipeline_chain(msg, self._unit_steps("gpm", "lpm"))
        assert abs(result["value"] - 100.0) < 0.1

    # ── 커스텀 변환 ──
    def test_custom_factor_offset(self):
        """커스텀 선형 변환: result = value * factor + offset"""
        msg = self._make_msg(100, unit="custom_a")
        result = run_pipeline_chain(msg, self._unit_steps(
            "custom_a", "custom_b", factor=2.5, offset=10
        ))
        assert abs(result["value"] - 260.0) < 0.01  # 100 * 2.5 + 10

    def test_same_unit_no_change(self):
        """동일 단위 → 변환 없음"""
        msg = self._make_msg(50, unit="celsius")
        result = run_pipeline_chain(msg, self._unit_steps("celsius", "celsius"))
        assert abs(result["value"] - 50.0) < 0.01

    def test_unit_field_updated(self):
        """변환 후 unit 필드 업데이트"""
        msg = self._make_msg(25.4, unit="mm")
        result = run_pipeline_chain(msg, self._unit_steps("mm", "inch"))
        assert result["unit"] == "inch"

    def test_none_value_passthrough(self):
        """None 값 → normalize에서 skip"""
        steps = [
            {"module_type": "normalize", "config": {"targetType": "float", "nullStrategy": "skip"}},
            {"module_type": "unit_convert", "config": {"sourceUnit": "mm", "targetUnit": "inch"}},
        ]
        msg = self._make_msg(None)
        result = run_pipeline_chain(msg, steps)
        assert result is None


class TestS8WithEnrich:
    """시나리오 8 확장: Unit Convert + Enrich 통합"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float"}},
        {"module_type": "unit_convert", "config": {"sourceUnit": "bar", "targetUnit": "psi"}},
        {"module_type": "enrich", "config": {
            "fields": {"equipment": "Pump-01", "location": "Building-A"},
            "addTimestamp": True,
        }},
    ]

    def test_convert_then_enrich(self):
        msg = make_message(
            tag_name="Pump.Pressure", value=5.0, unit="bar",
            connector_type="modbus", connector_id=1,
        )
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert abs(result["value"] - 72.519) < 0.1  # 5 * 14.5038
        assert result["unit"] == "psi"
        assert result["enrichment"]["equipment"] == "Pump-01"
        assert result["enrichment"]["location"] == "Building-A"
        assert "processedAt" in result["enrichment"]
