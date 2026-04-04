"""
시나리오 5: 전체 모듈 체이닝 (File Source)
[File Source] → Normalize → Unit Convert → Filter → Anomaly → Aggregate → Enrich

목적: 7개 처리 모듈 중 6개를 연속 체이닝하여 동작 검증
"""
import copy
from datetime import datetime, timedelta

import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS5FullChain:
    """시나리오 5: 전체 처리 모듈 체이닝"""

    STEPS = [
        {"module_type": "normalize", "config": {
            "targetType": "float", "nullStrategy": "zero"
        }},
        {"module_type": "unit_convert", "config": {
            "sourceUnit": "mm", "targetUnit": "inch"
        }},
        {"module_type": "filter", "config": {
            "filterType": "range", "minValue": 0, "maxValue": 10, "action": "clamp"
        }},
        {"module_type": "anomaly", "config": {
            "method": "zscore", "threshold": 3.0, "windowSize": 60,
            "action": "flag", "_pipeline_id": 5
        }},
        {"module_type": "enrich", "config": {
            "fields": {"equipment": "CNC-01", "measurement_type": "vibration"},
            "addTimestamp": True,
        }},
    ]

    def _make_file_msg(self, value, tag="vibration_x"):
        return make_message(
            tag_name=tag, value=value, unit="mm",
            connector_type="file", connector_id=1,
        )

    def test_full_chain_normal(self):
        """정상값: 2.5mm → inch 변환 → filter 통과 → enrich"""
        msg = self._make_file_msg(2.5)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        # 2.5mm ÷ 25.4 = 0.098425 inch
        assert abs(result["value"] - 0.098425) < 0.001
        assert result["unit"] == "inch"
        assert result["enrichment"]["equipment"] == "CNC-01"

    def test_null_to_zero_then_convert(self):
        """null → normalize(zero) → 0.0 → unit convert → 0.0 inch"""
        msg = self._make_file_msg(None)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 0.0

    def test_clamp_large_value(self):
        """300mm → 11.81 inch → clamp 10 inch"""
        msg = self._make_file_msg(300.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 10  # clamp

    def test_negative_clamped_to_zero(self):
        """음수 -5mm → -0.197 inch → clamp 0"""
        msg = self._make_file_msg(-5.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 0

    def test_chain_preserves_source(self):
        """전체 체인 후 source 정보 보존"""
        msg = self._make_file_msg(2.5, tag="vib_test")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["source"]["connectorType"] == "file"
        assert result["source"]["tagName"] == "vib_test"

    def test_chain_with_string_input(self):
        """문자열 '3.2' → normalize → float 변환 후 체인 진행"""
        msg = self._make_file_msg("3.2")
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        # 3.2mm → 0.12598 inch
        assert abs(result["value"] - 0.125984) < 0.001

    def test_enrichment_has_all_fields(self):
        """Enrich: equipment, measurement_type, processedAt 모두 존재"""
        msg = self._make_file_msg(1.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        enr = result["enrichment"]
        assert "equipment" in enr
        assert "measurement_type" in enr
        assert "processedAt" in enr


class TestS5FullChainWithAggregate:
    """시나리오 5 확장: Aggregate 포함 전체 체인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float", "nullStrategy": "zero"}},
        {"module_type": "unit_convert", "config": {"sourceUnit": "mm", "targetUnit": "inch"}},
        {"module_type": "filter", "config": {
            "filterType": "range", "minValue": 0, "maxValue": 10, "action": "clamp"
        }},
        {"module_type": "aggregate", "config": {
            "windowSeconds": 2, "functions": ["avg", "max", "count"],
            "emitMode": "end", "_pipeline_id": 55
        }},
        {"module_type": "enrich", "config": {
            "fields": {"equipment": "CNC-01"}, "addTimestamp": True,
        }},
    ]

    def test_aggregate_with_preceding_modules(self):
        """normalize → unit_convert → filter → aggregate 연속 동작"""
        from backend.services import pipeline_modules as pm
        steps = copy.deepcopy(self.STEPS)

        # 윈도우에 변환+필터된 값을 미리 적재
        converted_values = [v / 25.4 for v in [2.5, 3.0, 2.8]]  # mm→inch
        cache_key = "agg:55:vib_agg_full"
        pm._agg_buffers[cache_key] = {
            "values": converted_values,
            "start": datetime.utcnow() - timedelta(seconds=3),
            "count": len(converted_values),
        }

        msg = make_message(
            tag_name="vib_agg_full", value=2.7, unit="mm",
            connector_type="file", connector_id=1,
        )
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert isinstance(result["value"], dict)
        assert "avg" in result["value"]
        assert "max" in result["value"]
        assert "count" in result["value"]
        assert result["enrichment"]["equipment"] == "CNC-01"
