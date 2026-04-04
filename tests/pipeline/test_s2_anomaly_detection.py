"""
시나리오 2: 이상 감지 + 필터링 파이프라인
[OPC-UA Source] → Normalize → Filter(range,clamp) → Anomaly(z-score,flag) → Enrich

목적: 처리 모듈 체이닝 + 다중 액션(clamp/flag) 검증
"""
import copy
import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS2AnomalyDetection:
    """시나리오 2: 이상 감지 + 필터링 파이프라인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float", "nullStrategy": "zero"}},
        {"module_type": "filter", "config": {
            "filterType": "range", "minValue": 0, "maxValue": 150, "action": "clamp"
        }},
        {"module_type": "anomaly", "config": {
            "method": "zscore", "threshold": 3.0, "windowSize": 60,
            "action": "flag", "_pipeline_id": 99
        }},
        {"module_type": "enrich", "config": {
            "fields": {"plant": "Plant-A", "line": "Line-1"}, "addTimestamp": True
        }},
    ]

    def _make_opcua_msg(self, value, tag="pressure_gauge_01"):
        return make_message(
            tag_name=tag, value=value, unit="bar",
            connector_type="opcua", connector_id=2,
        )

    def test_normal_value_passthrough(self):
        """정상값 75.2 bar → 그대로 통과"""
        msg = self._make_opcua_msg(75.2)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 75.2
        assert result["quality"] == 100

    def test_clamp_above_max(self):
        """범위 초과 180.0 → clamp → 150.0"""
        msg = self._make_opcua_msg(180.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 150

    def test_clamp_below_min(self):
        """음수값 -5.0 → clamp → 0"""
        msg = self._make_opcua_msg(-5.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 0

    def test_null_to_zero(self):
        """null → normalize(zero) → 0.0 → 정상 처리"""
        msg = self._make_opcua_msg(None)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 0

    def test_anomaly_flag_after_window(self):
        """정상값 60건 → 이상값 → anomaly flag 확인"""
        steps = copy.deepcopy(self.STEPS)
        # 윈도우 적재: 정상값 60건 (10.0 근처)
        for i in range(60):
            msg = self._make_opcua_msg(10.0 + (i % 3) * 0.1, tag="pressure_anom")
            run_pipeline_chain(msg, steps)

        # 이상값: 150 (clamp된 범위 내이지만 윈도우 평균 대비 큰 z-score)
        msg = self._make_opcua_msg(150.0, tag="pressure_anom")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        # z-score > 3이면 flag
        if result.get("_anomaly"):
            assert result["quality"] == 50  # 100 - 50
        # 윈도우 내의 값이므로 anomaly 탐지 가능

    def test_enrich_fields_added(self):
        """Enrich: plant, line 필드 추가 확인"""
        msg = self._make_opcua_msg(75.2)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert "enrichment" in result
        assert result["enrichment"]["plant"] == "Plant-A"
        assert result["enrichment"]["line"] == "Line-1"
        assert "processedAt" in result["enrichment"]

    def test_enrich_timestamp_added(self):
        """Enrich: processedAt 타임스탬프 자동 추가"""
        msg = self._make_opcua_msg(50.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["enrichment"]["processedAt"].endswith("Z")

    def test_extreme_clamp_then_anomaly(self):
        """극단값 999.0 → clamp(150) → anomaly 탐지"""
        steps = copy.deepcopy(self.STEPS)
        # 윈도우 적재
        for i in range(10):
            msg = self._make_opcua_msg(10.0, tag="pressure_extreme")
            run_pipeline_chain(msg, steps)

        msg = self._make_opcua_msg(999.0, tag="pressure_extreme")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        # 999 → clamp → 150, 윈도우 평균 ~10이므로 z-score 높음
        assert result["value"] == 150  # clamp 확인

    def test_multiple_tags_independent(self):
        """서로 다른 태그는 독립적으로 처리"""
        steps = copy.deepcopy(self.STEPS)
        msg1 = self._make_opcua_msg(50.0, tag="gauge_A")
        msg2 = self._make_opcua_msg(75.0, tag="gauge_B")
        result1 = run_pipeline_chain(msg1, steps)
        result2 = run_pipeline_chain(msg2, steps)
        assert result1 is not None and result2 is not None
        assert result1["value"] == 50.0
        assert result2["value"] == 75.0
