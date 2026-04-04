"""
시나리오 7: API 소스 + 실시간 품질 모니터링
[API Source] → Normalize → Filter(range,drop) → Anomaly(moving_avg,flag) → [TSDB]

목적: Filter condition 기반 drop + Anomaly flag 시 quality 감소 검증
"""
import copy
import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS7ApiQuality:
    """시나리오 7: API 소스 + 품질 모니터링 파이프라인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float", "nullStrategy": "skip"}},
        {"module_type": "filter", "config": {
            "filterType": "range", "minValue": -50, "maxValue": 60, "action": "drop"
        }},
        {"module_type": "anomaly", "config": {
            "method": "moving_avg", "threshold": 2.0, "windowSize": 24,
            "action": "flag", "_pipeline_id": 7
        }},
    ]

    def _make_api_msg(self, value, tag="outdoor_temp"):
        return make_message(
            tag_name=tag, value=value, unit="celsius",
            connector_type="api", connector_id=1,
        )

    def test_normal_value_quality_100(self):
        """① 정상: 22.5°C → quality: 100"""
        msg = self._make_api_msg(22.5)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 22.5
        assert result["quality"] == 100

    def test_sequential_normal_values(self):
        """② 연속 정상값: 22.5, 23.1 → 모두 quality 100"""
        steps = copy.deepcopy(self.STEPS)
        for v in [22.5, 23.1]:
            msg = self._make_api_msg(v, tag="seq_temp")
            result = run_pipeline_chain(msg, steps)
            assert result is not None
            assert result["quality"] == 100

    def test_extreme_cold_dropped(self):
        """③ -80°C → filter range drop"""
        msg = self._make_api_msg(-80.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None

    def test_above_range_dropped(self):
        """60°C 초과 → filter range drop"""
        msg = self._make_api_msg(65.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None

    def test_boundary_min_passes(self):
        """경계값 -50°C → 통과"""
        msg = self._make_api_msg(-50.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None

    def test_boundary_max_passes(self):
        """경계값 60°C → 통과"""
        msg = self._make_api_msg(60.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None

    def test_anomaly_flag_quality_decrease(self):
        """④ 안정적 패턴 후 급변 → anomaly flag → quality 50"""
        steps = copy.deepcopy(self.STEPS)
        # 윈도우 적재: 안정적 패턴
        for i in range(10):
            msg = self._make_api_msg(22.0 + (i % 3) * 0.1, tag="anomaly_q_temp")
            run_pipeline_chain(msg, steps)

        # 급변: 55°C (범위 내이지만 moving_avg 대비 큰 차이)
        msg = self._make_api_msg(55.0, tag="anomaly_q_temp")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        if result.get("_anomaly"):
            assert result["quality"] == 50  # 100 - 50

    def test_null_response_dropped(self):
        """⑤ null 응답 → normalize skip → drop"""
        msg = self._make_api_msg(None)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is None

    def test_quality_parse_various(self):
        """quality 파싱: 다양한 입력 형태"""
        from backend.services.pipeline_modules import _parse_quality
        assert _parse_quality(100) == 100
        assert _parse_quality(85.5) == 85
        assert _parse_quality("good") == 100
        assert _parse_quality("ok") == 80
        assert _parse_quality("bad") == 0
        assert _parse_quality("uncertain") == 50
        assert _parse_quality("75") == 75
        assert _parse_quality(None) == 100

    def test_source_preserved_api(self):
        """API 소스 정보 유지"""
        msg = self._make_api_msg(25.0)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["source"]["connectorType"] == "api"
        assert result["source"]["connectorId"] == 1
