"""
시나리오 3: Modbus 멀티태그 집계 파이프라인
[Modbus Source] → Normalize → Aggregate(60s, avg/min/max/count) → [RDBMS Sink]

목적: Aggregate 모듈의 시간 윈도우 동작 + 다중 태그 독립 집계 검증
"""
import copy
import time
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS3Aggregate:
    """시나리오 3: Modbus 멀티태그 집계 파이프라인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float"}},
        {"module_type": "aggregate", "config": {
            "windowSeconds": 2,  # 테스트용 2초 윈도우
            "functions": ["avg", "min", "max", "count", "sum"],
            "emitMode": "end",
            "_pipeline_id": 3,
        }},
    ]

    def _make_modbus_msg(self, value, tag="motor_current"):
        return make_message(
            tag_name=tag, value=value, unit="A",
            connector_type="modbus", connector_id=2,
        )

    def test_buffer_before_window_end(self):
        """윈도우 종료 전에는 None 반환 (버퍼링)"""
        steps = copy.deepcopy(self.STEPS)
        msg = self._make_modbus_msg(12.5)
        result = run_pipeline_chain(msg, steps)
        assert result is None  # 아직 윈도우 내

    def test_aggregate_emits_on_window_end(self):
        """윈도우 종료 시 집계 결과 방출"""
        steps = copy.deepcopy(self.STEPS)
        values = [12.5, 13.1, 12.8, 14.2, 11.9, 13.5]

        # 윈도우 시작 시간을 과거로 설정하여 즉시 만료되도록
        from backend.services import pipeline_modules as pm
        cache_key = "agg:3:motor_current_agg"
        pm._agg_buffers[cache_key] = {
            "values": values[:-1],
            "start": datetime.utcnow() - timedelta(seconds=3),
            "count": len(values) - 1,
        }

        # 마지막 메시지가 윈도우 종료를 트리거
        msg = self._make_modbus_msg(values[-1], tag="motor_current_agg")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert isinstance(result["value"], dict)
        assert "avg" in result["value"]
        assert "min" in result["value"]
        assert "max" in result["value"]
        assert "count" in result["value"]
        assert "sum" in result["value"]

    def test_aggregate_values_correct(self):
        """집계 값 정확성 검증"""
        from backend.services import pipeline_modules as pm
        steps = copy.deepcopy(self.STEPS)
        values = [12.5, 13.1, 12.8, 14.2, 11.9, 13.5]
        cache_key = "agg:3:motor_agg_vals"
        pm._agg_buffers[cache_key] = {
            "values": values[:-1],
            "start": datetime.utcnow() - timedelta(seconds=3),
            "count": len(values) - 1,
        }

        msg = self._make_modbus_msg(values[-1], tag="motor_agg_vals")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        agg = result["value"]
        assert agg["count"] == len(values)
        assert agg["min"] == min(values)
        assert agg["max"] == max(values)
        assert abs(agg["avg"] - sum(values) / len(values)) < 0.001
        assert abs(agg["sum"] - sum(values)) < 0.001

    def test_independent_tag_aggregation(self):
        """서로 다른 태그는 독립적으로 집계"""
        from backend.services import pipeline_modules as pm
        steps = copy.deepcopy(self.STEPS)
        past = datetime.utcnow() - timedelta(seconds=3)

        # motor_current 버퍼
        pm._agg_buffers["agg:3:tag_ind_A"] = {
            "values": [10.0, 20.0], "start": past, "count": 2
        }
        # motor_speed 버퍼
        pm._agg_buffers["agg:3:tag_ind_B"] = {
            "values": [1500.0, 1600.0], "start": past, "count": 2
        }

        msg_a = self._make_modbus_msg(30.0, tag="tag_ind_A")
        msg_b = self._make_modbus_msg(1700.0, tag="tag_ind_B")

        result_a = run_pipeline_chain(msg_a, steps)
        result_b = run_pipeline_chain(msg_b, steps)

        assert result_a is not None and result_b is not None
        assert result_a["value"]["avg"] == pytest.approx(20.0, abs=0.01)
        assert result_b["value"]["avg"] == pytest.approx(1600.0, abs=0.01)

    def test_aggregate_data_type_changes(self):
        """집계 후 dataType이 'aggregate'로 변경"""
        from backend.services import pipeline_modules as pm
        steps = copy.deepcopy(self.STEPS)
        pm._agg_buffers["agg:3:dtype_tag"] = {
            "values": [10.0], "start": datetime.utcnow() - timedelta(seconds=3), "count": 1
        }
        msg = self._make_modbus_msg(20.0, tag="dtype_tag")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert result["dataType"] == "aggregate"
        assert result.get("_aggregated") is True

    def test_window_metadata_present(self):
        """집계 결과에 windowStart/windowEnd 메타데이터 포함"""
        from backend.services import pipeline_modules as pm
        steps = copy.deepcopy(self.STEPS)
        pm._agg_buffers["agg:3:win_meta_tag"] = {
            "values": [10.0], "start": datetime.utcnow() - timedelta(seconds=3), "count": 1
        }
        msg = self._make_modbus_msg(20.0, tag="win_meta_tag")
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert "_windowStart" in result
        assert "_windowEnd" in result

    def test_string_value_normalized(self):
        """문자열 값 '15.5' → float 변환 후 집계"""
        steps = copy.deepcopy(self.STEPS)
        msg = self._make_modbus_msg("15.5", tag="str_agg_tag")
        result = run_pipeline_chain(msg, steps)
        # 윈도우 내이므로 None (버퍼링)
        assert result is None
