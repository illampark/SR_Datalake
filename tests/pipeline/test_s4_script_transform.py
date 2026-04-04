"""
시나리오 4: DB 소스 → 스크립트 변환 → Enrich → 외부 Kafka
[DB Source] → Normalize → Script(Python) → Enrich → [External Kafka Sink]

목적: Custom Script 모듈 + 조건부 변환 + Enrich 검증
"""
import copy
import pytest
from tests.pipeline.conftest import make_message, run_pipeline_chain


class TestS4ScriptTransform:
    """시나리오 4: 스크립트 변환 파이프라인"""

    STEPS = [
        {"module_type": "normalize", "config": {"targetType": "float"}},
        {"module_type": "script", "config": {
            "language": "python",
            "code": (
                "source = message.get('source', {})\n"
                "if source.get('tagName') == 'daily_revenue':\n"
                "    value = value * 1.1\n"
            ),
            "timeout": 5,
        }},
        {"module_type": "enrich", "config": {
            "fields": {"department": "Sales", "currency": "KRW"},
            "addTimestamp": True,
        }},
    ]

    def _make_db_msg(self, tag, value):
        return make_message(
            tag_name=tag, value=value, unit="",
            connector_type="db", connector_id=3,
        )

    def test_revenue_scaled(self):
        """daily_revenue → ×1.1 변환"""
        msg = self._make_db_msg("daily_revenue", 50000000)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == pytest.approx(55000000, rel=1e-6)

    def test_order_count_unchanged(self):
        """daily_order_count → 변환 없음 (tagName 불일치)"""
        msg = self._make_db_msg("daily_order_count", 150)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["value"] == 150.0

    def test_enrich_department_currency(self):
        """Enrich: department, currency 필드 추가"""
        msg = self._make_db_msg("daily_revenue", 1000)
        result = run_pipeline_chain(msg, self.STEPS)
        assert result is not None
        assert result["enrichment"]["department"] == "Sales"
        assert result["enrichment"]["currency"] == "KRW"

    def test_script_math_operations(self):
        """Script에서 math 모듈 사용"""
        steps = copy.deepcopy(self.STEPS)
        steps[1]["config"]["code"] = "value = math.sqrt(value)"
        msg = self._make_db_msg("test_sqrt", 144)
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert result["value"] == pytest.approx(12.0, abs=0.01)

    def test_script_error_handling(self):
        """Script 오류 시 _script_error 추가, 메시지는 유지"""
        steps = copy.deepcopy(self.STEPS)
        steps[1]["config"]["code"] = "result = 1 / 0"  # ZeroDivisionError
        msg = self._make_db_msg("test_error", 100)
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert "_script_error" in result

    def test_script_value_assignment(self):
        """Script에서 value 변수 직접 대입"""
        steps = copy.deepcopy(self.STEPS)
        steps[1]["config"]["code"] = "value = value * 2 + 10"
        msg = self._make_db_msg("test_calc", 50)
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert result["value"] == 110.0

    def test_script_conditional_complex(self):
        """Script 복합 조건"""
        steps = copy.deepcopy(self.STEPS)
        steps[1]["config"]["code"] = (
            "if value > 100:\n"
            "    value = 100\n"
            "    message['_capped'] = True\n"
        )
        msg = self._make_db_msg("test_cap", 200)
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert result["value"] == 100
        assert result.get("_capped") is True

    def test_unsupported_language_passthrough(self):
        """지원하지 않는 언어 → 메시지 그대로 통과"""
        steps = copy.deepcopy(self.STEPS)
        steps[1]["config"]["language"] = "javascript"
        steps[1]["config"]["code"] = "value = value * 2"
        msg = self._make_db_msg("test_js", 50)
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert result["value"] == 50.0  # 변환 없음

    def test_empty_script_passthrough(self):
        """빈 스크립트 → 메시지 그대로 통과"""
        steps = copy.deepcopy(self.STEPS)
        steps[1]["config"]["code"] = ""
        msg = self._make_db_msg("test_empty", 42)
        result = run_pipeline_chain(msg, steps)
        assert result is not None
        assert result["value"] == 42.0
