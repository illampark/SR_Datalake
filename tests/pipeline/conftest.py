"""
파이프라인 테스트 공통 픽스처 및 헬퍼
"""
import copy
import sys
import os
import pytest

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


def make_message(tag_name="temperature_sensor_1", value=25.5, data_type="float",
                 unit="celsius", quality=100, connector_type="mqtt", connector_id=1,
                 timestamp="2026-02-10T10:00:00Z"):
    """테스트용 표준 메시지 생성"""
    return {
        "source": {
            "connectorType": connector_type,
            "connectorId": connector_id,
            "tagName": tag_name,
        },
        "value": value,
        "dataType": data_type,
        "unit": unit,
        "quality": quality,
        "timestamp": timestamp,
    }


def run_pipeline_chain(message, steps):
    """파이프라인 모듈 체인 실행 (process_message 사용)
    steps: [{"module_type": "normalize", "config": {...}}, ...]
    """
    from backend.services.pipeline_modules import process_message
    msg = copy.deepcopy(message)
    for step in steps:
        msg = process_message(msg, step["module_type"], step["config"])
        if msg is None:
            return None
    return msg


def clear_module_caches():
    """모듈 내부 캐시 초기화 (테스트 격리용)"""
    from backend.services import pipeline_modules as pm
    pm._window_cache.clear()
    pm._agg_buffers.clear()
    pm._sink_buffers.clear()


@pytest.fixture(autouse=True)
def clean_caches():
    """각 테스트 전후 캐시 초기화"""
    clear_module_caches()
    yield
    clear_module_caches()
