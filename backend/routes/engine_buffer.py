"""엔진 버퍼 관리 — 파이프라인 버퍼 상태 조회 및 플러시 API"""

import logging
from flask import Blueprint, jsonify

from backend.services.pipeline_modules import (
    get_sink_buffer_status,
    get_agg_buffer_status,
    get_window_cache_status,
    flush_all_sink_buffers,
    flush_single_sink_buffer,
)

logger = logging.getLogger(__name__)

engine_buffer_bp = Blueprint("engine_buffer", __name__, url_prefix="/api/engine")


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ══════════════════════════════════════════════════
# GET /api/engine/buffer — 버퍼 상태 조회
# ══════════════════════════════════════════════════
@engine_buffer_bp.route("/buffer", methods=["GET"])
def get_buffer_status():
    try:
        sink_buffers = get_sink_buffer_status()
        agg_buffers = get_agg_buffer_status()
        window_cache = get_window_cache_status()

        total_sink_items = sum(b["count"] for b in sink_buffers.values())
        total_agg_items = sum(b["valuesCount"] for b in agg_buffers.values())
        total_window_items = sum(b["size"] for b in window_cache.values())

        return _ok({
            "sinkBuffers": sink_buffers,
            "aggBuffers": agg_buffers,
            "windowCache": window_cache,
            "summary": {
                "sinkBufferCount": len(sink_buffers),
                "sinkBufferItems": total_sink_items,
                "aggBufferCount": len(agg_buffers),
                "aggBufferItems": total_agg_items,
                "windowCacheCount": len(window_cache),
                "windowCacheItems": total_window_items,
            },
        })
    except Exception as e:
        logger.error("버퍼 상태 조회 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)


# ══════════════════════════════════════════════════
# POST /api/engine/buffer/flush — 전체 싱크 버퍼 플러시
# ══════════════════════════════════════════════════
@engine_buffer_bp.route("/buffer/flush", methods=["POST"])
def flush_all_buffers():
    try:
        pre_status = get_sink_buffer_status()
        total_before = sum(b["count"] for b in pre_status.values())
        buffer_count = len(pre_status)

        flush_all_sink_buffers()

        logger.info("전체 싱크 버퍼 플러시 완료: %d개 버퍼, %d건",
                     buffer_count, total_before)
        return _ok({
            "message": "전체 싱크 버퍼 플러시 완료",
            "flushedBuffers": buffer_count,
            "flushedItems": total_before,
        })
    except Exception as e:
        logger.error("전체 싱크 버퍼 플러시 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)


# ══════════════════════════════════════════════════
# POST /api/engine/buffer/flush/<buffer_key> — 개별 싱크 버퍼 플러시
# ══════════════════════════════════════════════════
@engine_buffer_bp.route("/buffer/flush/<path:buffer_key>", methods=["POST"])
def flush_single_buffer(buffer_key):
    try:
        count = flush_single_sink_buffer(buffer_key)
        if count == -1:
            return _err(f"버퍼를 찾을 수 없습니다: {buffer_key}", "NOT_FOUND", 404)

        logger.info("싱크 버퍼 플러시 완료: %s (%d건)", buffer_key, count)
        return _ok({
            "message": f"버퍼 플러시 완료: {buffer_key}",
            "bufferKey": buffer_key,
            "flushedItems": count,
        })
    except Exception as e:
        logger.error("싱크 버퍼 플러시 오류 [%s]: %s", buffer_key, e)
        return _err(str(e), "SERVER_ERROR", 500)
