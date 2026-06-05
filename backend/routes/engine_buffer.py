"""엔진 버퍼 관리 — 파이프라인 버퍼 상태 조회 및 플러시 API"""

import logging
from flask import Blueprint, jsonify

from backend.database import SessionLocal
from backend.models.pipeline import Pipeline
from backend.services.pipeline_modules import (
    get_sink_buffer_status,
    get_agg_buffer_status,
    get_window_cache_status,
    flush_single_sink_buffer,
    _pid_from_cache_key,
)
from backend.services.tenant_filter import filter_by_tenant

logger = logging.getLogger(__name__)

engine_buffer_bp = Blueprint("engine_buffer", __name__, url_prefix="/api/engine")


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


def _my_pipeline_ids():
    """현 tenant 의 pipeline id set."""
    db = SessionLocal()
    try:
        rows = filter_by_tenant(db.query(Pipeline.id), Pipeline).all()
        return {r[0] for r in rows}
    finally:
        db.close()


def _filter_by_pid(buf_dict, pid_set):
    """cache_key → pid 추출 후 자기 tenant pid set 에 속한 항목만 남김.

    pid 추출 실패 시 보수적으로 제외 (cross-tenant 노출 방지).
    """
    out = {}
    for k, v in buf_dict.items():
        try:
            pid = _pid_from_cache_key(k)
        except Exception:
            pid = None
        if pid in pid_set:
            out[k] = v
    return out


# ══════════════════════════════════════════════════
# GET /api/engine/buffer — 버퍼 상태 조회 (tenant 격리)
# ══════════════════════════════════════════════════
@engine_buffer_bp.route("/buffer", methods=["GET"])
def get_buffer_status():
    try:
        my_pids = _my_pipeline_ids()
        sink_buffers  = _filter_by_pid(get_sink_buffer_status(),  my_pids)
        agg_buffers   = _filter_by_pid(get_agg_buffer_status(),   my_pids)
        window_cache  = _filter_by_pid(get_window_cache_status(), my_pids)

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
# POST /api/engine/buffer/flush — 자기 tenant 의 싱크 버퍼만 플러시
# ══════════════════════════════════════════════════
@engine_buffer_bp.route("/buffer/flush", methods=["POST"])
def flush_all_buffers():
    try:
        my_pids = _my_pipeline_ids()
        pre_status = _filter_by_pid(get_sink_buffer_status(), my_pids)
        total_before = sum(b["count"] for b in pre_status.values())
        buffer_count = len(pre_status)

        # 자기 tenant 의 키만 순회하여 개별 flush.
        # per-key try/except 로 한 키 실패가 나머지 flush 를 막지 않게 한다.
        flushed_keys = []
        failed_keys = []
        for key in list(pre_status.keys()):
            try:
                c = flush_single_sink_buffer(key)
                if c >= 0:
                    flushed_keys.append(key)
            except Exception as fe:
                logger.error("flush_all_buffers: key=%s 실패 — %s", key, fe)
                failed_keys.append(key)

        logger.info("자기 tenant 싱크 버퍼 플러시: %d/%d 키 성공, 실패 %d, %d건",
                    len(flushed_keys), buffer_count, len(failed_keys), total_before)
        return _ok({
            "message": "자기 테넌트 싱크 버퍼 플러시 완료",
            "flushedBuffers": len(flushed_keys),
            "failedBuffers": len(failed_keys),
            "flushedItems": total_before,
        })
    except Exception as e:
        logger.error("싱크 버퍼 플러시 오류: %s", e)
        return _err(str(e), "SERVER_ERROR", 500)


# ══════════════════════════════════════════════════
# POST /api/engine/buffer/flush/<buffer_key> — 개별 (tenant 가드)
# ══════════════════════════════════════════════════
@engine_buffer_bp.route("/buffer/flush/<path:buffer_key>", methods=["POST"])
def flush_single_buffer(buffer_key):
    try:
        # cross-tenant 가드
        try:
            pid = _pid_from_cache_key(buffer_key)
        except Exception:
            pid = None
        if pid is None or pid not in _my_pipeline_ids():
            return _err(f"버퍼를 찾을 수 없습니다: {buffer_key}", "NOT_FOUND", 404)

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
