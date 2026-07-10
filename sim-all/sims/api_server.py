"""HTTP REST API 서버 시뮬레이터.

Flask 로 다음 엔드포인트 제공:
  GET /health                    → OK
  GET /api/press01/current       → 최신 스냅샷 (JSON)
  GET /api/press01/history?minutes=N → 최근 N분 히스토리 (JSON list)
  GET /api/press01/spec          → AAS 스키마 예시 (엣지 개발자 참고)
"""
import logging
from dataclasses import asdict

from flask import Flask, jsonify, request

log = logging.getLogger("sim.api")


def run(state, cfg):
    if not cfg.get("enabled", True):
        log.info("api disabled")
        return

    app = Flask(__name__)
    host = cfg.get("host", "0.0.0.0")
    port = int(cfg.get("port", 8080))
    history_max = int(cfg.get("history_max_records", 3600))

    @app.route("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.route("/api/press01/current")
    def current():
        return jsonify(state.to_dict())

    @app.route("/api/press01/history")
    def history():
        minutes = int(request.args.get("minutes", 10))
        rows = state.history(minutes=minutes)
        return jsonify([asdict(r) for r in rows[-history_max:]])

    @app.route("/api/press01/spec")
    def spec():
        return jsonify({
            "asset_id": "PRESS-01",
            "properties": [
                {"name": "temperature", "type": "float", "unit": "°C"},
                {"name": "humidity", "type": "float", "unit": "%RH"},
                {"name": "pressure", "type": "float", "unit": "hPa"},
                {"name": "mode", "type": "string"},
                {"name": "total_cycle", "type": "int"},
                {"name": "last_error", "type": "string"},
                {"name": "firmware_ver", "type": "string"},
            ],
        })

    log.info("HTTP API server starting at %s:%d", host, port)
    # flask dev server: 시뮬레이터 용도로 충분
    app.run(host=host, port=port, threaded=True, debug=False, use_reloader=False)
