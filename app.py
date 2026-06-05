import logging
import os
from datetime import timedelta
from flask import Flask, render_template, request, redirect, session, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
from backend.config import SECRET_KEY

_auth_diag = logging.getLogger("sdl.authdiag")
_auth_diag.setLevel(logging.INFO)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 2 * 1024 * 1024 * 1024  # 2GB max upload
app.secret_key = SECRET_KEY
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=480)
# 기본 이름 'session'은 타 Flask 앱의 스테일 쿠키와 충돌하므로 SDL 전용 이름 사용
app.config['SESSION_COOKIE_NAME'] = 'sdl_session'
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
# HTTPS 환경에서만 True. 역방향 프록시(nginx) 뒤에서도 환경변수로 제어
app.config['SESSION_COOKIE_SECURE'] = (
    os.getenv('SESSION_COOKIE_SECURE', 'false').lower() == 'true'
)
# Nginx 등 역방향 프록시 뒤에서 X-Forwarded-Proto/Host 신뢰
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# ── Backend API Setup ──
from backend.database import init_db
from backend.routes import tsdb_bp, rdbms_bp, file_bp, mqtt_bp, db_bp, file_watch_bp, retention_bp
from backend.routes import opcua_bp, modbus_bp, api_bp
from backend.routes import pipeline_bp, metadata_bp, catalog_bp
from backend.routes import engine_status_bp, engine_batch_bp, engine_buffer_bp, engine_perf_bp
from backend.routes import integration_bp
from backend.routes import monitoring_bp
from backend.routes import alarm_bp
from backend.routes import admin_bp
from backend.routes import backup_bp
from backend.routes import gateway_bp
from backend.routes import import_bp
from backend.routes.notice import notice_bp

from backend.routes.sys import sys_bp
from backend.routes.tenant_me import tenant_me_bp
app.register_blueprint(sys_bp)
app.register_blueprint(tenant_me_bp)

app.register_blueprint(tsdb_bp)
app.register_blueprint(rdbms_bp)
app.register_blueprint(file_bp)
app.register_blueprint(mqtt_bp)
app.register_blueprint(db_bp)
app.register_blueprint(file_watch_bp)
app.register_blueprint(opcua_bp)
app.register_blueprint(modbus_bp)
app.register_blueprint(api_bp)
app.register_blueprint(retention_bp)
app.register_blueprint(pipeline_bp)
app.register_blueprint(metadata_bp)
app.register_blueprint(catalog_bp)
app.register_blueprint(engine_status_bp)
app.register_blueprint(engine_batch_bp)
app.register_blueprint(engine_buffer_bp)
app.register_blueprint(engine_perf_bp)
app.register_blueprint(integration_bp)
app.register_blueprint(monitoring_bp)
app.register_blueprint(alarm_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(backup_bp)
app.register_blueprint(gateway_bp)
app.register_blueprint(import_bp)
app.register_blueprint(notice_bp)

with app.app_context():
    init_db()

# Load i18n translations
from backend.i18n import load_translations, get_translation, get_all_translations, get_current_lang
load_translations()

@app.context_processor
def inject_i18n():
    lang = get_current_lang()
    def t(key):
        return get_translation(key, lang)
    return {'t': t, 'current_lang': lang, 'i18n_all': get_all_translations(lang)}


@app.context_processor
def inject_system_timezone():
    """모든 템플릿에서 system_timezone 변수로 admin_setting 의 TZ 값을 접근.
    UI 가 timestamp 를 표시할 때 fmtLocal() 이 이 값 기준으로 변환한다.
    저장소(PG/MinIO)는 UTC 그대로 유지."""
    try:
        from backend.services.system_settings import get_timezone
        return {'system_timezone': get_timezone()}
    except Exception:
        return {'system_timezone': 'Asia/Seoul'}


@app.after_request
def _set_lang_cookie(response):
    """첫 방문 시 lang 쿠키가 없으면 시스템 기본 언어로 설정한다."""
    from flask import request
    if not request.cookies.get('lang'):
        lang = get_current_lang()
        response.set_cookie('lang', lang, max_age=365 * 24 * 3600, samesite='Lax')
    return response

# Seed default users and login policy
from backend.routes.admin import seed_default_users
seed_default_users()

# Start retention scheduler (background thread)
from backend.services.retention_scheduler import start_scheduler
start_scheduler()

# Start MQTT manager (connect to Mosquitto)
from backend.services import mqtt_manager
mqtt_manager.connect()

# Start alarm evaluation engine (background thread, 60s interval)
from backend.services import alarm_engine
alarm_engine.start()

# Start backup scheduler (background thread)
from backend.services.backup_scheduler import start_scheduler as start_backup_scheduler
start_backup_scheduler()

# Start file indexer (background thread) — scans local_path import collectors
# and caches into file_index table so /local/browse can return in ms (vs NFS walks)
from backend.services.file_indexer import start_scheduler as start_file_indexer
start_file_indexer()

# Resume any interrupted catalog-export requests after process restart
from backend.services import catalog_export_worker as _catalog_export_worker
try:
    _catalog_export_worker.resume_pending_on_startup()
except Exception:
    pass

# Clear stale file-source locks left behind by container restart — pipeline.current_run_id
# pointing at a now-dead thread blocks /run-file-source for 24h otherwise.
from backend.services import pipeline_engine as _pipeline_engine
try:
    _pipeline_engine.clear_stale_file_source_locks()
except Exception:
    pass

# Setup DB logging handler (backend.* → PostgreSQL)
from backend.services.log_handler import setup_db_logging
setup_db_logging()

# Setup API access logging middleware (before/after_request hooks)
from backend.services.api_access_logger import setup_access_logging
setup_access_logging(app)

# Phase 3: Tenant cross-write guard (SQLAlchemy before_flush event).
# MULTITENANT_MODE=off 일 때는 노옵. on 일 땐 g.tenant_id 기반으로
# cross-tenant write 를 자동 차단 (claudedocs/multitenant-test-policy.md § 2.2).
from backend.config import MULTITENANT_MODE as _MT_MODE
app.config["MULTITENANT_MODE"] = _MT_MODE
from backend.services.tenant_guard import install_tenant_guard
install_tenant_guard()


# ── Authentication Middleware ──
@app.before_request
def require_login():
    """세션 또는 X-API-Key 인증 미들웨어.

    우선순위: 화이트리스트 경로 → 세션 → API 키 → 실패 시 401/리다이렉트.
    """
    allowed_paths = ("/login", "/api/admin/auth/login", "/api/admin/lang", "/static/",
                     "/api/storage/file/minio-events")
    if any(request.path == p or request.path.startswith(p) for p in allowed_paths):
        return None
    # 외부 커넥터 콜백은 인증 없이 허용
    if "/callback" in request.path:
        return None
    # CORS preflight
    if request.method == "OPTIONS":
        return None
    # 세션 인증
    if "user_id" in session:
        return None
    # ── [DIAG] 세션이 비어 있을 때 상태를 기록 ──
    try:
        raw_session = request.cookies.get("session")
        _auth_diag.warning(
            "[AUTH-MISS] path=%s method=%s ip=%s ua=%s cookies=%s "
            "session_cookie_len=%s session_cookie_head=%s "
            "session_keys=%s referer=%s",
            request.path, request.method, request.remote_addr,
            (request.user_agent.string or "")[:80],
            list(request.cookies.keys()),
            len(raw_session) if raw_session else 0,
            (raw_session[:25] + "...") if raw_session else None,
            list(session.keys()),
            request.headers.get("Referer", ""),
        )
    except Exception:
        _auth_diag.exception("[AUTH-MISS] logging failed")
    # API 경로: X-API-Key 검증
    if request.path.startswith("/api/"):
        from backend.services.api_auth import authenticate_api_key
        ok, err = authenticate_api_key()
        if ok is True:
            from flask import g
            g.api_key_authenticated = True
            return None
        if ok is False:
            code, msg, status = err
            return jsonify({"success": False, "data": None,
                            "error": {"code": code, "message": msg}}), status
        # 헤더 없음 → 세션도 없음 → 인증 실패
        return jsonify({"success": False, "data": None,
                        "error": {"code": "UNAUTHORIZED",
                                  "message": "로그인 또는 API 키가 필요합니다."}}), 401
    return redirect("/login")


# ── Tenant Context Injection (Phase 2) ──
# claudedocs/multitenant-design-v1.md § 6 — 세션/API 키에서 tenant_id 등을 g 에 주입.
# require_login 다음, enforce_rbac 직전에 실행되어야 한다 (등록 순서로 보장).
@app.before_request
def inject_tenant_context():
    """g.tenant_id / g.tenant_role / g.is_super 주입.

    MULTITENANT_MODE=off 일 때도 안전한 기본값(tenant_id=1, tenant_viewer)을 주입한다.
    Phase 4 이후 라우트가 g.tenant_id 로 쿼리 필터링을 적용한다.
    """
    allowed_paths = ("/login", "/api/admin/auth/login", "/api/admin/lang", "/static/",
                     "/api/storage/file/minio-events")
    if any(request.path == p or request.path.startswith(p) for p in allowed_paths):
        return None
    if "/callback" in request.path or request.method == "OPTIONS":
        return None
    from flask import g
    # API 키 인증 경로: authenticate_api_key 가 향후(Phase 6) g.tenant_id 를 세팅한다.
    # 그 전엔 키도 tenant=1 으로 본다.
    if getattr(g, "tenant_id", None) is None:
        g.tenant_id = session.get("tenant_id", 1) if "user_id" in session else 1
    if getattr(g, "tenant_role", None) is None:
        g.tenant_role = session.get("tenant_role", "tenant_viewer")
    if getattr(g, "is_super", None) is None:
        g.is_super = bool(session.get("is_super", False))
    return None


# ── Path Class Gate (Phase 7) ──
@app.before_request
def enforce_path_class():
    """경로 4분류 가드 - SYSTEM_ONLY / TENANT_ADMIN_ONLY 등."""
    allowed_paths = ("/login", "/api/admin/auth/login", "/api/admin/lang", "/static/",
                     "/api/storage/file/minio-events")
    from flask import request as _req
    if any(_req.path == p or _req.path.startswith(p) for p in allowed_paths):
        return None
    if "/callback" in _req.path or _req.method == "OPTIONS":
        return None
    if "user_id" not in session and not getattr(__import__("flask").g, "api_key_authenticated", False):
        return None  # 안전망
    from backend.services.rbac import enforce_class_gate
    return enforce_class_gate()


# ── RBAC Middleware ──
@app.before_request
def enforce_rbac():
    """역할 기반 접근 제어 — require_login 통과 후 호출.

    정책: GET = 인증된 모든 사용자, 변경 = admin 만.
    예외 매트릭스는 backend/services/rbac.py 참조.
    """
    # 인증 면제 경로 (login/static/callback) 는 require_login 에서 이미 통과
    allowed_paths = ("/login", "/api/admin/auth/login", "/api/admin/lang", "/static/",
                     "/api/storage/file/minio-events")
    if any(request.path == p or request.path.startswith(p) for p in allowed_paths):
        return None
    if "/callback" in request.path:
        return None
    if request.method == "OPTIONS":
        return None
    # 인증 통과 못한 경우는 require_login 에서 이미 응답을 반환했음 → 여기는 인증 후
    # 단, 비-API 페이지(GET /admin/users 등)도 동일 정책 적용 — 화면 자체를 막음
    if "user_id" not in session and not getattr(__import__("flask").g, "api_key_authenticated", False):
        return None  # 안전망: 인증 미통과는 require_login 이 처리
    from backend.services.rbac import enforce_request_rbac
    return enforce_request_rbac()


# ── Jinja Context: is_admin + tenant info (Phase 2 확장) ──
@app.context_processor
def _inject_rbac():
    from backend.services.rbac import (
        is_admin, current_role,
        current_tenant_id, current_tenant_role, is_super, is_impersonating,
    )
    _ctr = current_tenant_role()
    _isuper = is_super()
    return {
        "is_admin": is_admin(),
        "current_role": current_role(),
        # Phase 2: 템플릿에서도 4-role / tenant 컨텍스트 노출. MULTITENANT_MODE=off 일 땐
        # 안전 기본값(1 / tenant_viewer / False) 가 반환된다.
        "current_tenant_id": current_tenant_id(),
        "current_tenant_role": _ctr,
        "is_super": _isuper,
        "is_impersonating": is_impersonating(),
        # Phase 8: 4-role 직접 확인용 헬퍼 (사이드바·페이지 가시성 분기에 사용)
        "is_tenant_admin": _isuper or _ctr in ("tenant_admin", "super_admin"),
        "is_tenant_editor": _isuper or _ctr in ("tenant_admin", "super_admin", "tenant_editor"),
    }


# ── Login Page ──
@app.route("/login")
def login_page():
    if "user_id" in session:
        return redirect("/")
    from backend.models.user import AdminSetting
    from backend.database import SessionLocal
    db = SessionLocal()
    row = db.query(AdminSetting).filter_by(key="system.name").first()
    sys_name = row.value if row else "SR DataLake"
    db.close()
    return render_template("login.html", system_name=sys_name, i18n_all=get_all_translations(get_current_lang()))


# ── Dashboard ──
@app.route("/")
def dashboard():
    return render_template("dashboard.html", active="dashboard")


# ── Data Collection: Connectors ──
@app.route("/collection/connectors/opcua")
def connector_opcua():
    return render_template("collection/opcua.html", active="conn-opcua")

@app.route("/collection/connectors/modbus")
def connector_modbus():
    return render_template("collection/modbus.html", active="conn-modbus")

@app.route("/collection/connectors/mqtt")
def connector_mqtt():
    return render_template("collection/mqtt.html", active="conn-mqtt")

@app.route("/collection/connectors/api")
def connector_api():
    return render_template("collection/api_connector.html", active="conn-api")

@app.route("/collection/connectors/file")
def connector_file():
    return render_template("collection/file_collector.html", active="conn-file")

@app.route("/collection/connectors/db")
def connector_db():
    return render_template("collection/db_connector.html", active="conn-db")

@app.route("/collection/connectors/import")
def connector_import():
    return render_template("collection/import_collector.html", active="conn-import")


# ── Data Collection: Engine ──
@app.route("/collection/engine/status")
def engine_status():
    return render_template("collection/engine_status.html", active="eng-status")

@app.route("/collection/engine/batch")
def engine_batch():
    return render_template("collection/engine_batch.html", active="eng-batch")

@app.route("/collection/engine/buffer")
def engine_buffer():
    return render_template("collection/engine_buffer.html", active="eng-buffer")

@app.route("/collection/engine/performance")
def engine_performance():
    return render_template("collection/engine_performance.html", active="eng-perf")


# ── Pipeline ──
@app.route("/pipeline/builder")
def pipeline_builder():
    return render_template("pipeline/builder.html", active="pip-builder")

@app.route("/pipeline/modules/normalize")
def module_normalize():
    return render_template("pipeline/normalize.html", active="mod-normalize")

@app.route("/pipeline/modules/unit-convert")
def module_unit_convert():
    return render_template("pipeline/unit_convert.html", active="mod-unit")

@app.route("/pipeline/modules/filter")
def module_filter():
    return render_template("pipeline/filter.html", active="mod-filter")

@app.route("/pipeline/modules/anomaly")
def module_anomaly():
    return render_template("pipeline/anomaly.html", active="mod-anomaly")

@app.route("/pipeline/modules/aggregate")
def module_aggregate():
    return render_template("pipeline/aggregate.html", active="mod-aggregate")

@app.route("/pipeline/modules/enrich")
def module_enrich():
    return render_template("pipeline/enrich.html", active="mod-enrich")

@app.route("/pipeline/modules/script")
def module_script():
    return render_template("pipeline/script.html", active="mod-script")

@app.route("/pipeline/queue")
def pipeline_queue():
    return render_template("pipeline/queue.html", active="pip-queue")

@app.route("/pipeline/metadata")
def pipeline_metadata():
    return render_template("pipeline/metadata.html", active="data-metadata")

@app.route("/pipeline/catalog")
def pipeline_catalog():
    return render_template("pipeline/catalog.html", active="data-catalog")


# ── Storage ──
@app.route("/storage/tsdb")
def storage_tsdb():
    return render_template("storage/tsdb.html", active="stor-tsdb")

@app.route("/storage/file")
def storage_file():
    return render_template("storage/file_storage.html", active="stor-file")

@app.route("/storage/rdbms")
def storage_rdbms():
    return render_template("storage/rdbms.html", active="stor-rdbms")

@app.route("/storage/retention")
def storage_retention():
    return render_template("storage/retention.html", active="stor-retention")


# ── Integration ──
@app.route("/integration/tsdb")
def integration_tsdb():
    return render_template("integration/ext_tsdb.html", active="int-tsdb")

@app.route("/integration/rdbms")
def integration_rdbms():
    return render_template("integration/ext_rdbms.html", active="int-rdbms")

@app.route("/integration/kafka")
def integration_kafka():
    return redirect("/integration/messaging")

@app.route("/integration/messaging")
def integration_messaging():
    return render_template("integration/ext_messaging.html", active="int-messaging")

@app.route("/integration/file")
def integration_file():
    return render_template("integration/ext_file.html", active="int-file")


# ── Monitoring ──
@app.route("/monitoring/dashboard")
def monitoring_dashboard():
    return render_template("monitoring/ops_dashboard.html", active="mon-dashboard")

@app.route("/monitoring/healthcheck")
def monitoring_healthcheck():
    return render_template("monitoring/healthcheck.html", active="mon-health")

@app.route("/monitoring/alarm")
def monitoring_alarm():
    return render_template("monitoring/alarm.html", active="mon-alarm")

@app.route("/monitoring/logs/system")
def monitoring_system_log():
    return render_template("monitoring/system_log.html", active="mon-syslog")

@app.route("/monitoring/logs/audit")
def monitoring_audit_log():
    return render_template("monitoring/audit_log.html", active="mon-audit")


# ── Admin ──
@app.route("/admin/users")
def admin_users():
    return render_template("admin/users.html", active="adm-users")


@app.route("/admin/infra/backup")
def admin_backup():
    return render_template("admin/backup.html", active="adm-backup")

@app.route("/admin/gateway")
def admin_gateway():
    return render_template("admin/gateway.html", active="adm-gateway")

@app.route("/admin/settings")
def admin_settings():
    return render_template("admin/settings.html", active="adm-settings")


if __name__ == "__main__":
    app.run(debug=True, port=5001)


# Phase 7 - 테넌트 관리 (super_admin 전용)
@app.route("/admin/sys/tenants")
def page_sys_tenants():
    from backend.services.rbac import is_super
    if "user_id" not in session:
        return redirect("/login")
    if not is_super():
        return redirect("/")
    return render_template("admin/tenants.html", active="adm-sys-tenants")
