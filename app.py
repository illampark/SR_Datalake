from datetime import timedelta
from flask import Flask, render_template, request, redirect, session, jsonify
from backend.config import SECRET_KEY

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max upload
app.secret_key = SECRET_KEY
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=480)

# ── Backend API Setup ──
from backend.database import init_db
from backend.routes import tsdb_bp, rdbms_bp, file_bp, mqtt_bp, db_bp, file_watch_bp, retention_bp
from backend.routes import opcua_bp, opcda_bp, modbus_bp, api_bp
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

app.register_blueprint(tsdb_bp)
app.register_blueprint(rdbms_bp)
app.register_blueprint(file_bp)
app.register_blueprint(mqtt_bp)
app.register_blueprint(db_bp)
app.register_blueprint(file_watch_bp)
app.register_blueprint(opcua_bp)
app.register_blueprint(opcda_bp)
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

# Setup DB logging handler (backend.* → PostgreSQL)
from backend.services.log_handler import setup_db_logging
setup_db_logging()

# Setup API access logging middleware (before/after_request hooks)
from backend.services.api_access_logger import setup_access_logging
setup_access_logging(app)


# ── Authentication Middleware ──
@app.before_request
def require_login():
    """세션 기반 인증 미들웨어 — 미인증 시 /login 리다이렉트 또는 401"""
    allowed_paths = ("/login", "/api/admin/auth/login", "/api/admin/lang", "/static/")
    if any(request.path == p or request.path.startswith(p) for p in allowed_paths):
        return None
    # 외부 커넥터 콜백은 인증 없이 허용
    if "/callback" in request.path:
        return None
    if "user_id" not in session:
        if request.path.startswith("/api/"):
            return jsonify({"success": False, "data": None,
                            "error": {"code": "UNAUTHORIZED",
                                      "message": "로그인이 필요합니다."}}), 401
        return redirect("/login")
    return None


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

@app.route("/collection/connectors/opcda")
def connector_opcda():
    return render_template("collection/opcda.html", active="conn-opcda")

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
    return render_template("integration/ext_kafka.html", active="int-kafka")

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
