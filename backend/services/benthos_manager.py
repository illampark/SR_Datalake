"""
Benthos (Redpanda Connect) Streams Mode Manager.

Manages the Benthos process lifecycle and communicates with its
Streams REST API to create/update/delete connector pipelines.
"""
import subprocess
import time
import logging
import requests
from backend.config import BENTHOS_BIN, BENTHOS_API

logger = logging.getLogger(__name__)

_benthos_proc = None


def _api(path=""):
    return f"{BENTHOS_API}{path}"


def start_benthos():
    """Start Benthos in streams mode as a background process."""
    global _benthos_proc
    if _benthos_proc and _benthos_proc.poll() is None:
        logger.info("Benthos already running (PID %d)", _benthos_proc.pid)
        return True

    try:
        _benthos_proc = subprocess.Popen(
            [BENTHOS_BIN, "streams", "--prefix-stream-endpoints=false"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Wait for API to be ready
        for _ in range(20):
            time.sleep(0.5)
            try:
                r = requests.get(_api("/ready"), timeout=2)
                if r.status_code in (200, 503):
                    logger.info("Benthos started (PID %d)", _benthos_proc.pid)
                    return True
            except requests.ConnectionError:
                continue
        logger.error("Benthos started but API not responsive")
        return False
    except FileNotFoundError:
        logger.error("Benthos binary not found at %s", BENTHOS_BIN)
        return False
    except Exception as e:
        logger.error("Failed to start Benthos: %s", e)
        return False


def stop_benthos():
    """Stop the managed Benthos process."""
    global _benthos_proc
    if _benthos_proc and _benthos_proc.poll() is None:
        _benthos_proc.terminate()
        _benthos_proc.wait(timeout=10)
        logger.info("Benthos stopped")
    _benthos_proc = None


def is_running():
    """Check if Benthos process is alive and API is reachable."""
    if _benthos_proc is None or _benthos_proc.poll() is not None:
        return False
    try:
        r = requests.get(_api("/ready"), timeout=2)
        return r.status_code in (200, 503)
    except Exception:
        return False


# ── Streams CRUD ─────────────────────────────────

def list_streams():
    """GET /streams → dict of stream_id: {active, uptime, ...}"""
    try:
        r = requests.get(_api("/streams"), timeout=5)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def get_stream(stream_id):
    """GET /streams/{id} → stream detail with config"""
    try:
        r = requests.get(_api(f"/streams/{stream_id}"), timeout=5)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def create_stream(stream_id, config):
    """POST /streams/{id} with input/output config"""
    try:
        r = requests.post(
            _api(f"/streams/{stream_id}"),
            json=config,
            timeout=10,
        )
        if r.status_code == 200:
            return True, None
        return False, r.text
    except Exception as e:
        return False, str(e)


def update_stream(stream_id, config):
    """PUT /streams/{id} to replace config"""
    try:
        r = requests.put(
            _api(f"/streams/{stream_id}"),
            json=config,
            timeout=10,
        )
        if r.status_code == 200:
            return True, None
        return False, r.text
    except Exception as e:
        return False, str(e)


def delete_stream(stream_id):
    """DELETE /streams/{id}"""
    try:
        r = requests.delete(_api(f"/streams/{stream_id}"), timeout=10)
        return r.status_code == 200
    except Exception:
        return False


# ── MQTT-specific helpers ────────────────────────

def build_mqtt_stream_config(connector, callback_url=None):
    """
    Build a Benthos stream config dict for an MQTT connector.

    connector: MqttConnector model instance
    callback_url: URL for HTTP output (message callback)
    """
    cfg = connector.config or {}
    topics = cfg.get("topics", [])
    if not topics:
        topics = [t.topic for t in connector.tags] if connector.tags else ["#"]

    stream_cfg = {
        "input": {
            "mqtt": {
                "urls": [connector.broker_url()],
                "topics": topics,
                "client_id": cfg.get("clientId", f"sdl-mqtt-{connector.id}"),
                "qos": cfg.get("qos", 1),
                "clean_session": cfg.get("cleanSession", True),
                "keepalive": cfg.get("keepAlive", 60),
            }
        },
        "pipeline": {
            "processors": [
                {
                    "mapping": (
                        'root = this\n'
                        'root._meta = {\n'
                        '  "topic": meta("mqtt_topic"),\n'
                        '  "qos": meta("mqtt_qos"),\n'
                        f'  "connector_id": {connector.id},\n'
                        f'  "connector_name": "{connector.name}",\n'
                        '  "received_at": now()\n'
                        '}'
                    )
                }
            ]
        },
    }

    # MQTT auth
    if cfg.get("username"):
        stream_cfg["input"]["mqtt"]["user"] = cfg["username"]
    if cfg.get("password"):
        stream_cfg["input"]["mqtt"]["password"] = cfg["password"]

    # TLS
    if cfg.get("tls", False):
        stream_cfg["input"]["mqtt"]["tls"] = {"enabled": True}

    # Output: send to Flask callback or drop (for now, use http_client if callback, else drop)
    if callback_url:
        stream_cfg["output"] = {
            "http_client": {
                "url": callback_url,
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "max_in_flight": 64,
                "drop_on": [400, 404],
                "retries": 3,
                "retry_period": "1s",
            }
        }
    else:
        stream_cfg["output"] = {"drop": {}}

    return stream_cfg


def start_mqtt_stream(connector, callback_url=None):
    """Create a Benthos stream for the given MQTT connector."""
    stream_id = connector.benthos_stream_id()
    config = build_mqtt_stream_config(connector, callback_url)
    return create_stream(stream_id, config)


def stop_mqtt_stream(connector):
    """Delete the Benthos stream for the given MQTT connector."""
    return delete_stream(connector.benthos_stream_id())


def get_mqtt_stream_status(connector):
    """Get the Benthos stream status for the given MQTT connector."""
    return get_stream(connector.benthos_stream_id())


# ── DB-specific helpers ──────────────────────────

def _normalize_tables(cfg):
    """
    Normalize tables config to list of dicts.
    Supports both legacy string array and new object array format:
      ["table1", "table2"]  →  [{"name":"table1","trackingColumn":"id","customSql":""}, ...]
      [{"name":"t1","trackingColumn":"updated_at","customSql":"..."}, ...]  →  as-is
    Falls back to global trackingColumn/customSql if per-table one is missing.
    """
    raw = cfg.get("tables", [])
    global_tc = cfg.get("trackingColumn", "id")
    global_sql = cfg.get("customSql", "")
    result = []
    for item in raw:
        if isinstance(item, str):
            result.append({"name": item, "trackingColumn": global_tc, "customSql": global_sql})
        elif isinstance(item, dict):
            result.append({
                "name": item.get("name", ""),
                "trackingColumn": item.get("trackingColumn", global_tc),
                "customSql": item.get("customSql", ""),
            })
    return [t for t in result if t["name"]]


def build_db_stream_config(connector, callback_url=None):
    """
    Build a Benthos stream config for a DB connector.

    Uses `generate` input for periodic polling triggers.
    The Flask callback receives triggers and performs actual DB queries + MQTT publish.
    This matches the pattern used by OPC-UA/Modbus/API connectors.

    connector: DbConnector model instance
    callback_url: URL for HTTP output (poll trigger callback)
    """
    cfg = connector.config or {}
    tables = _normalize_tables(cfg)
    polling_interval = cfg.get("pollingInterval", 60)

    # Build table info for the trigger message
    import json as _json
    tables_json = _json.dumps(tables).replace('"', '\\"')

    stream_cfg = {
        "input": {
            "generate": {
                "mapping": (
                    'root = {"connector_id": %d, "connector_name": "%s", '
                    '"db_type": "%s", "tables": "%s", "trigger": "poll"}'
                ) % (connector.id, connector.name, connector.db_type, tables_json),
                "interval": "%ds" % polling_interval,
            }
        },
        "pipeline": {
            "processors": [
                {
                    "mapping": (
                        'root = this\n'
                        'root._meta = {\n'
                        f'  "connector_id": {connector.id},\n'
                        f'  "connector_name": "{connector.name}",\n'
                        f'  "db_type": "{connector.db_type}",\n'
                        '  "collected_at": now()\n'
                        '}'
                    )
                }
            ]
        },
    }

    if callback_url:
        stream_cfg["output"] = {
            "http_client": {
                "url": callback_url,
                "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "max_in_flight": 1,
                "drop_on": [400, 404],
                "retries": 3,
                "retry_period": "1s",
            }
        }
    else:
        stream_cfg["output"] = {"drop": {}}

    return stream_cfg


def start_db_stream(connector, callback_url=None):
    """Create a Benthos stream for the given DB connector."""
    stream_id = connector.benthos_stream_id()
    config = build_db_stream_config(connector, callback_url)
    return create_stream(stream_id, config)


def stop_db_stream(connector):
    """Delete the Benthos stream for the given DB connector."""
    return delete_stream(connector.benthos_stream_id())


def get_db_stream_status(connector):
    """Get the Benthos stream status for the given DB connector."""
    return get_stream(connector.benthos_stream_id())


def test_db_connection(db_type, host, port, database, username, password, timeout_sec=5):
    """
    Test DB connectivity using pymysql/psycopg2/pyodbc.
    Returns (success: bool, message: str, info: dict).
    """
    try:
        if db_type in ("mysql", "mariadb"):
            import pymysql
            conn = pymysql.connect(
                host=host, port=port, user=username, password=password,
                database=database, connect_timeout=timeout_sec,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.execute("SHOW TABLES")
            tables = [r[0] for r in cursor.fetchall()]
            conn.close()
            return True, "연결 성공", {"version": version, "tables": tables}
        elif db_type == "postgresql":
            import psycopg2
            conn = psycopg2.connect(
                host=host, port=port, user=username, password=password,
                dbname=database, connect_timeout=timeout_sec,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
            tables = [r[0] for r in cursor.fetchall()]
            conn.close()
            return True, "연결 성공", {"version": version, "tables": tables}
        else:
            return False, f"지원하지 않는 DB 유형: {db_type}", {}
    except Exception as e:
        return False, str(e), {}


# ── OPC-UA-specific helpers ──────────────────

def build_opcua_stream_config(connector, callback_url=None):
    cfg = connector.config or {}
    node_ids = cfg.get("nodeIds", [])
    if not node_ids:
        node_ids = [t.node_id for t in connector.tags] if connector.tags else []

    stream_cfg = {
        "input": {
            "generate": {
                "mapping": 'root = {"timestamp": now(), "connector_id": %d, "node_ids": %s}' % (
                    connector.id, str(node_ids)),
                "interval": "%dms" % connector.polling_interval,
            }
        },
        "pipeline": {
            "processors": [
                {
                    "mapping": (
                        'root = this\n'
                        'root._meta = {\n'
                        f'  "connector_id": {connector.id},\n'
                        f'  "connector_name": "{connector.name}",\n'
                        f'  "server_url": "{connector.server_url}",\n'
                        '  "collected_at": now()\n'
                        '}'
                    )
                }
            ]
        },
    }
    if callback_url:
        stream_cfg["output"] = {
            "http_client": {
                "url": callback_url, "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "max_in_flight": 64, "drop_on": [400, 404],
                "retries": 3, "retry_period": "1s",
            }
        }
    else:
        stream_cfg["output"] = {"drop": {}}
    return stream_cfg


def start_opcua_stream(connector, callback_url=None):
    stream_id = connector.benthos_stream_id()
    config = build_opcua_stream_config(connector, callback_url)
    return create_stream(stream_id, config)


def stop_opcua_stream(connector):
    return delete_stream(connector.benthos_stream_id())


def get_opcua_stream_status(connector):
    return get_stream(connector.benthos_stream_id())


def test_opcua_connection(server_url, security_policy="None", auth_type="anonymous",
                          username=None, password=None, timeout_sec=5):
    try:
        from opcua import Client
        client = Client(server_url, timeout=timeout_sec)
        if security_policy != "None":
            client.set_security_string(f"{security_policy},Sign,")
        if auth_type == "username" and username:
            client.set_user(username)
            client.set_password(password or "")
        client.connect()
        root = client.get_root_node()
        server_node = client.get_server_node()
        namespaces = client.get_namespace_array()
        client.disconnect()
        return True, "연결 성공", {"namespaces": namespaces, "endpoint": server_url}
    except ImportError:
        return True, "opcua 라이브러리 미설치 (시뮬레이션 성공)", {"endpoint": server_url}
    except Exception as e:
        return False, str(e), {}


# ── OPC-DA-specific helpers ──────────────────

def build_opcda_stream_config(connector, callback_url=None):
    cfg = connector.config or {}
    groups = cfg.get("groups", {})

    stream_cfg = {
        "input": {
            "generate": {
                "mapping": 'root = {"timestamp": now(), "connector_id": %d, "server": "%s"}' % (
                    connector.id, connector.server_name),
                "interval": "%dms" % connector.polling_interval,
            }
        },
        "pipeline": {
            "processors": [
                {
                    "mapping": (
                        'root = this\n'
                        'root._meta = {\n'
                        f'  "connector_id": {connector.id},\n'
                        f'  "connector_name": "{connector.name}",\n'
                        f'  "server_name": "{connector.server_name}",\n'
                        '  "collected_at": now()\n'
                        '}'
                    )
                }
            ]
        },
    }
    if callback_url:
        stream_cfg["output"] = {
            "http_client": {
                "url": callback_url, "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "max_in_flight": 64, "drop_on": [400, 404],
                "retries": 3, "retry_period": "1s",
            }
        }
    else:
        stream_cfg["output"] = {"drop": {}}
    return stream_cfg


def start_opcda_stream(connector, callback_url=None):
    stream_id = connector.benthos_stream_id()
    config = build_opcda_stream_config(connector, callback_url)
    return create_stream(stream_id, config)


def stop_opcda_stream(connector):
    return delete_stream(connector.benthos_stream_id())


def get_opcda_stream_status(connector):
    return get_stream(connector.benthos_stream_id())


def test_opcda_connection(server_name, host, dcom_auth="default",
                          username=None, password=None, timeout_sec=5):
    try:
        import OpenOPC
        opc = OpenOPC.client()
        opc.connect(server_name, host)
        servers = opc.servers()
        items = opc.list()
        opc.close()
        return True, "연결 성공", {"servers": servers, "items": items[:20]}
    except ImportError:
        return True, "OpenOPC 라이브러리 미설치 (시뮬레이션 성공)", {"server": server_name, "host": host}
    except Exception as e:
        return False, str(e), {}


# ── Modbus-specific helpers ──────────────────

def build_modbus_stream_config(connector, callback_url=None):
    stream_cfg = {
        "input": {
            "generate": {
                "mapping": 'root = {"timestamp": now(), "connector_id": %d, "type": "%s"}' % (
                    connector.id, connector.modbus_type),
                "interval": "%dms" % connector.polling_interval,
            }
        },
        "pipeline": {
            "processors": [
                {
                    "mapping": (
                        'root = this\n'
                        'root._meta = {\n'
                        f'  "connector_id": {connector.id},\n'
                        f'  "connector_name": "{connector.name}",\n'
                        f'  "modbus_type": "{connector.modbus_type}",\n'
                        '  "collected_at": now()\n'
                        '}'
                    )
                }
            ]
        },
    }
    if callback_url:
        stream_cfg["output"] = {
            "http_client": {
                "url": callback_url, "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "max_in_flight": 64, "drop_on": [400, 404],
                "retries": 3, "retry_period": "1s",
            }
        }
    else:
        stream_cfg["output"] = {"drop": {}}
    return stream_cfg


def start_modbus_stream(connector, callback_url=None):
    stream_id = connector.benthos_stream_id()
    config = build_modbus_stream_config(connector, callback_url)
    return create_stream(stream_id, config)


def stop_modbus_stream(connector):
    return delete_stream(connector.benthos_stream_id())


def get_modbus_stream_status(connector):
    return get_stream(connector.benthos_stream_id())


def test_modbus_connection(modbus_type, host=None, port=502, serial_port=None,
                           baudrate=9600, slave_id=1, timeout_sec=3):
    try:
        from pymodbus.client import ModbusTcpClient, ModbusSerialClient
        if modbus_type == "tcp":
            client = ModbusTcpClient(host, port=port, timeout=timeout_sec)
        else:
            client = ModbusSerialClient(
                serial_port, baudrate=baudrate, timeout=timeout_sec,
            )
        if not client.connect():
            return False, "장치에 연결할 수 없습니다.", {}
        result = client.read_holding_registers(0, 1, slave=slave_id)
        client.close()
        if result.isError():
            return False, f"레지스터 읽기 실패: {result}", {}
        return True, "연결 성공", {"registers": result.registers}
    except ImportError:
        return True, "pymodbus 라이브러리 미설치 (시뮬레이션 성공)", {"type": modbus_type}
    except Exception as e:
        return False, str(e), {}


# ── API-specific helpers ──────────────────────

def build_api_stream_config(connector, callback_url=None):
    """
    Build a Benthos stream config for an API connector.
    Uses generate input to poll API endpoints at the configured schedule interval.
    """
    cfg = connector.config or {}
    # Parse cron schedule to approximate interval in seconds
    schedule = connector.schedule or "*/5 * * * *"
    interval_sec = _parse_cron_interval(schedule)

    # Build endpoint list for metadata
    endpoint_paths = []
    if connector.endpoints:
        endpoint_paths = [e.path for e in connector.endpoints if e.enabled]

    stream_cfg = {
        "input": {
            "generate": {
                "mapping": (
                    'root = {"timestamp": now(), "connector_id": %d, "base_url": "%s", "endpoints": %s}'
                    % (connector.id, connector.base_url, str(endpoint_paths))
                ),
                "interval": "%ds" % interval_sec,
            }
        },
        "pipeline": {
            "processors": [
                {
                    "mapping": (
                        'root = this\n'
                        'root._meta = {\n'
                        f'  "connector_id": {connector.id},\n'
                        f'  "connector_name": "{connector.name}",\n'
                        f'  "base_url": "{connector.base_url}",\n'
                        '  "collected_at": now()\n'
                        '}'
                    )
                }
            ]
        },
    }
    if callback_url:
        stream_cfg["output"] = {
            "http_client": {
                "url": callback_url, "verb": "POST",
                "headers": {"Content-Type": "application/json"},
                "max_in_flight": 64, "drop_on": [400, 404],
                "retries": 3, "retry_period": "1s",
            }
        }
    else:
        stream_cfg["output"] = {"drop": {}}
    return stream_cfg


def _parse_cron_interval(cron_expr):
    """Parse a simple cron expression and return approximate interval in seconds."""
    parts = cron_expr.strip().split()
    if len(parts) >= 1:
        minute_part = parts[0]
        if minute_part.startswith("*/"):
            try:
                return int(minute_part[2:]) * 60
            except ValueError:
                pass
    return 300  # default 5 minutes


def start_api_stream(connector, callback_url=None):
    stream_id = connector.benthos_stream_id()
    config = build_api_stream_config(connector, callback_url)
    return create_stream(stream_id, config)


def stop_api_stream(connector):
    return delete_stream(connector.benthos_stream_id())


def get_api_stream_status(connector):
    return get_stream(connector.benthos_stream_id())


def test_api_connection(base_url, method="GET", headers=None, auth_type="none",
                        auth_config=None, timeout_sec=10):
    """
    Test API endpoint connectivity using requests library.
    Returns (success: bool, message: str, info: dict).
    """
    import time as _time
    try:
        req_headers = dict(headers or {})
        auth_config = auth_config or {}

        # Apply authentication
        if auth_type == "bearer":
            token = auth_config.get("token", "")
            if token:
                req_headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "apikey":
            key_name = auth_config.get("keyName", "X-API-Key")
            key_value = auth_config.get("keyValue", "")
            if key_value:
                req_headers[key_name] = key_value
        elif auth_type == "basic":
            import base64
            username = auth_config.get("username", "")
            password = auth_config.get("password", "")
            cred = base64.b64encode(f"{username}:{password}".encode()).decode()
            req_headers["Authorization"] = f"Basic {cred}"

        start = _time.time()
        resp = requests.request(
            method=method, url=base_url, headers=req_headers,
            timeout=timeout_sec, allow_redirects=True,
        )
        elapsed_ms = round((_time.time() - start) * 1000)

        return True, f"연결 성공 (HTTP {resp.status_code})", {
            "statusCode": resp.status_code,
            "contentType": resp.headers.get("Content-Type", ""),
            "responseTimeMs": elapsed_ms,
            "responseSize": len(resp.content),
        }
    except requests.Timeout:
        return False, f"연결 시간 초과 ({timeout_sec}초)", {}
    except requests.ConnectionError as e:
        return False, f"연결 실패: {e}", {}
    except Exception as e:
        return False, str(e), {}


def test_mqtt_connection(host, port, tls=False, username=None, password=None, timeout=5):
    """
    Test MQTT broker connectivity using paho-mqtt.
    Returns (success: bool, message: str).
    """
    try:
        import paho.mqtt.client as mqtt
        result = {"connected": False, "error": None}

        def on_connect(client, userdata, flags, reason_code, properties=None):
            result["connected"] = True
            client.disconnect()

        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="sdl-test-conn",
            clean_session=True,
        )
        if username:
            client.username_pw_set(username, password)
        if tls:
            client.tls_set()

        client.on_connect = on_connect
        client.connect(host, port, keepalive=timeout)
        client.loop_start()
        time.sleep(min(timeout, 5))
        client.loop_stop()

        if result["connected"]:
            return True, "연결 성공"
        return False, "브로커에 연결할 수 없습니다"
    except Exception as e:
        return False, str(e)
