"""DB writer 시뮬레이터.

sdl-postgres 의 지정 schema/table 에 주기적으로 스냅샷 INSERT.
스키마·테이블은 부팅 시 자동 생성 (CREATE IF NOT EXISTS).
"""
import logging
import time

import psycopg2

log = logging.getLogger("sim.db")


DDL_TEMPLATE = """
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id           BIGSERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL,
    asset_id     TEXT NOT NULL,
    temperature  DOUBLE PRECISION,
    humidity     DOUBLE PRECISION,
    pressure     DOUBLE PRECISION,
    mode         TEXT,
    total_cycle  BIGINT,
    last_error   TEXT,
    firmware_ver TEXT
);
CREATE INDEX IF NOT EXISTS ix_{table}_ts ON {schema}.{table}(ts DESC);
"""

INSERT_TEMPLATE = """
INSERT INTO {schema}.{table}
  (ts, asset_id, temperature, humidity, pressure, mode, total_cycle, last_error, firmware_ver)
VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _connect(cfg):
    return psycopg2.connect(
        host=cfg.get("host", "sdl-postgres"),
        port=int(cfg.get("port", 5432)),
        dbname=cfg.get("database", "sdl"),
        user=cfg.get("user", "sdl_user"),
        password=cfg.get("password", ""),
        connect_timeout=10,
    )


def _ensure_schema(cfg):
    schema = cfg.get("schema", "sim")
    table = cfg.get("table", "press01_readings")
    conn = _connect(cfg)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(DDL_TEMPLATE.format(schema=schema, table=table))
        log.info("db schema ready: %s.%s", schema, table)
    finally:
        conn.close()


def run(state, cfg):
    if not cfg.get("enabled", True):
        log.info("db disabled")
        return

    schema = cfg.get("schema", "sim")
    table = cfg.get("table", "press01_readings")
    interval = float(cfg.get("write_interval_sec", 10.0))

    # 부팅 재시도 (postgres 준비 대기)
    for i in range(20):
        try:
            _ensure_schema(cfg)
            break
        except Exception as e:
            log.warning("db init retry %d: %s", i, e)
            time.sleep(3)
    else:
        log.error("db init failed after retries; aborting db sim")
        return

    insert_sql = INSERT_TEMPLATE.format(schema=schema, table=table)
    conn = None
    while True:
        snap = state.snapshot()
        try:
            if conn is None or conn.closed:
                conn = _connect(cfg)
            with conn.cursor() as cur:
                cur.execute(insert_sql, (
                    snap.ts, snap.asset_id, snap.temperature, snap.humidity, snap.pressure,
                    snap.mode, snap.total_cycle, snap.last_error, snap.firmware_ver,
                ))
            conn.commit()
        except Exception as e:
            log.warning("db insert error: %s", e)
            try:
                conn.close()
            except Exception:
                pass
            conn = None
        time.sleep(interval)
