"""
Retention Data Transition Executor.

Implements the 3-tier data lifecycle transitions:
  Hot (TimescaleDB) → Warm (RDBMS): Downsampling with 1-min aggregation
  Warm (RDBMS) → Cold (MinIO): CSV archival to S3
  Cold (MinIO): Expiry deletion of aged objects
"""
import io
import csv
import time
import logging
import psycopg2
from datetime import datetime, timedelta

from minio.deleteobjects import DeleteObject
from minio.error import S3Error

from backend.database import SessionLocal
from backend.models.storage import (
    TsdbConfig, RdbmsConfig, RetentionPolicy, RetentionExecutionLog,
    WarmAggregatedData,
)
from backend.services.minio_client import get_minio_client

logger = logging.getLogger(__name__)


# ── Public API ──────────────────────────────────────


def execute_downsampling():
    """Hot → Warm: TimescaleDB 만료 데이터를 1분 집계하여 RDBMS에 저장 후 원본 삭제."""
    db = SessionLocal()
    t0 = time.time()
    total_bytes = 0
    total_records = 0
    status = "success"
    error_msg = ""

    try:
        policy = db.query(RetentionPolicy).first()
        hot_days = policy.hot_retention_days if policy else 30
        cutoff = datetime.utcnow() - timedelta(days=hot_days)

        tsdb_config = db.query(TsdbConfig).first()
        if not tsdb_config:
            error_msg = "TimescaleDB 인스턴스가 설정되지 않았습니다."
            return

        tsdb_conn = _get_tsdb_connection(tsdb_config)
        if not tsdb_conn:
            status = "partial"
            error_msg = "TimescaleDB 연결 실패"
            return

        try:
            tables = _discover_tsdb_tables(tsdb_conn)

            if not tables:
                error_msg = "시계열 테이블이 없습니다."
                return

            for table_name, ts_column, numeric_columns in tables:
                query = _build_aggregation_query(
                    table_name, ts_column, numeric_columns, cutoff
                )
                cur = tsdb_conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()

                for row in rows:
                    bucket_start, bucket_end, metric, avg_val, min_val, max_val, count_val = row
                    warm_record = WarmAggregatedData(
                        source_table=table_name,
                        metric_name=metric,
                        bucket_start=bucket_start,
                        bucket_end=bucket_end,
                        avg_value=avg_val,
                        min_value=min_val,
                        max_value=max_val,
                        count_value=count_val,
                        source_db=tsdb_config.database_name or "",
                    )
                    db.add(warm_record)
                    total_records += 1

                # 원본 삭제
                cur.execute(
                    f'DELETE FROM "{table_name}" WHERE "{ts_column}" < %s',
                    (cutoff,),
                )
                deleted_count = cur.rowcount
                total_bytes += deleted_count * 100  # ~100 bytes/row 추정
                tsdb_conn.commit()
                cur.close()

            db.commit()

        finally:
            tsdb_conn.close()

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("Downsampling failed: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        duration = time.time() - t0
        _log_execution(db, "downsampling", "TimescaleDB → RDBMS",
                       total_bytes, total_records, round(duration, 1),
                       status, error_msg)
        db.close()


def execute_archiving():
    """Warm → Cold: RDBMS 만료 집계 데이터를 CSV로 MinIO에 업로드 후 삭제."""
    db = SessionLocal()
    t0 = time.time()
    total_bytes = 0
    total_records = 0
    status = "success"
    error_msg = ""

    try:
        policy = db.query(RetentionPolicy).first()
        warm_months = policy.warm_retention_months if policy else 12
        cutoff = datetime.utcnow() - timedelta(days=warm_months * 30)

        old_records = (
            db.query(WarmAggregatedData)
            .filter(WarmAggregatedData.created_at < cutoff)
            .all()
        )

        if not old_records:
            error_msg = "아카이빙 대상 데이터가 없습니다."
            return

        # CSV 직렬화
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "id", "source_table", "metric_name", "bucket_start", "bucket_end",
            "avg_value", "min_value", "max_value", "count_value",
            "source_db", "created_at",
        ])

        record_ids = []
        for r in old_records:
            writer.writerow([
                r.id, r.source_table, r.metric_name,
                r.bucket_start.isoformat() if r.bucket_start else "",
                r.bucket_end.isoformat() if r.bucket_end else "",
                r.avg_value, r.min_value, r.max_value, r.count_value,
                r.source_db,
                r.created_at.isoformat() if r.created_at else "",
            ])
            record_ids.append(r.id)
            total_records += 1

        csv_bytes = output.getvalue().encode("utf-8")
        total_bytes = len(csv_bytes)

        # MinIO 업로드
        client = _get_minio_client()
        bucket_name = "sdl-archive"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        object_name = f"retention/warm_archive_{timestamp_str}.csv"

        client.put_object(
            bucket_name, object_name,
            io.BytesIO(csv_bytes), length=total_bytes,
            content_type="text/csv",
        )

        # RDBMS에서 삭제 (배치)
        batch_size = 1000
        for i in range(0, len(record_ids), batch_size):
            batch = record_ids[i:i + batch_size]
            db.query(WarmAggregatedData).filter(
                WarmAggregatedData.id.in_(batch)
            ).delete(synchronize_session=False)
        db.commit()

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("Archiving failed: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        duration = time.time() - t0
        _log_execution(db, "archiving", "RDBMS → MinIO",
                       total_bytes, total_records, round(duration, 1),
                       status, error_msg)
        db.close()


def execute_cold_expiry():
    """Cold 만료: MinIO sdl-archive에서 보관 기간 초과 오브젝트 삭제."""
    db = SessionLocal()
    t0 = time.time()
    total_bytes = 0
    total_records = 0
    status = "success"
    error_msg = ""

    try:
        policy = db.query(RetentionPolicy).first()
        cold_years = policy.cold_retention_years if policy else 3
        cutoff = datetime.utcnow() - timedelta(days=cold_years * 365)

        client = _get_minio_client()
        bucket_name = "sdl-archive"

        if not client.bucket_exists(bucket_name):
            error_msg = "sdl-archive 버킷이 존재하지 않습니다."
            return

        objects_to_delete = []
        for obj in client.list_objects(bucket_name, recursive=True):
            if obj.last_modified:
                obj_time = obj.last_modified.replace(tzinfo=None)
                if obj_time < cutoff:
                    objects_to_delete.append(obj.object_name)
                    total_bytes += obj.size or 0
                    total_records += 1

        if not objects_to_delete:
            error_msg = "만료된 오브젝트가 없습니다."
            return

        delete_list = [DeleteObject(name) for name in objects_to_delete]
        errors = list(client.remove_objects(bucket_name, delete_list))

        if errors:
            status = "partial"
            error_msg = f"{len(errors)}개 오브젝트 삭제 실패"
            logger.warning("Cold expiry partial: %s", error_msg)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        logger.error("Cold expiry failed: %s", e)
    finally:
        duration = time.time() - t0
        _log_execution(db, "delete", "MinIO (만료)",
                       total_bytes, total_records, round(duration, 1),
                       status, error_msg)
        db.close()


# ── Internal helpers ────────────────────────────────


def _get_tsdb_connection(tsdb_config):
    """TsdbConfig로 psycopg2 연결 생성."""
    try:
        return psycopg2.connect(
            host=tsdb_config.host,
            port=tsdb_config.port,
            dbname=tsdb_config.database_name or "postgres",
            user="sdl_user",
            password="sdl_password_2025",
            connect_timeout=5,
        )
    except Exception as e:
        logger.error("TSDB connection failed: %s", e)
        return None


def _get_minio_client():
    """MinIO 클라이언트 생성 (DB 설정 우선)."""
    db = SessionLocal()
    try:
        return get_minio_client(db)
    finally:
        db.close()


def _discover_tsdb_tables(conn):
    """
    TimescaleDB에서 timestamp + numeric 컬럼을 가진 테이블 탐색.
    Returns: [(table_name, ts_column, [numeric_columns])]
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name NOT LIKE 'pg_%%'
          AND table_name NOT LIKE '_timescaledb_%%'
        ORDER BY table_name, ordinal_position
    """)

    tables = {}
    for tbl, col, dtype in cur.fetchall():
        if tbl not in tables:
            tables[tbl] = {"ts_cols": [], "num_cols": []}
        if dtype in ("timestamp without time zone", "timestamp with time zone",
                      "timestamptz", "timestamp"):
            tables[tbl]["ts_cols"].append(col)
        elif dtype in ("integer", "bigint", "real", "double precision",
                        "numeric", "float", "smallint"):
            tables[tbl]["num_cols"].append(col)

    cur.close()

    result = []
    for tbl, info in tables.items():
        if info["ts_cols"] and info["num_cols"]:
            result.append((tbl, info["ts_cols"][0], info["num_cols"]))
    return result


def _build_aggregation_query(table_name, ts_column, numeric_columns, cutoff):
    """1분 집계 UNION ALL SQL 생성."""
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    unions = []
    for col in numeric_columns:
        q = (
            f'SELECT date_trunc(\'minute\', "{ts_column}") AS bucket_start, '
            f'date_trunc(\'minute\', "{ts_column}") + interval \'1 minute\' AS bucket_end, '
            f"'{col}' AS metric, "
            f'AVG("{col}") AS avg_val, MIN("{col}") AS min_val, '
            f'MAX("{col}") AS max_val, COUNT(*) AS count_val '
            f'FROM "{table_name}" '
            f"WHERE \"{ts_column}\" < '{cutoff_str}' "
            f'GROUP BY 1, 2, 3'
        )
        unions.append(q)
    return " UNION ALL ".join(unions)


def _log_execution(db, task_type, tier_transition, processed_bytes,
                   processed_records, duration_seconds, status, error_message=""):
    """RetentionExecutionLog에 실행 기록 저장."""
    try:
        log = RetentionExecutionLog(
            execution_time=datetime.utcnow(),
            task_type=task_type,
            tier_transition=tier_transition,
            processed_bytes=int(processed_bytes),
            processed_records=int(processed_records),
            duration_seconds=float(duration_seconds),
            status=status,
            error_message=error_message or "",
        )
        db.add(log)
        db.commit()
    except Exception as e:
        logger.error("Failed to log execution: %s", e)
        try:
            db.rollback()
        except Exception:
            pass
