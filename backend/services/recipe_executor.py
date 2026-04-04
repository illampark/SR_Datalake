"""데이터 레시피 실행 엔진 — SQL 검증, 비주얼→SQL 변환, 실행, 결과 저장"""

import logging
import re
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# ── SQL 보안 ──────────────────────────────────────

_BLOCKED_KEYWORDS = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|'
    r'EXECUTE|EXEC|CALL|INTO\s+OUTFILE|LOAD\s+DATA|COPY)\b',
    re.IGNORECASE,
)

_MAX_RESULT_ROWS = 50000
_PREVIEW_ROWS = 100


def validate_sql(sql):
    """SELECT 쿼리만 허용, DML/DDL 차단. (ok, error_msg) 반환."""
    stripped = sql.strip()
    if not stripped:
        return False, "SQL 쿼리가 비어 있습니다."
    if not stripped.upper().lstrip("(").startswith("SELECT"):
        return False, "SELECT 쿼리만 실행할 수 있습니다."
    # 주석 제거 후 위험 키워드 검사
    cleaned = re.sub(r'--[^\n]*', '', sql)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    m = _BLOCKED_KEYWORDS.search(cleaned)
    if m:
        return False, f"허용되지 않는 SQL 키워드: {m.group(0).upper()}"
    return True, ""


# ── 비주얼 빌더 → SQL 변환 ─────────────────────────

_ALLOWED_AGG_FUNCS = {"AVG", "SUM", "MIN", "MAX", "COUNT", "STDDEV"}
_ALLOWED_OPS = {"=", "!=", "<>", "<", ">", "<=", ">=", "LIKE", "NOT LIKE",
                "IN", "NOT IN", "IS NULL", "IS NOT NULL", "BETWEEN"}
_ALLOWED_JOIN_TYPES = {"INNER", "LEFT", "RIGHT", "FULL", "CROSS"}


def _quote_ident(name, db_type="postgresql"):
    """식별자를 안전하게 따옴표 처리."""
    name = name.replace('"', '').replace('`', '').replace("'", "")
    if db_type in ("mysql", "mariadb"):
        return f"`{name}`"
    return f'"{name}"'


def build_sql_from_visual(config, db_type="postgresql"):
    """visual_config JSON → SQL 문자열 변환."""
    tables = config.get("tables", [])
    joins = config.get("joins", [])
    columns = config.get("columns", [])
    where = config.get("where", [])
    group_by = config.get("groupBy", [])
    having = config.get("having", [])
    order_by = config.get("orderBy", [])
    limit = min(int(config.get("limit", _MAX_RESULT_ROWS)), _MAX_RESULT_ROWS)

    if not tables:
        raise ValueError("테이블을 최소 1개 선택해야 합니다.")
    if not columns:
        raise ValueError("출력 컬럼을 최소 1개 선택해야 합니다.")

    qi = lambda n: _quote_ident(n, db_type)

    # SELECT
    select_parts = []
    has_agg = False
    for col in columns:
        expr = col.get("expr", "")
        alias = col.get("alias", "")
        agg = (col.get("aggFunc") or "").upper()
        if agg:
            if agg not in _ALLOWED_AGG_FUNCS:
                raise ValueError(f"허용되지 않는 집계 함수: {agg}")
            part = f"{agg}({expr})"
            has_agg = True
        else:
            part = expr
        if alias:
            part += f" AS {qi(alias)}"
        select_parts.append(part)

    sql = "SELECT " + ", ".join(select_parts)

    # FROM
    main_table = tables[0]
    sql += f"\nFROM {qi(main_table['table'])} {main_table.get('alias', '')}"

    # JOINs
    alias_map = {t.get("alias", t["table"]): t["table"] for t in tables}
    for j in joins:
        jtype = (j.get("type") or "LEFT").upper()
        if jtype not in _ALLOWED_JOIN_TYPES:
            raise ValueError(f"허용되지 않는 JOIN 타입: {jtype}")
        to_alias = j["to"]["alias"]
        to_table = alias_map.get(to_alias, to_alias)
        from_col = f"{j['from']['alias']}.{qi(j['from']['col'])}"
        to_col = f"{to_alias}.{qi(j['to']['col'])}"
        sql += f"\n{jtype} JOIN {qi(to_table)} {to_alias} ON {from_col} = {to_col}"

    # WHERE
    if where:
        conditions = []
        for w in where:
            col = w.get("col", "")
            op = (w.get("op") or "=").upper()
            val = w.get("val", "")
            if op in ("IS NULL", "IS NOT NULL"):
                conditions.append(f"{col} {op}")
            elif op == "BETWEEN":
                vals = str(val).split(",")
                if len(vals) == 2:
                    conditions.append(f"{col} BETWEEN '{vals[0].strip()}' AND '{vals[1].strip()}'")
            elif op in ("IN", "NOT IN"):
                items = [f"'{v.strip()}'" for v in str(val).split(",")]
                conditions.append(f"{col} {op} ({', '.join(items)})")
            else:
                # 숫자면 따옴표 없이, 아니면 따옴표
                try:
                    float(val)
                    conditions.append(f"{col} {op} {val}")
                except (ValueError, TypeError):
                    conditions.append(f"{col} {op} '{val}'")
        if conditions:
            sql += "\nWHERE " + " AND ".join(conditions)

    # GROUP BY
    if group_by and has_agg:
        sql += "\nGROUP BY " + ", ".join(group_by)

    # HAVING
    if having and has_agg:
        hparts = []
        for h in having:
            expr = h.get("expr", "")
            op = (h.get("op") or ">=").upper()
            val = h.get("val", "0")
            hparts.append(f"{expr} {op} {val}")
        sql += "\nHAVING " + " AND ".join(hparts)

    # ORDER BY
    if order_by:
        oparts = []
        for o in order_by:
            col = o.get("col", "")
            direction = (o.get("dir") or "ASC").upper()
            if direction not in ("ASC", "DESC"):
                direction = "ASC"
            oparts.append(f"{col} {direction}")
        sql += "\nORDER BY " + ", ".join(oparts)

    # LIMIT
    sql += f"\nLIMIT {limit}"

    return sql


# ── 데이터베이스 연결 헬퍼 ─────────────────────────

def _connect_rdbms(rdbms_config):
    """RdbmsConfig 객체로 DB 연결 생성 (읽기 전용)."""
    db_type = (rdbms_config.db_type or "").lower()

    if "mysql" in db_type or "maria" in db_type:
        import pymysql
        conn = pymysql.connect(
            host=rdbms_config.host,
            port=rdbms_config.port,
            database=rdbms_config.database_name,
            user=rdbms_config.username or "",
            password=rdbms_config.password or "",
            charset="utf8mb4",
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return conn, "mysql"
    else:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(
            host=rdbms_config.host,
            port=rdbms_config.port,
            dbname=rdbms_config.database_name or "postgres",
            user=rdbms_config.username or "",
            password=rdbms_config.password or "",
            connect_timeout=10,
        )
        conn.set_session(readonly=True, autocommit=True)
        return conn, "postgresql"


def _connect_tsdb(tsdb_config):
    """TSDB 연결 생성 (읽기 전용).

    시계열 데이터는 메인 앱 DB의 time_series_data 테이블에 저장되므로
    앱 DB에 연결한다.
    """
    import psycopg2
    from urllib.parse import urlparse
    from backend.config import DATABASE_URL
    parsed = urlparse(DATABASE_URL.replace("+psycopg2", ""))
    conn = psycopg2.connect(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/") or "sdl",
        user=parsed.username or "",
        password=parsed.password or "",
        connect_timeout=10,
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _execute_sql(conn, db_type, sql, max_rows):
    """SQL 실행 후 (columns, rows_as_dicts, total_fetched) 반환."""
    if db_type == "mysql":
        cur = conn.cursor()
    else:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    t0 = time.time()
    cur.execute(sql)
    elapsed = round((time.time() - t0) * 1000, 1)

    if db_type == "mysql":
        rows = cur.fetchmany(max_rows)
        columns = list(rows[0].keys()) if rows else []
    else:
        columns = [desc[0] for desc in cur.description] if cur.description else []
        raw = cur.fetchmany(max_rows)
        rows = [dict(r) for r in raw]

    # datetime → str 변환
    items = []
    for r in rows:
        item = {}
        for k, v in r.items():
            if isinstance(v, datetime):
                item[k] = v.isoformat()
            elif v is None:
                item[k] = None
            else:
                item[k] = v
        items.append(item)

    cur.close()
    return columns, items, len(items), elapsed


# ── 레시피 실행 ───────────────────────────────────

def execute_recipe_preview(db, recipe):
    """레시피 미리보기 — LIMIT 100으로 실행, 결과 저장 없음."""
    from backend.models.storage import RdbmsConfig, TsdbConfig

    sql = recipe.query_sql
    if not sql:
        return {"error": "SQL이 정의되지 않았습니다."}

    ok, err = validate_sql(sql)
    if not ok:
        return {"error": err}

    # LIMIT 강제 적용 (기존 LIMIT 교체)
    preview_sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {_PREVIEW_ROWS}', sql, flags=re.IGNORECASE)
    if 'LIMIT' not in preview_sql.upper():
        preview_sql += f'\nLIMIT {_PREVIEW_ROWS}'

    try:
        if recipe.source_type == "rdbms":
            rdbms = db.query(RdbmsConfig).get(recipe.rdbms_id)
            if not rdbms:
                return {"error": "RDBMS 설정을 찾을 수 없습니다."}
            conn, db_type = _connect_rdbms(rdbms)
        else:
            tsdb = db.query(TsdbConfig).get(recipe.tsdb_id)
            if not tsdb:
                return {"error": "TSDB 설정을 찾을 수 없습니다."}
            conn = _connect_tsdb(tsdb)
            db_type = "postgresql"

        try:
            columns, items, count, elapsed = _execute_sql(conn, db_type, preview_sql, _PREVIEW_ROWS)
            return {
                "columns": columns,
                "rows": items,
                "rowCount": count,
                "elapsedMs": elapsed,
            }
        finally:
            conn.close()
    except Exception as e:
        logger.error("레시피 미리보기 실패 (recipe=%d): %s", recipe.id, e)
        return {"error": f"쿼리 실행 오류: {e}"}


def execute_recipe(db, recipe):
    """레시피 실행 — 결과 저장 + 카탈로그 생성/갱신."""
    from backend.models.storage import RdbmsConfig, TsdbConfig
    from backend.models.catalog import DataCatalog, AggregatedData, CatalogSearchTag

    sql = recipe.query_sql
    ok, err = validate_sql(sql)
    if not ok:
        recipe.last_error = err
        db.commit()
        return {"error": err}

    # LIMIT 강제
    exec_sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {_MAX_RESULT_ROWS}', sql, flags=re.IGNORECASE)
    if 'LIMIT' not in exec_sql.upper():
        exec_sql += f'\nLIMIT {_MAX_RESULT_ROWS}'

    try:
        if recipe.source_type == "rdbms":
            rdbms = db.query(RdbmsConfig).get(recipe.rdbms_id)
            if not rdbms:
                recipe.last_error = "RDBMS 설정을 찾을 수 없습니다."
                db.commit()
                return {"error": recipe.last_error}
            conn, db_type = _connect_rdbms(rdbms)
            source_name = rdbms.name
        else:
            tsdb = db.query(TsdbConfig).get(recipe.tsdb_id)
            if not tsdb:
                recipe.last_error = "TSDB 설정을 찾을 수 없습니다."
                db.commit()
                return {"error": recipe.last_error}
            conn = _connect_tsdb(tsdb)
            db_type = "postgresql"
            source_name = tsdb.name

        try:
            columns, items, count, elapsed = _execute_sql(conn, db_type, exec_sql, _MAX_RESULT_ROWS)
        finally:
            conn.close()

        # snapshot 모드: 기존 결과 삭제 후 새 결과 저장
        if recipe.execution_mode == "snapshot":
            db.query(AggregatedData).filter_by(recipe_id=recipe.id).delete()
            for idx, row in enumerate(items):
                db.add(AggregatedData(
                    recipe_id=recipe.id,
                    row_data=row,
                    row_index=idx,
                ))

        # 카탈로그 생성/갱신
        if recipe.catalog_id:
            cat = db.query(DataCatalog).get(recipe.catalog_id)
        else:
            cat = None

        if not cat:
            cat = DataCatalog(
                name=recipe.name,
                description=recipe.description or f"레시피 '{recipe.name}'의 실행 결과",
                connector_type="recipe",
                data_level="user_created",
                owner=recipe.owner,
                category=recipe.category,
                format="json",
                is_published=True,
            )
            db.add(cat)
            db.flush()
            recipe.catalog_id = cat.id
            # 검색 태그
            for tag in ["recipe", recipe.name, source_name]:
                if tag:
                    db.add(CatalogSearchTag(catalog_id=cat.id, tag=tag))
        else:
            cat.name = recipe.name
            cat.description = recipe.description or cat.description

        # 스키마 정보 저장
        cat.schema_info = ", ".join(columns) if columns else ""

        # 레시피 상태 갱신
        recipe.last_executed_at = datetime.utcnow()
        recipe.last_row_count = count
        recipe.last_error = ""

        db.commit()
        logger.info("레시피 실행 완료: recipe=%d, rows=%d, elapsed=%.1fms",
                     recipe.id, count, elapsed)
        return {
            "catalogId": cat.id,
            "rowCount": count,
            "columns": columns,
            "elapsedMs": elapsed,
        }

    except Exception as e:
        db.rollback()
        recipe.last_error = str(e)
        try:
            db.commit()
        except Exception:
            pass
        logger.error("레시피 실행 실패 (recipe=%d): %s", recipe.id, e)
        return {"error": f"실행 오류: {e}"}
