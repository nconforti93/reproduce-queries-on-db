#!/usr/bin/env python3
"""
exasol_parallel_runner.py - updated (revert meta_nosql and fix error insert bug)

Features:
- parallel execution (file or audit)
- prepared statement support (single normal prepare attempt)
- context-aware param gen for to_date/to_timestamp
- strip comments for '?' detection
- OPEN SCHEMA for scope_schema
- optional insert of grouped errors into Exasol table (--error-target)
- disable SQL preprocessor on new connections
- fallback insert mechanisms for errors table (insert_multi, execute_many, prepared execute_many, single inserts)
- prints final summary of what happened
"""

from __future__ import annotations
import argparse
import csv
import datetime
import json
import os
import random
import re
import sys
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

try:
    import pyexasol
except Exception:
    print("Missing dependency: pyexasol. Install with `pip install pyexasol`.", file=sys.stderr)
    raise

# -------------------------
# Utilities & regexes
# -------------------------
SESSION_SUFFIX_RE = re.compile(r'\s*\(Session:.*$', re.IGNORECASE)
DB_SESSION_RE = re.compile(r'\(Session:\s*[^)]+\)', re.IGNORECASE)

def normalize_error_message(err: Optional[str]) -> str:
    if err is None:
        return ""
    s = " ".join(str(err).split())
    return SESSION_SUFFIX_RE.sub("", s).strip()

def detect_error_type(raw_err: Optional[str]) -> str:
    if not raw_err:
        return "SCRIPT"
    return "DATABASE" if DB_SESSION_RE.search(str(raw_err)) else "SCRIPT"

def quote_ident(name: str) -> str:
    return f'"{name.replace("\"","\"\"")}"'

def make_output_dir(path: str):
    os.makedirs(path, exist_ok=True)

# -------------------------
# SQL parsing helpers
# -------------------------
def strip_sql_comments_keep_strings(sql: str) -> str:
    out = []
    i = 0
    n = len(sql)
    in_s = False
    while i < n:
        ch = sql[i]
        if ch == "'" and not in_s:
            in_s = True
            out.append(ch)
            i += 1
            continue
        if ch == "'" and in_s:
            if i + 1 < n and sql[i+1] == "'":
                out.append("''")
                i += 2
                continue
            in_s = False
            out.append(ch)
            i += 1
            continue
        if not in_s:
            if sql.startswith("--", i):
                j = sql.find('\n', i)
                if j == -1:
                    break
                out.append('\n')
                i = j + 1
                continue
            if sql.startswith("/*", i):
                j = sql.find("*/", i+2)
                if j == -1:
                    break
                i = j + 2
                continue
            out.append(ch)
            i += 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)

# -------------------------
# Read SQL file
# -------------------------
def read_sql_file(sql_file: str, debug: bool = False) -> List[str]:
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()
    if content.startswith("\ufeff"):
        content = content.lstrip("\ufeff")
    # Strip block comments and line comments for splitting
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
    content = re.sub(r"--.*?(?=\r?\n|$)", "", content)
    statements: List[str] = []
    buf: List[str] = []
    in_single_quote = False
    i = 0
    while i < len(content):
        ch = content[i]
        if ch == "'":
            if in_single_quote and i + 1 < len(content) and content[i+1] == "'":
                buf.append("''")
                i += 2
                continue
            in_single_quote = not in_single_quote
            buf.append(ch)
            i += 1
            continue
        if ch == ";" and not in_single_quote:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    if debug:
        print(f"[debug] Parsed statements: {len(statements)} from file: {sql_file}")
    return statements

# -------------------------
# to_date/to_timestamp context detection
# -------------------------
_TO_DATE_RE = re.compile(r"to_date\s*\(\s*\?\s*(?:,\s*'([^']*)')?\s*\)", re.IGNORECASE)
_TO_TS_RE   = re.compile(r"to_timestamp\s*\(\s*\?\s*(?:,\s*'([^']*)')?\s*\)", re.IGNORECASE)

def build_param_context_map(sql: str) -> Dict[int, Dict[str, Optional[str]]]:
    positions = [m.start() for m in re.finditer(r"\?", sql)]
    pos_to_index = {pos: i for i,pos in enumerate(positions)}
    ctx: Dict[int, Dict[str, Optional[str]]] = {}
    for m in _TO_DATE_RE.finditer(sql):
        local_q = m.group(0).lower().find("?")
        if local_q < 0: continue
        q_abs = m.start() + local_q
        idx = pos_to_index.get(q_abs)
        if idx is None: continue
        fmt = m.group(1)
        ctx[idx] = {"kind": "DATE", "fmt": fmt}
    for m in _TO_TS_RE.finditer(sql):
        local_q = m.group(0).lower().find("?")
        if local_q < 0: continue
        q_abs = m.start() + local_q
        idx = pos_to_index.get(q_abs)
        if idx is None: continue
        fmt = m.group(1)
        ctx[idx] = {"kind": "TIMESTAMP", "fmt": fmt}
    return ctx

# -------------------------
# NLS handling & formatting
# -------------------------
_FF_RE = re.compile(r'FF\d*', re.IGNORECASE)
def _sanitize_ts_format(fmt: str) -> str:
    return _FF_RE.sub("", fmt)

def _oracle_to_strftime(fmt: str) -> str:
    fmt2 = _sanitize_ts_format(fmt)
    pairs = [("YYYY","%Y"),("MM","%m"),("DD","%d"),("HH24","%H"),("MI","%M"),("SS","%S")]
    out = fmt2
    for a,b in pairs:
        out = out.replace(a,b)
    return out

def format_date_value(d: datetime.date, fmt: str) -> str:
    return d.strftime(_oracle_to_strftime(fmt))

def format_timestamp_value(dt: datetime.datetime, fmt: str) -> str:
    return dt.replace(microsecond=0).strftime(_oracle_to_strftime(fmt))

def load_nls_formats(conn, debug: bool=False) -> Dict[str,str]:
    sql = (
        "select parameter_name, session_value "
        "from exa_parameters "
        "where parameter_name in ('NLS_DATE_FORMAT','NLS_TIMESTAMP_FORMAT')"
    )
    rows = conn.execute(sql).fetchall()
    out: Dict[str,str] = {}
    for r in rows:
        if not r or len(r) < 2:
            continue
        out[str(r[0]).upper()] = str(r[1])
    out.setdefault("NLS_DATE_FORMAT", "YYYY-MM-DD")
    out.setdefault("NLS_TIMESTAMP_FORMAT", "YYYY-MM-DD HH24:MI:SS")
    if debug:
        print("[debug] NLS formats:", out)
    return out

# -------------------------
# Param generation
# -------------------------
def fallback_value() -> str:
    return str(random.randint(1, 10))

def _param_value_from_datatype(dt: Dict[str,Any], nls_formats: Dict[str,str]) -> Any:
    t = str(dt.get("type", "VARCHAR")).upper()
    scale = dt.get("scale")
    if t in ("DECIMAL","NUMERIC","NUMBER"):
        if scale and int(scale) > 0:
            whole = random.randint(1,10)
            frac = random.randint(0, (10**int(scale))-1)
            return Decimal(f"{whole}.{frac:0{int(scale)}d}")
        return random.randint(1,10)
    if t in ("INTEGER","INT","SMALLINT","BIGINT"):
        return random.randint(1,10)
    if t in ("DOUBLE","FLOAT"):
        return float(random.randint(1,10))
    if t == "BOOLEAN":
        return random.choice([True,False])
    if t == "DATE":
        return format_date_value(datetime.date.today(), nls_formats["NLS_DATE_FORMAT"])
    if "TIMESTAMP" in t:
        return format_timestamp_value(datetime.datetime.now(), nls_formats["NLS_TIMESTAMP_FORMAT"])
    if t in ("CHAR","VARCHAR","CLOB","TEXT"):
        return fallback_value()
    return fallback_value()

def _param_value_with_context(index: int, dt: Dict[str,Any], ctx_map: Dict[int,Dict[str,Optional[str]]], nls_formats: Dict[str,str]) -> Any:
    ctx = ctx_map.get(index)
    if not ctx:
        return _param_value_from_datatype(dt, nls_formats)
    kind = ctx.get("kind")
    fmt = ctx.get("fmt")
    if kind == "DATE":
        fmt_use = fmt if fmt else nls_formats["NLS_DATE_FORMAT"]
        return format_date_value(datetime.date.today(), fmt_use)
    if kind == "TIMESTAMP":
        fmt_use = fmt if fmt else nls_formats["NLS_TIMESTAMP_FORMAT"]
        fmt_use = _sanitize_ts_format(fmt_use)
        return format_timestamp_value(datetime.datetime.now(), fmt_use)
    return _param_value_from_datatype(dt, nls_formats)

def build_params_from_parameter_data(sql: str, parameter_data: Dict[str,Any], nls_formats: Dict[str,str], debug: bool=False) -> List[Any]:
    ctx_map = build_param_context_map(sql)
    cols = parameter_data.get("columns", []) or []
    vals = []
    for i,c in enumerate(cols):
        dt = c.get("dataType", {}) or {}
        vals.append(_param_value_with_context(i, dt, ctx_map, nls_formats))
    if debug and ctx_map:
        print("[debug] param context map:", ctx_map)
    if debug:
        print("[debug] generated params:", vals)
    return vals

def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int,float,Decimal)):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"

def render_sql_with_params(sql: str, params: List[Any]) -> str:
    out = sql
    for v in params:
        out = out.replace("?", _sql_literal(v), 1)
    return out

# -------------------------
# Connection creation (disable preprocessor)
# -------------------------
def create_connection(creds: Dict[str,Any], debug: bool=False):
    if debug:
        safe = {k: ("***" if k.lower()=="password" else v) for k,v in creds.items()}
        print("[debug] create_connection with creds:", safe)
    conn = pyexasol.connect(**creds, compression=True, socket_timeout=3600)
    schema_name = creds.get("schema")
    if schema_name:
        try:
            conn.execute(f'OPEN SCHEMA {quote_ident(schema_name)}')
            if debug:
                print(f"[debug] OPEN SCHEMA {schema_name}")
        except Exception as e:
            if debug:
                print("[debug] OPEN SCHEMA failed:", e)
    try:
        conn.execute("ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT=''")
        if debug:
            print("[debug] Disabled SQL preprocessor script on session")
    except Exception as e:
        if debug:
            print("[debug] Failed to disable SQL preprocessor script:", e)
    return conn

# -------------------------
# Fetch queries from audit
# -------------------------
def fetch_queries_from_audit(conn, hours_back: int, ignore_import_export: bool=False, debug: bool=False) -> List[Dict[str,Any]]:
    if hours_back <= 0:
        raise ValueError("--audit-hours must be > 0")
    sql = (
        "select distinct sql_text, scope_schema "
        "from exa_Dba_audit_sql "
        f"WHERE start_time >= TRUNC(CURRENT_TIMESTAMP, 'HH') - {int(hours_back)} / 24 "
        "and command_class not in ('TRANSACTION', 'DCL', 'OTHER') "
        "and success is true"
    )
    if ignore_import_export:
        sql += " and command_name not in ('IMPORT','EXPORT')"
    if debug:
        print("[debug] audit SQL:", sql)
    rows = conn.execute(sql).fetchall()
    out = []
    for r in rows:
        if not r: continue
        q = str(r[0]).strip()
        if not q: continue
        if q.endswith(";"):
            q = q[:-1].rstrip()
        scope = None
        if len(r) > 1 and r[1] is not None:
            scope = str(r[1]).strip() or None
        out.append({"sql": q, "scope_schema": scope, "source": "audit"})
    if debug:
        print(f"[debug] fetched {len(out)} queries from audit")
    return out

# -------------------------
# ensure_error_table_and_insert (defensive, uses insert_multi tuple API)
# -------------------------
def ensure_error_table_and_insert(conn, error_target: str, run_id_int: int, run_ts_iso: str,
                                  grouped: Dict[Tuple[Any,...], int], debug: bool=False,
                                  batch_size: int = 1000):
    """
    Create error table if missing and insert grouped rows.
    grouped keys expected to be tuples containing:
      (run_id, run_ts, sql_text, sql_executed, error_message, error_type)
    """
    target = error_target.strip()
    if "." not in target:
        raise ValueError("--error-target must be SCHEMA.TABLE")
    schema, table = target.split(".", 1)
    schema = schema.strip().strip('"')
    table = table.strip().strip('"')

    full_table_quoted = f'{quote_ident(schema)}.{quote_ident(table)}'
    full_table_tuple = (schema, table)

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {full_table_quoted} ("
        "RUN_ID DECIMAL(18,0),"
        "RUN_TS TIMESTAMP,"
        "QUERY CLOB,"
        "SQL_EXECUTED CLOB,"
        "ERROR_MESSAGE CLOB,"
        "ERROR_TYPE VARCHAR(20),"
        "OCCURRENCES DECIMAL(18,0)"
        ")"
    )
    if debug:
        print("[debug] ensure_error_table_and_insert create_sql:", create_sql)
    conn.execute(create_sql)

    # Build rows carefully, validating entries
    rows_to_insert: List[List[Any]] = []
    for key, cnt in grouped.items():
        try:
            if not isinstance(key, (list, tuple)) or len(key) < 6:
                if debug:
                    print("[debug] Skipping malformed grouped key:", key)
                continue
            run_id_k, run_ts_k, sql_text, sql_exec, err_msg, err_type = key[:6]
            try:
                run_id_val = int(run_id_k)
            except Exception:
                run_id_val = int(run_id_int)
            run_ts_val = str(run_ts_k) if run_ts_k is not None else str(run_ts_iso)
            sql_text_val = str(sql_text) if sql_text is not None else "<no_sql>"
            sql_exec_val = str(sql_exec) if sql_exec is not None else sql_text_val
            err_msg_val = str(err_msg) if err_msg is not None else ""
            err_type_val = str(err_type) if err_type is not None else detect_error_type(err_msg_val)
            if not sql_text_val.strip():
                if debug:
                    print("[debug] Skipping grouped entry with empty SQL text:", key)
                continue
            rows_to_insert.append([run_id_val, run_ts_val, sql_text_val, sql_exec_val, err_msg_val, err_type_val, int(cnt)])
        except Exception as e:
            if debug:
                print("[debug] Error building row from grouped key:", key, "reason:", e)
            continue

    if not rows_to_insert:
        if debug:
            print("[debug] No valid error rows to insert.")
        return

    if debug:
        print("[debug] sample rows to insert (first 5):")
        for r in rows_to_insert[:5]:
            print("  ", r)

    # try conn.ext.insert_multi with (schema, table) tuple
    try:
        ext = getattr(conn, "ext", None)
        if ext is not None and hasattr(ext, "insert_multi"):
            if debug:
                print("[debug] Using conn.ext.insert_multi for bulk insert.")
            def rows_generator():
                for r in rows_to_insert:
                    yield r
            columns = ["RUN_ID", "RUN_TS", "QUERY", "SQL_EXECUTED", "ERROR_MESSAGE", "ERROR_TYPE", "OCCURRENCES"]
            ext.insert_multi(full_table_tuple, rows_generator(), columns=columns)
            try:
                conn.commit()
            except Exception as e:
                print("[warn] commit after insert_multi failed:", e)
                if debug:
                    traceback.print_exc()
            if debug:
                print(f"[debug] insert_multi inserted {len(rows_to_insert)} rows into {schema}.{table}")
            return
    except Exception as e:
        if debug:
            print("[debug] insert_multi failed, falling back. Reason:", e)
            traceback.print_exc()

    # fallback: execute_many if available
    insert_sql = f"INSERT INTO {full_table_quoted} (RUN_ID,RUN_TS,QUERY,SQL_EXECUTED,ERROR_MESSAGE,ERROR_TYPE,OCCURRENCES) VALUES (?,?,?,?,?,?,?)"
    try:
        if hasattr(conn, "execute_many") and callable(getattr(conn, "execute_many")):
            if debug:
                print("[debug] Using conn.execute_many (batched).")
            i = 0
            n = len(rows_to_insert)
            while i < n:
                batch = rows_to_insert[i:i+batch_size]
                conn.execute_many(insert_sql, batch)
                i += batch_size
            conn.commit()
            if debug:
                print(f"[debug] execute_many inserted {n} rows into {full_table_quoted}")
            return
    except Exception as e:
        if debug:
            print("[debug] conn.execute_many failed, falling back. Reason:", e)
            traceback.print_exc()

    # fallback: prepared statement + execute_many
    try:
        if debug:
            print("[debug] Attempting prepared statement + execute_many fallback.")
        st = conn.cls_statement(conn, insert_sql, None, prepare=True)
        i = 0
        n = len(rows_to_insert)
        while i < n:
            batch = rows_to_insert[i:i+batch_size]
            st.execute_many(batch)
            i += batch_size
        try:
            st.close()
        except Exception:
            pass
        conn.commit()
        if debug:
            print(f"[debug] prepared execute_many inserted {n} rows into {full_table_quoted}")
        return
    except Exception as e:
        if debug:
            print("[debug] prepared execute_many failed, falling back to single-row inserts. Reason:", e)
            traceback.print_exc()
        try:
            if 'st' in locals() and st is not None:
                try:
                    st.close()
                except Exception:
                    pass
        except Exception:
            pass

    # final fallback: single-row inserts
    if debug:
        print("[debug] Falling back to single-row inserts (slow).")
    failed = 0
    for row in rows_to_insert:
        try:
            conn.execute(insert_sql, row)
        except Exception as e:
            failed += 1
            print("[warn] Failed to insert error row (continuing):", e)
            if debug:
                traceback.print_exc()
    try:
        conn.commit()
    except Exception as e:
        print("[warn] commit of error rows failed:", e)
        if debug:
            traceback.print_exc()
    if debug:
        print(f"[debug] inserted {len(rows_to_insert)-failed} rows, failed {failed} rows into {full_table_quoted}")

# -------------------------
# execute_query (single normal prepare attempt only)
# -------------------------
def execute_query(conn, query_obj: Dict[str, Any], nls_formats: Dict[str, str], debug: bool = False):
    start_time = time.time()
    sql_original = query_obj.get("sql", "")
    scope_schema = query_obj.get("scope_schema")
    source = query_obj.get("source")
    meta: Dict[str, Any] = {
        "sql": sql_original,
        "sql_executed": sql_original,
        "scope_schema": scope_schema,
        "source": source,
        "prepared": False,
        "parameter_data": None,
        "success": False,
        "error": None,
        "elapsed_sec": None,
    }
    st = None
    try:
        if scope_schema:
            try:
                conn.execute(f'OPEN SCHEMA {quote_ident(scope_schema)}')
                if debug:
                    print(f"[debug] OPEN SCHEMA {scope_schema}")
            except Exception as e:
                if debug:
                    print("[debug] OPEN SCHEMA failed:", e)

        sql_uncommented = strip_sql_comments_keep_strings(sql_original)

        if "?" in sql_uncommented:
            meta["prepared"] = True
            try:
                if debug:
                    print("[debug] Trying normal prepare for SQL.")
                st = conn.cls_statement(conn, sql_original, None, prepare=True)
            except Exception as e:
                # Normal prepare failed -> fallback to direct execute
                if debug:
                    print("[debug] Normal prepare failed; falling back to direct execute. Reason:", e)
                    traceback.print_exc()
                meta["prepared"] = False
                try:
                    conn.execute(sql_original)
                    meta["success"] = True
                except Exception as exec_e:
                    meta["error"] = str(exec_e)
                return meta

            # get parameter_data
            try:
                parameter_data = st["parameter_data"]
            except Exception:
                parameter_data = getattr(st, "parameter_data", None)

            num_cols = None
            if isinstance(parameter_data, dict):
                num_cols = parameter_data.get("numColumns")

            if not parameter_data or (isinstance(num_cols, int) and num_cols == 0):
                try:
                    st.close()
                except Exception:
                    pass
                meta["prepared"] = False
                try:
                    conn.execute(sql_original)
                    meta["success"] = True
                except Exception as exec_e:
                    meta["error"] = str(exec_e)
                return meta

            meta["parameter_data"] = parameter_data
            try:
                row_params = build_params_from_parameter_data(sql_original, parameter_data, nls_formats, debug=debug)
            except Exception as e:
                meta["error"] = f"Parameter generation failed: {e}"
                if debug:
                    print("[debug] Parameter generation failed:", e)
                try:
                    st.close()
                except Exception:
                    pass
                meta["prepared"] = False
                try:
                    conn.execute(sql_original)
                    meta["success"] = True
                except Exception as exec_e:
                    meta["error"] = str(exec_e)
                return meta

            try:
                meta["sql_executed"] = render_sql_with_params(sql_original, row_params)
            except Exception:
                meta["sql_executed"] = sql_original

            try:
                if debug:
                    print("[debug] Executing prepared statement with params:", row_params)
                st.execute_prepared([row_params])
                meta["success"] = True
            except Exception as exec_e:
                meta["error"] = str(exec_e)
            finally:
                try:
                    st.close()
                except Exception:
                    pass

        else:
            try:
                conn.execute(sql_original)
                meta["success"] = True
            except Exception as exec_e:
                meta["error"] = str(exec_e)

    except Exception as e:
        meta["error"] = str(e)
        if debug:
            traceback.print_exc()
    finally:
        meta["elapsed_sec"] = time.time() - start_time
        try:
            if st is not None:
                try:
                    st.close()
                except Exception:
                    pass
        except Exception:
            pass
    return meta

# -------------------------
# worker & main
# -------------------------
def worker_thread(worker_id:int, creds: Dict[str,Any], queries: List[Dict[str,Any]],
                  commit: bool, debug: bool, stop_event: threading.Event,
                  deadline_monotonic: Optional[float], nls_formats: Dict[str,str]) -> Dict[str,Any]:
    report = {"worker_id": worker_id, "count_assigned": len(queries), "executed": 0, "results": []}
    conn = None
    try:
        conn = create_connection(creds, debug=debug)
        for i,q in enumerate(queries, start=1):
            if stop_event.is_set():
                break
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                stop_event.set()
                break
            if conn is None or getattr(conn, "is_closed", False):
                if debug:
                    print(f"[debug] worker {worker_id}: connection closed -> reconnect")
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                conn = create_connection(creds, debug=debug)
            res = execute_query(conn, q, nls_formats, debug=debug)
            report["results"].append(res)
            report["executed"] += 1
            try:
                if commit and res.get("success"):
                    try:
                        conn.commit()
                        res["committed"] = True
                    except Exception as e:
                        res["committed"] = False
                        res["commit_error"] = str(e)
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                else:
                    try:
                        conn.rollback()
                        res["rolled_back"] = True
                    except Exception as e:
                        res["rollback_error"] = str(e)
            except Exception:
                if debug:
                    traceback.print_exc()
    except Exception as e:
        report["connection_error"] = str(e)
        if debug:
            traceback.print_exc()
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    return report

def write_error_summary_csv(output_dir: str, grouped: Dict[Tuple[Any,...], int]) -> str:
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(output_dir, f"error_summary_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["run_id","run_ts","query","sql_executed","error_message","error_type","count"])
        for (run_id, run_ts, sql, sql_exec, err, err_type), cnt in grouped.items():
            w.writerow([run_id, run_ts, sql, sql_exec, err, err_type, cnt])
    return path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--creds-file", required=True)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--sql-file")
    src.add_argument("--audit-hours", type=int)
    p.add_argument("--sessions", type=int, default=4)
    p.add_argument("--commit", action="store_true", help="commit executed statements (default rollback)")
    p.add_argument("--timeout-sec", type=int, default=0)
    p.add_argument("--output-dir", default="./results")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--dump-statements", action="store_true")
    p.add_argument("--error-target", help="Schema.Table to insert error summary rows (optional)")
    p.add_argument("--ignore-import-export", action="store_true", help="When using --audit-hours, ignore IMPORT/EXPORT queries")
    args = p.parse_args()

    make_output_dir(args.output_dir)
    creds = json.load(open(args.creds_file, "r", encoding="utf-8"))

    meta_conn = None
    try:
        meta_conn = create_connection(creds, debug=args.debug)
        nls_formats = load_nls_formats(meta_conn, debug=args.debug)
        if args.sql_file:
            stmts = []
            for s in read_sql_file(args.sql_file, debug=args.debug):
                if args.ignore_import_export:
                    sstr = s.strip().lower()
                    if sstr.startswith("import ") or sstr.startswith("export "):
                        if args.debug:
                            print("[debug] skipping IMPORT/EXPORT statement from file")
                        continue
                stmts.append({"sql": s, "scope_schema": None, "source":"file"})
            query_objs = stmts
            src_desc = f"file:{args.sql_file}"
        else:
            query_objs = fetch_queries_from_audit(meta_conn, args.audit_hours, ignore_import_export=args.ignore_import_export, debug=args.debug)
            src_desc = f"audit:last_{args.audit_hours}_hours"
    finally:
        if meta_conn is not None:
            try:
                meta_conn.close()
            except Exception:
                pass

    print(f"[info] Loaded {len(query_objs)} statements from {src_desc}")
    if args.dump_statements:
        for i,q in enumerate(query_objs[:10], start=1):
            print(f"{i:03d}: {q['sql'][:200]} ... [scope_schema={q.get('scope_schema')}]")

    if not query_objs:
        print("No statements to execute. Exiting.")
        sys.exit(0)

    run_ts = datetime.datetime.utcnow()
    run_id_int = int(run_ts.strftime("%Y%m%d%H%M%S"))
    run_ts_iso = run_ts.strftime("%Y-%m-%d %H:%M:%S")

    chunks = [[] for _ in range(args.sessions)]
    for idx, q in enumerate(query_objs):
        chunks[idx % args.sessions].append(q)

    stop_event = threading.Event()
    deadline = None
    if args.timeout_sec and args.timeout_sec > 0:
        deadline = time.monotonic() + args.timeout_sec

    reports = []
    with ThreadPoolExecutor(max_workers=args.sessions) as exe:
        futures = []
        for w in range(args.sessions):
            if not chunks[w]:
                continue
            futures.append(exe.submit(worker_thread, w+1, creds, chunks[w], args.commit, args.debug, stop_event, deadline, nls_formats))
        for fut in as_completed(futures):
            try:
                reports.append(fut.result())
            except Exception as e:
                print("Worker exception:", e)
                traceback.print_exc()

    # aggregate errors grouped by meaningful tuple
    grouped: Dict[Tuple[Any,...], int] = {}
    total_statements_executed = 0
    total_prepared_attempts = 0
    total_success = 0
    total_failed = 0
    total_committed = 0
    total_rolled_back = 0
    for r in reports:
        total_statements_executed += r.get("executed", 0)
        for res in r.get("results", []):
            if res.get("prepared"):
                total_prepared_attempts += 1
            if res.get("success"):
                total_success += 1
            else:
                total_failed += 1
            if res.get("committed"):
                total_committed += 1
            if res.get("rolled_back"):
                total_rolled_back += 1
            if not res.get("error"):
                continue
            raw_err = res.get("error")
            norm_err = normalize_error_message(raw_err)
            err_type = detect_error_type(raw_err)
            key = (run_id_int, run_ts_iso, res.get("sql",""), res.get("sql_executed",""), norm_err, err_type)
            grouped[key] = grouped.get(key,0) + 1

    # write CSV if any
    if grouped:
        csv_path = write_error_summary_csv(args.output_dir, grouped)
        print(f"[info] Error CSV written to {csv_path}")
    else:
        print("[info] No errors to report.")

    # If error-target specified, insert into DB (commit regardless)
    inserted_rows_into_error_table = 0
    if args.error_target:
        db_conn = None
        try:
            db_conn = create_connection(creds, debug=args.debug)
            insert_group = grouped.copy()
            ensure_error_table_and_insert(db_conn, args.error_target, run_id_int, run_ts_iso, insert_group, debug=args.debug)
            inserted_rows_into_error_table = len(insert_group)
            if not args.commit:
                print("[warn] Error rows were inserted and COMMITTED even though --commit was not set (per configured behavior).")
            print(f"[info] Inserted {len(insert_group)} grouped error rows into {args.error_target}")
        except Exception as e:
            print("[error] Could not insert error summary into DB:", e)
            if args.debug:
                traceback.print_exc()
        finally:
            if db_conn is not None:
                try:
                    db_conn.close()
                except Exception:
                    pass

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    report_file = os.path.join(args.output_dir, f"exasol_parallel_report_{ts}.json")
    summary = {
        "run_id": run_id_int,
        "run_ts": run_ts_iso,
        "source": src_desc,
        "sessions": args.sessions,
        "statements_loaded": len(query_objs),
        "statements_executed": total_statements_executed,
        "prepared_attempts": total_prepared_attempts,
        "successes": total_success,
        "failures": total_failed,
        "committed": total_committed,
        "rolled_back": total_rolled_back,
        "error_rows_inserted_into_table": inserted_rows_into_error_table,
        "reports": reports
    }
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, default=str, indent=2)
    print(f"[info] JSON report written to {report_file}")

    # FINAL SUMMARY PRINT (human readable)
    print("\n===== Run Summary =====")
    print(f"Run ID            : {run_id_int}")
    print(f"Run timestamp     : {run_ts_iso}")
    print(f"Source            : {src_desc}")
    print(f"Statements loaded : {len(query_objs)}")
    print(f"Statements executed: {total_statements_executed}")
    print(f"Prepared attempts : {total_prepared_attempts}")
    print(f"Successes         : {total_success}")
    print(f"Failures          : {total_failed}")
    print(f"Committed         : {total_committed}")
    print(f"Rolled back       : {total_rolled_back}")
    print(f"Error groups      : {len(grouped)}")
    if grouped:
        print(f"Error CSV         : {csv_path}")
    if args.error_target:
        print(f"Inserted error groups into: {args.error_target} (rows: {inserted_rows_into_error_table})")
    print("=======================\n")

if __name__ == "__main__":
    main()
