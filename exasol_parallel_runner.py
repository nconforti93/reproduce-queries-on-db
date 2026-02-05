#!/usr/bin/env python3
"""
exasol_parallel_runner.py

Updates:
- Smarter prepared param generation:
  If '?' is inside to_date(?, 'FMT') or to_timestamp(?, 'FMT'), generate current date/timestamp string
  formatted according to FMT (preferred) or NLS_* format from EXA_PARAMETERS.
- For prepared statements, sql_executed now includes substituted values (for logging/debugging).
- Connection check simplified: reconnect only if conn.is_closed.
- EXA_PARAMETERS uses (parameter_name, session_value).
- Char types fall back to fallback value.
- Fallback value: string integer 1..10.
"""

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
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any

import pyexasol

# -----------------------------
# Error normalization
# -----------------------------
SESSION_SUFFIX_RE = re.compile(r'\s*\(Session:.*$', re.IGNORECASE)

def normalize_error_message(err: str) -> str:
    if err is None:
        return ""
    err = " ".join(str(err).split())
    return SESSION_SUFFIX_RE.sub("", err).strip()

# -----------------------------
# SQL file parsing
# -----------------------------
def read_sql_file(sql_file: str, debug: bool = False) -> List[str]:
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    if content.startswith("\ufeff"):
        content = content.lstrip("\ufeff")

    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
    content = re.sub(r"--.*?(?=\r?\n|$)", "", content)

    stmts: List[str] = []
    buf: List[str] = []
    in_single_quote = False

    i = 0
    while i < len(content):
        ch = content[i]

        if ch == "'":
            if in_single_quote and i + 1 < len(content) and content[i + 1] == "'":
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
                stmts.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)

    if debug:
        print(f"[debug] Parsed statements: {len(stmts)} from file: {sql_file}")
    return stmts

def chunk_list_evenly(items: List, n_chunks: int) -> List[List]:
    out = [[] for _ in range(n_chunks)]
    for idx, item in enumerate(items):
        out[idx % n_chunks].append(item)
    return out

def make_output_dir(path: str):
    os.makedirs(path, exist_ok=True)

# -----------------------------
# Identifier quoting
# -----------------------------
def quote_ident(name: str) -> str:
    name = name.replace('"', '""')
    return f'"{name}"'

# -----------------------------
# Connection creation
# -----------------------------
def create_connection(creds: Dict, debug: bool = False):
    if debug:
        safe = {k: ("***" if k.lower() == "password" else v) for k, v in creds.items()}
        print("[debug] creating new connection:", safe)

    conn = pyexasol.connect(**creds, compression=True, socket_timeout=3600)

    schema_name = creds.get("schema")
    if schema_name:
        try:
            conn.execute(f'OPEN SCHEMA {quote_ident(schema_name)}')
            if debug:
                print(f"[debug] OPEN SCHEMA {schema_name} on new connection")
        except Exception as e:
            if debug:
                print("[debug] failed to open default schema on new connection:", e)

    return conn

# -----------------------------
# EXA_PARAMETERS (NLS formats)
# -----------------------------
def load_nls_formats(conn, debug: bool = False) -> Dict[str, str]:
    sql = (
        "select parameter_name, session_value "
        "from exa_parameters "
        "where parameter_name in ('NLS_DATE_FORMAT','NLS_TIMESTAMP_FORMAT')"
    )
    rows = conn.execute(sql).fetchall()

    out = {}
    for r in rows:
        if not r or len(r) < 2:
            continue
        out[str(r[0]).upper()] = str(r[1])

    out.setdefault("NLS_DATE_FORMAT", "YYYY-MM-DD")
    out.setdefault("NLS_TIMESTAMP_FORMAT", "YYYY-MM-DD HH24:MI:SS")

    if debug:
        print("[debug] loaded NLS formats:", out)
    return out

def _oracleish_to_python_strftime(fmt: str) -> str:
    # Best-effort token mapping. Extend if your formats include other tokens.
    mapping = [
        ("HH24", "%H"),
        ("YYYY", "%Y"),
        ("MM", "%m"),
        ("DD", "%d"),
        ("MI", "%M"),
        ("SS", "%S"),
    ]
    py = fmt
    for a, b in mapping:
        py = py.replace(a, b)
    return py

def format_date_value(d: datetime.date, fmt: str) -> str:
    return d.strftime(_oracleish_to_python_strftime(fmt))

def format_timestamp_value(dt: datetime.datetime, fmt: str) -> str:
    return dt.strftime(_oracleish_to_python_strftime(fmt))

# -----------------------------
# Audit query input (sql_text + scope_schema)
# -----------------------------
def fetch_queries_from_audit(conn, hours_back: int, debug: bool = False) -> List[Dict[str, Any]]:
    if hours_back <= 0:
        raise ValueError("--audit-hours must be > 0")

    sql = (
        "select distinct sql_text, scope_schema "
        "from exa_Dba_audit_sql "
        f"WHERE start_time >= TRUNC(CURRENT_TIMESTAMP, 'HH') - {int(hours_back)} / 24 "
        "and command_class not in ('TRANSACTION', 'DCL', 'OTHER') "
        "and success is true"
    )

    if debug:
        print("[debug] audit query:", sql)

    rows = conn.execute(sql).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        if not r:
            continue
        q = str(r[0]).strip()
        if not q:
            continue
        if q.endswith(";"):
            q = q[:-1].rstrip()

        scope_schema = None
        if len(r) > 1 and r[1] is not None:
            scope_schema = str(r[1]).strip() or None

        out.append({"sql": q, "scope_schema": scope_schema, "source": "audit"})

    if debug:
        print(f"[debug] fetched {len(out)} distinct (sql_text, scope_schema) rows from audit")
    return out

# -----------------------------
# OPEN SCHEMA (separate statement)
# -----------------------------
def open_schema_if_needed(conn, scope_schema: Optional[str], debug: bool = False) -> None:
    if not scope_schema:
        return
    stmt = f'OPEN SCHEMA {quote_ident(scope_schema)}'
    if debug:
        print(f"[debug] executing schema context statement: {stmt}")
    conn.execute(stmt)

# -----------------------------
# Prepared: context detection for to_date / to_timestamp around '?'
# -----------------------------
_TO_DATE_RE = re.compile(r"to_date\s*\(\s*\?\s*(?:,\s*'([^']*)')?\s*\)", re.IGNORECASE)
_TO_TS_RE   = re.compile(r"to_timestamp\s*\(\s*\?\s*(?:,\s*'([^']*)')?\s*\)", re.IGNORECASE)

def build_param_context_map(sql: str) -> Dict[int, Dict[str, Optional[str]]]:
    """
    Returns:
      { param_index_0_based: {"kind": "DATE"|"TIMESTAMP", "fmt": <format or None>} }
    param_index is based on the order of '?' in the SQL.
    """
    # First, find all '?' positions in order
    q_positions = [m.start() for m in re.finditer(r"\?", sql)]
    pos_to_index = {pos: i for i, pos in enumerate(q_positions)}

    ctx: Dict[int, Dict[str, Optional[str]]] = {}

    # Mark all to_date(?) occurrences
    for m in _TO_DATE_RE.finditer(sql):
        # Locate the '?' inside the match
        match_text = m.group(0)
        local_q = match_text.lower().find("?")
        if local_q < 0:
            continue
        q_abs_pos = m.start() + local_q
        idx = pos_to_index.get(q_abs_pos)
        if idx is None:
            continue
        fmt = m.group(1)  # may be None
        ctx[idx] = {"kind": "DATE", "fmt": fmt}

    # Mark all to_timestamp(?) occurrences
    for m in _TO_TS_RE.finditer(sql):
        match_text = m.group(0)
        local_q = match_text.lower().find("?")
        if local_q < 0:
            continue
        q_abs_pos = m.start() + local_q
        idx = pos_to_index.get(q_abs_pos)
        if idx is None:
            continue
        fmt = m.group(1)  # may be None
        ctx[idx] = {"kind": "TIMESTAMP", "fmt": fmt}

    return ctx

# -----------------------------
# Prepared parameter generation
# -----------------------------
def fallback_value() -> str:
    # Your current baseline: string int 1..10
    return str(random.randint(1, 10))

def _param_value_from_datatype(dt: Dict[str, Any], nls_formats: Dict[str, str]):
    """
    Default datatype-driven generation.
    Note: char types -> fallback.
    """
    t = str(dt.get("type", "VARCHAR")).upper()
    precision = dt.get("precision")
    scale = dt.get("scale")

    if t in ("DECIMAL", "NUMERIC", "NUMBER"):
        if scale is None:
            scale = 0
        if int(scale) <= 0:
            return random.randint(1, 10)
        whole = random.randint(1, 10)
        frac = random.randint(0, (10 ** int(scale)) - 1)
        return random.randint(1, 10)

    if t in ("INTEGER", "INT", "BIGINT", "SMALLINT"):
        return random.randint(1, 10)

    if t in ("DOUBLE", "FLOAT"):
        return float(random.randint(1, 10))

    if t == "BOOLEAN":
        return random.choice([True, False])

    # DATE/TIMESTAMP *by type* can still be produced, but your main request
    # is the to_date/to_timestamp context override (handled separately).
    if t == "DATE":
        d = datetime.date.today()
        return format_date_value(d, nls_formats["NLS_DATE_FORMAT"])

    if "TIMESTAMP" in t:
        now = datetime.datetime.now()
        return format_timestamp_value(now, nls_formats["NLS_TIMESTAMP_FORMAT"])

    # CHAR/VARCHAR/etc -> fallback (your baseline)
    if t in ("CHAR", "VARCHAR", "CLOB", "TEXT"):
        return fallback_value()

    # Fallback
    return fallback_value()

def _param_value_with_context(
    param_index: int,
    dt: Dict[str, Any],
    ctx_map: Dict[int, Dict[str, Optional[str]]],
    nls_formats: Dict[str, str],
) -> Any:
    """
    If the placeholder is wrapped by to_date/to_timestamp, override with current date/time string.
    """
    ctx = ctx_map.get(param_index)
    if not ctx:
        return _param_value_from_datatype(dt, nls_formats)

    kind = ctx.get("kind")
    fmt = ctx.get("fmt")

    if kind == "DATE":
        fmt_use = fmt if fmt else nls_formats["NLS_DATE_FORMAT"]
        return format_date_value(datetime.date.today(), fmt_use)

    if kind == "TIMESTAMP":
        fmt_use = fmt if fmt else nls_formats["NLS_TIMESTAMP_FORMAT"]
        return format_timestamp_value(datetime.datetime.now(), fmt_use)

    return _param_value_from_datatype(dt, nls_formats)

def build_params_from_parameter_data(
    sql: str,
    parameter_data: Dict[str, Any],
    nls_formats: Dict[str, str],
    debug: bool = False
) -> List[Any]:
    ctx_map = build_param_context_map(sql)
    cols = parameter_data.get("columns", []) or []

    vals: List[Any] = []
    for i, c in enumerate(cols):
        dt = c.get("dataType", {}) or {}
        v = _param_value_with_context(i, dt, ctx_map, nls_formats)
        vals.append(v)

    if debug and ctx_map:
        print("[debug] param context map:", ctx_map)
    if debug:
        print("[debug] generated prepared params:", vals)
    return vals

# -----------------------------
# Render sql_executed for prepared statements
# -----------------------------
def _sql_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    # strings/dates/timestamps produced as strings -> quote and escape quotes
    s = str(v).replace("'", "''")
    return f"'{s}'"

def render_sql_with_params(sql: str, params: List[Any]) -> str:
    out = sql
    for v in params:
        out = out.replace("?", _sql_literal(v), 1)
    return out

# -----------------------------
# Execute query
# -----------------------------
def execute_query(conn, query_obj: Dict[str, Any], debug: bool, nls_formats: Dict[str, str]):
    start = time.time()
    sql_original = query_obj["sql"]
    scope_schema = query_obj.get("scope_schema")
    source = query_obj.get("source")

    meta = {
        "sql": sql_original,
        "sql_executed": sql_original,
        "scope_schema": scope_schema,
        "source": source,
        "success": False,
        "elapsed_sec": None,
        "rowcount": None,
        "error": None,
        "prepared": False,
        "parameter_data": None,
    }

    st = None
    try:
        if scope_schema:
            open_schema_if_needed(conn, scope_schema, debug=debug)

        if "?" in sql_original:
            meta["prepared"] = True

            if debug:
                print("[debug] preparing statement via conn.cls_statement(..., prepare=True)")
            st = conn.cls_statement(conn, sql_original, None, prepare=True)

            try:
                parameter_data = st["parameter_data"]
            except Exception:
                parameter_data = getattr(st, "parameter_data", None)

            if not parameter_data:
                raise RuntimeError("Prepared statement did not expose parameter_data; cannot generate params.")

            meta["parameter_data"] = parameter_data

            # Generate params AFTER prepare, with to_date/to_timestamp context overrides
            row_params = build_params_from_parameter_data(sql_original, parameter_data, nls_formats, debug=debug)

            # Log what effectively ran
            meta["sql_executed"] = render_sql_with_params(sql_original, row_params)

            if debug:
                print("[debug] executing prepared statement with 1 row:", row_params)

            res = st.execute_prepared([row_params])

            try:
                meta["rowcount"] = getattr(res, "rowcount", None)
            except Exception:
                meta["rowcount"] = None

            meta["success"] = True
        else:
            st2 = conn.execute(sql_original)
            try:
                meta["rowcount"] = st2.rowcount
            except Exception:
                meta["rowcount"] = None
            meta["success"] = True

    except Exception as e:
        meta["error"] = str(e)
        if debug:
            traceback.print_exc()
    finally:
        meta["elapsed_sec"] = time.time() - start
        if st is not None:
            try:
                st.close()
            except Exception:
                pass

    return meta

# -----------------------------
# Worker
# -----------------------------
def worker_thread(
    worker_id: int,
    creds: Dict,
    queries: List[Dict[str, Any]],
    commit: bool,
    debug: bool,
    stop_event: threading.Event,
    deadline_monotonic: Optional[float],
    nls_formats: Dict[str, str],
) -> Dict:
    report = {
        "worker_id": worker_id,
        "queries_count_assigned": len(queries),
        "queries_executed": 0,
        "stopped": False,
        "results": []
    }

    conn = None
    try:
        conn = create_connection(creds, debug=debug)

        for i, qobj in enumerate(queries, start=1):
            if stop_event.is_set():
                report["stopped"] = True
                break
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                stop_event.set()
                report["stopped"] = True
                break

            # Reconnect only if is_closed (your preferred approach)
            if conn is None or getattr(conn, "is_closed", False):
                if debug:
                    print(f"[debug] worker {worker_id}: conn.is_closed -> reconnecting")
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                conn = create_connection(creds, debug=debug)

            res = execute_query(conn, qobj, debug=debug, nls_formats=nls_formats)
            res["query_index_in_worker"] = i
            report["results"].append(res)
            report["queries_executed"] += 1

            # Transaction control
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
                            res["rolled_back_after_commit_failure"] = True
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

# -----------------------------
# Error summary CSV
# -----------------------------
def write_error_summary_csv(output_dir: str, reports: List[Dict]) -> str:
    counter: Counter[Tuple[str, str]] = Counter()

    for r in reports:
        for qres in r.get("results", []):
            err = qres.get("error")
            if not err:
                continue
            sql = qres.get("sql", "")
            sql_norm = " ".join(str(sql).split())
            err_norm = normalize_error_message(err)
            counter[(sql_norm, err_norm)] += 1

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(output_dir, f"error_summary_{ts}.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query", "error_message", "count"])
        for (sql, err), cnt in counter.most_common():
            w.writerow([sql, err, cnt])

    return path

# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Run SQL queries in parallel sessions against Exasol using PyExasol")

    parser.add_argument("--creds-file", required=True, help="Path to JSON credentials file")

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--sql-file", help="Path to .sql file containing statements")
    src.add_argument("--audit-hours", type=int, help="Load distinct sql_text from EXA_DBA_AUDIT_SQL for last N hours")

    parser.add_argument("--sessions", type=int, default=4, help="Number of parallel sessions/workers")
    parser.add_argument("--commit", action="store_true", help="If set, commit successful statements. Default = rollback.")
    parser.add_argument("--timeout-sec", type=int, default=0, help="Global wall-clock timeout in seconds (0 = no timeout)")
    parser.add_argument("--output-dir", default="./results", help="Directory to write report files")
    parser.add_argument("--debug", action="store_true", help="Enable debug printing")
    parser.add_argument("--dump-statements", action="store_true", help="Print statement count + previews before running")

    args = parser.parse_args()
    make_output_dir(args.output_dir)

    creds = json.load(open(args.creds_file, "r", encoding="utf-8"))

    meta_conn = None
    query_objs: List[Dict[str, Any]] = []
    src_desc = ""

    try:
        meta_conn = create_connection(creds, debug=args.debug)

        # Load NLS formats ONCE
        nls_formats = load_nls_formats(meta_conn, debug=args.debug)

        if args.sql_file:
            queries = read_sql_file(args.sql_file, debug=args.debug)
            query_objs = [{"sql": q, "scope_schema": None, "source": "file"} for q in queries]
            src_desc = f"file:{args.sql_file}"
        else:
            query_objs = fetch_queries_from_audit(meta_conn, args.audit_hours, debug=args.debug)
            src_desc = f"audit:last_{args.audit_hours}_hours"

    finally:
        if meta_conn is not None:
            try:
                meta_conn.close()
            except Exception:
                pass

    print(f"[info] Loaded statements: {len(query_objs)} from: {src_desc}")
    print("[info] Transaction mode:", "COMMIT" if args.commit else "ROLLBACK (default)")

    if args.dump_statements:
        print("[info] First 10 statement previews:")
        for i, qo in enumerate(query_objs[:10], start=1):
            pv = qo["sql"].replace("\n", " ")
            ss = qo.get("scope_schema")
            print(f"  {i:03d}: {pv[:200]}{'...' if len(pv) > 200 else ''}  [scope_schema={ss}]")

    if not query_objs:
        print("No statements to execute. Exiting.")
        sys.exit(1)

    chunks = chunk_list_evenly(query_objs, args.sessions)

    stop_event = threading.Event()
    deadline = None
    if args.timeout_sec and args.timeout_sec > 0:
        deadline = time.monotonic() + args.timeout_sec
        print(f"[info] Global timeout enabled: {args.timeout_sec} seconds")

    reports: List[Dict] = []
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=args.sessions) as exe:
        futures = []
        for w_id in range(args.sessions):
            worker_queries = chunks[w_id]
            if not worker_queries:
                continue
            futures.append(exe.submit(
                worker_thread,
                w_id + 1,
                creds,
                worker_queries,
                args.commit,
                args.debug,
                stop_event,
                deadline,
                nls_formats,
            ))

        for fut in as_completed(futures):
            try:
                reports.append(fut.result())
            except Exception as e:
                print("Unexpected worker exception:", e)
                traceback.print_exc()

    total_elapsed = time.time() - start_all

    executed_count = sum(len(r.get("results", [])) for r in reports)
    success_count = sum(1 for r in reports for q in r.get("results", []) if q.get("success"))
    failure_count = sum(1 for r in reports for q in r.get("results", []) if not q.get("success"))
    prepared_count = sum(1 for r in reports for q in r.get("results", []) if q.get("prepared"))

    print("=" * 70)
    print("Execution summary")
    print(f"  source              : {src_desc}")
    print(f"  sessions requested  : {args.sessions}")
    print(f"  statements loaded   : {len(query_objs)}")
    print(f"  executed count      : {executed_count}")
    print(f"  prepared executed   : {prepared_count}")
    print(f"  success count       : {success_count}")
    print(f"  failure count       : {failure_count}")
    print(f"  elapsed sec         : {total_elapsed:.2f}")
    if deadline is not None:
        timed_out = stop_event.is_set() and time.monotonic() >= deadline
        print(f"  timed out           : {'YES' if timed_out else 'NO'}")
    print("=" * 70)

    error_csv = write_error_summary_csv(args.output_dir, reports)
    print(f"[info] Error summary CSV written to: {error_csv}")

    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    report_path = os.path.join(args.output_dir, f"exasol_parallel_report_{ts}.json")
    summary = {
        "source": src_desc,
        "sessions_requested": args.sessions,
        "statements_loaded": len(query_objs),
        "executed_count": executed_count,
        "prepared_executed": prepared_count,
        "success_count": success_count,
        "failure_count": failure_count,
        "elapsed_sec": total_elapsed,
        "timeout_sec": args.timeout_sec,
        "workers": reports,
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"[info] JSON report written to: {report_path}")


if __name__ == "__main__":
    main()
