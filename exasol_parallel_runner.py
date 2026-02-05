#!/usr/bin/env python3
"""
exasol_parallel_runner.py (v3.7)

Change in this version:
- For prepared statements, meta["sql_executed"] now shows the SQL with
  prepared values substituted (for logging/debugging only).
- Execution remains server-side prepared via execute_prepared().
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
from typing import Dict, List, Optional, Any

import pyexasol


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

SESSION_SUFFIX_RE = re.compile(r'\s*\(Session:.*$', re.IGNORECASE)


def normalize_error_message(err: str) -> str:
    if err is None:
        return ""
    err = " ".join(str(err).split())
    return SESSION_SUFFIX_RE.sub("", err).strip()


def quote_ident(name: str) -> str:
    return f'"{name.replace("\"", "\"\"")}"'


def make_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

def load_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_creds(creds_file: str) -> Dict:
    return load_json_file(creds_file)


# ---------------------------------------------------------------------------
# SQL file parsing
# ---------------------------------------------------------------------------

def read_sql_file(sql_file: str) -> List[str]:
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
    content = re.sub(r"--.*?(?=\r?\n|$)", "", content)

    stmts, buf, in_string = [], [], False
    for ch in content:
        if ch == "'":
            in_string = not in_string
        if ch == ";" and not in_string:
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
        else:
            buf.append(ch)

    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)

    return stmts


def chunk_list_evenly(items: List, n_chunks: int) -> List[List]:
    out = [[] for _ in range(n_chunks)]
    for i, item in enumerate(items):
        out[i % n_chunks].append(item)
    return out


# ---------------------------------------------------------------------------
# Audit query input
# ---------------------------------------------------------------------------

def fetch_queries_from_audit(conn, hours_back: int) -> List[Dict[str, Any]]:
    sql = (
        "select distinct sql_text, scope_schema "
        "from exa_dba_audit_sql "
        f"where start_time >= trunc(current_timestamp, 'HH') - {hours_back} / 24 "
        "and command_class not in ('TRANSACTION','DCL','OTHER') "
        "and success is true"
    )
    rows = conn.execute(sql).fetchall()

    out = []
    for sql_text, scope_schema in rows:
        q = sql_text.strip()
        if q.endswith(";"):
            q = q[:-1]
        out.append({"sql": q, "scope_schema": scope_schema, "source": "audit"})
    return out


# ---------------------------------------------------------------------------
# Connection handling
# ---------------------------------------------------------------------------

def create_connection(creds: Dict):
    conn = pyexasol.connect(**creds, compression=True, socket_timeout=3600)
    schema = creds.get("schema")
    if schema:
        conn.execute(f"OPEN SCHEMA {quote_ident(schema)}")
    return conn


# ---------------------------------------------------------------------------
# NLS formats
# ---------------------------------------------------------------------------

def load_nls_formats(conn) -> Dict[str, str]:
    sql = (
        "select parameter_name, session_value "
        "from exa_parameters "
        "where parameter_name in ('NLS_DATE_FORMAT','NLS_TIMESTAMP_FORMAT')"
    )
    rows = conn.execute(sql).fetchall()
    out = {r[0]: r[1] for r in rows}
    out.setdefault("NLS_DATE_FORMAT", "YYYY-MM-DD")
    out.setdefault("NLS_TIMESTAMP_FORMAT", "YYYY-MM-DD HH24:MI:SS")
    return out


def oracle_to_strftime(fmt: str) -> str:
    return (
        fmt.replace("YYYY", "%Y")
           .replace("MM", "%m")
           .replace("DD", "%d")
           .replace("HH24", "%H")
           .replace("MI", "%M")
           .replace("SS", "%S")
    )


# ---------------------------------------------------------------------------
# Prepared parameter generation
# ---------------------------------------------------------------------------

def fallback_value() -> str:
    return str(random.randint(1, 10))


def param_value_from_dt(dt: Dict[str, Any], nls: Dict[str, str]):
    t = dt.get("type", "").upper()
    precision = dt.get("precision")
    scale = dt.get("scale")

    if t in ("DECIMAL", "NUMERIC", "NUMBER"):
        if scale and scale > 0:
            whole = random.randint(1, 10)
            frac = random.randint(0, 10 ** scale - 1)
            return Decimal(f"{whole}.{frac:0{scale}d}")
        return random.randint(1, 10)

    if t in ("INTEGER", "INT", "SMALLINT", "BIGINT"):
        return random.randint(1, 10)

    if t == "DATE":
        d = datetime.date.today()
        return d.strftime(oracle_to_strftime(nls["NLS_DATE_FORMAT"]))

    if "TIMESTAMP" in t:
        dtv = datetime.datetime.now()
        return dtv.strftime(oracle_to_strftime(nls["NLS_TIMESTAMP_FORMAT"]))

    # CHAR/VARCHAR/etc. → fallback
    return fallback_value()


def render_sql_with_params(sql: str, params: List[Any]) -> str:
    """
    Replace ? with values for logging only.
    Strings are quoted, others rendered as-is.
    """
    out = sql
    for v in params:
        if isinstance(v, str):
            rep = f"'{v}'"
        else:
            rep = str(v)
        out = out.replace("?", rep, 1)
    return out


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def execute_query(conn, qobj: Dict[str, Any], nls_formats: Dict[str, str]):
    sql = qobj["sql"]
    scope_schema = qobj.get("scope_schema")

    meta = {
        "sql": sql,
        "sql_executed": sql,
        "prepared": False,
        "success": False,
        "error": None,
    }

    try:
        if scope_schema:
            conn.execute(f"OPEN SCHEMA {quote_ident(scope_schema)}")

        if "?" in sql:
            meta["prepared"] = True
            st = conn.cls_statement(conn, sql, None, prepare=True)

            param_data = st["parameter_data"]
            row_params = [
                param_value_from_dt(c["dataType"], nls_formats)
                for c in param_data["columns"]
            ]

            # NEW: log substituted SQL
            meta["sql_executed"] = render_sql_with_params(sql, row_params)

            st.execute_prepared([row_params])
            meta["success"] = True
            st.close()
        else:
            conn.execute(sql)
            meta["success"] = True

    except Exception as e:
        meta["error"] = str(e)

    return meta


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def worker_thread(worker_id, creds, queries, commit, nls_formats):
    conn = None
    results = []

    try:
        conn = create_connection(creds)

        for q in queries:
            if conn.is_closed:
                conn = create_connection(creds)

            res = execute_query(conn, q, nls_formats)
            results.append(res)

            if commit and res["success"]:
                conn.commit()
            else:
                conn.rollback()

    finally:
        if conn and not conn.is_closed:
            conn.close()

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--creds-file", required=True)
    parser.add_argument("--sql-file")
    parser.add_argument("--audit-hours", type=int)
    parser.add_argument("--sessions", type=int, default=4)
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--output-dir", default="./results")
    args = parser.parse_args()

    make_output_dir(args.output_dir)
    creds = load_creds(args.creds_file)

    meta_conn = create_connection(creds)
    nls_formats = load_nls_formats(meta_conn)

    if args.sql_file:
        stmts = read_sql_file(args.sql_file)
        queries = [{"sql": s, "scope_schema": None, "source": "file"} for s in stmts]
    else:
        queries = fetch_queries_from_audit(meta_conn, args.audit_hours)

    meta_conn.close()

    chunks = chunk_list_evenly(queries, args.sessions)

    with ThreadPoolExecutor(max_workers=args.sessions) as exe:
        futures = [
            exe.submit(worker_thread, i + 1, creds, chunks[i], args.commit, nls_formats)
            for i in range(len(chunks))
        ]
        results = []
        for f in as_completed(futures):
            results.extend(f.result())

    # Write error CSV
    counter = Counter()
    for r in results:
        if r["error"]:
            counter[(r["sql"], normalize_error_message(r["error"]))] += 1

    csv_path = os.path.join(args.output_dir, "error_summary.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query", "error_message", "count"])
        for (sql, err), cnt in counter.items():
            w.writerow([sql, err, cnt])

    print(f"[info] Done. Error summary written to {csv_path}")


if __name__ == "__main__":
    main()
