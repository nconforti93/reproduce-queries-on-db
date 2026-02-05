#!/usr/bin/env python3
"""
exasol_parallel_runner.py (v3.6)

Change in this version:
- For prepared statement parameter generation:
  - Character types (CHAR/VARCHAR/etc.) now respect dt['size'] (max length).
  - Generated string is digits only (number as string), no "X" prefix.
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

try:
    import pyexasol
except Exception:
    print("Missing dependency: pyexasol is required. Install with `pip install pyexasol`.")
    raise


# ---------------------------------------------------------------------------
# JSON loading (tolerant)
# ---------------------------------------------------------------------------

def _remove_json_comments_and_trailing_commas(s: str) -> str:
    s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)
    s = re.sub(r'//.*?(?=\r?\n|$)', '', s)
    s = re.sub(r',\s*(?=[}\]])', '', s)
    return s


def load_json_file(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    cleaned = _remove_json_comments_and_trailing_commas(raw)
    try:
        obj = json.loads(cleaned)
    except json.JSONDecodeError as e:
        pos = getattr(e, "pos", None)
        snippet = ""
        if pos is not None:
            start = max(0, pos - 60)
            end = min(len(cleaned), pos + 60)
            snippet = cleaned[start:end].replace("\n", "\\n")
        raise ValueError(
            f"Failed to parse JSON file: {path}\n"
            f"{e.msg} at line {e.lineno} col {e.colno} (char {e.pos}).\n"
            f"Nearby text: {snippet}\n"
            "Fix JSON: use double quotes; avoid trailing commas; remove comments."
        ) from e
    if not isinstance(obj, dict):
        raise ValueError(f"Top-level JSON must be an object/dict: {path}")
    return obj


def load_creds(creds_file: str) -> Dict:
    return load_json_file(creds_file)


# ---------------------------------------------------------------------------
# SQL splitting (robust)
# ---------------------------------------------------------------------------

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
        print(f"[debug] read_sql_file(): Found {len(stmts)} statements.")
    return stmts


def chunk_list_evenly(items: List, n_chunks: int) -> List[List]:
    out = [[] for _ in range(n_chunks)]
    for idx, item in enumerate(items):
        out[idx % n_chunks].append(item)
    return out


def make_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


# ---------------------------------------------------------------------------
# Audit query input (includes scope_schema)
# Includes:
#   - success is true
#   - excludes TRANSACTION, DCL, OTHER
# ---------------------------------------------------------------------------

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

    st = conn.execute(sql)
    rows = st.fetchall()

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


# ---------------------------------------------------------------------------
# Error normalization (strip session id suffix)
# ---------------------------------------------------------------------------

SESSION_SUFFIX_RE = re.compile(r'\s*\(Session:.*$', re.IGNORECASE)

def normalize_error_message(err: str) -> str:
    if err is None:
        return ""
    err = " ".join(str(err).split())
    err = SESSION_SUFFIX_RE.sub("", err).strip()
    return err


# ---------------------------------------------------------------------------
# Schema context execution (OPEN SCHEMA as separate statement)
# ---------------------------------------------------------------------------

def quote_ident(name: str) -> str:
    name = name.replace('"', '""')
    return f'"{name}"'

def open_schema_if_needed(conn, scope_schema: Optional[str], debug: bool = False) -> None:
    if not scope_schema:
        return
    stmt = f'OPEN SCHEMA {quote_ident(scope_schema)}'
    if debug:
        print(f"[debug] executing schema context statement: {stmt}")
    conn.execute(stmt)


# ---------------------------------------------------------------------------
# Connection creation
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# NLS formats (queried once in main thread)
# IMPORTANT: use parameter_name + session_value (per your note)
# ---------------------------------------------------------------------------

def load_nls_formats(conn, debug: bool = False) -> Dict[str, str]:
    sql = (
        "select parameter_name, session_value "
        "from exa_parameters "
        "where parameter_name in ('NLS_DATE_FORMAT','NLS_TIMESTAMP_FORMAT')"
    )
    st = conn.execute(sql)
    rows = st.fetchall()

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


def format_date_value(d: datetime.date, nls_date_format: str) -> str:
    pyfmt = _oracleish_to_python_strftime(nls_date_format)
    return d.strftime(pyfmt)


def format_timestamp_value(dt: datetime.datetime, nls_ts_format: str) -> str:
    pyfmt = _oracleish_to_python_strftime(nls_ts_format)
    return dt.strftime(pyfmt)


# ---------------------------------------------------------------------------
# Prepared statements: value generation from st['parameter_data']
# ---------------------------------------------------------------------------

def _fallback_value_as_string_int_1_10() -> str:
    return str(random.randint(1, 10))

def _param_value_from_datatype(dt: Dict[str, Any], nls_formats: Dict[str, str]):
    t = str(dt.get("type", "VARCHAR")).upper()
    precision = dt.get("precision")
    scale = dt.get("scale")

    if t in ("DECIMAL", "NUMERIC", "NUMBER"):
        if scale is None:
            scale = 0
        if int(scale) <= 0:
            if precision is not None:
                max_val = 10 ** max(1, int(precision) - 1)
                return random.randint(0, max_val)
            return random.randint(0, 10_000_000)
        else:
            whole = random.randint(0, 10_000_000)
            frac = random.randint(0, (10 ** int(scale)) - 1)
            return Decimal(f"{whole}.{frac:0{int(scale)}d}")

    if t in ("INTEGER", "INT"):
        return random.randint(-2_000_000_000, 2_000_000_000)

    if t == "BIGINT":
        return random.randint(-9_000_000_000_000_000_000, 9_000_000_000_000_000_000)

    if t == "SMALLINT":
        return random.randint(-32768, 32767)

    if t in ("DOUBLE", "FLOAT"):
        return random.random() * 1000.0

    if t == "BOOLEAN":
        return random.choice([True, False])

    if t == "DATE":
        start = datetime.date(2000, 1, 1).toordinal()
        end = datetime.date(2030, 12, 31).toordinal()
        d = datetime.date.fromordinal(random.randint(start, end))
        return format_date_value(d, nls_formats["NLS_DATE_FORMAT"])

    if "TIMESTAMP" in t:
        start_ts = int(datetime.datetime(2000, 1, 1).timestamp())
        end_ts = int(datetime.datetime(2030, 12, 31, 23, 59, 59).timestamp())
        dtv = datetime.datetime.fromtimestamp(random.randint(start_ts, end_ts))
        return format_timestamp_value(dtv, nls_formats["NLS_TIMESTAMP_FORMAT"])

    # Fallback: string int 1..100 (reduces cast errors)
    return _fallback_value_as_string_int_1_10()


def _build_params_from_parameter_data(parameter_data: Dict[str, Any], nls_formats: Dict[str, str], debug: bool = False) -> List[Any]:
    cols = parameter_data.get("columns", []) or []
    vals = []
    for c in cols:
        dt = c.get("dataType", {}) or {}
        vals.append(_param_value_from_datatype(dt, nls_formats))
    if debug:
        print("[debug] generated prepared params:", vals)
    return vals


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

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
            if debug:
                print("[debug] parameter_data:", parameter_data)

            row_params = _build_params_from_parameter_data(parameter_data, nls_formats, debug=debug)

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

            # Simplified reconnect logic: only check conn.is_closed
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


# ---------------------------------------------------------------------------
# Error summary CSV
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run SQL queries in parallel sessions against Exasol using PyExasol")

    parser.add_argument("--creds-file", required=True, help="Path to JSON credentials file")

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--sql-file", help="Path to .sql file containing statements")
    src.add_argument("--audit-hours", type=int, help="Load distinct sql_text from EXA_DBA_AUDIT_SQL for last N hours")

    parser.add_argument("--sessions", type=int, default=4, help="Number of parallel sessions/workers")
    parser.add_argument("--commit", action="store_true", help="If set, commit successful statements. Default = rollback.")
    parser.add_argument("--timeout-sec", type=int, default=0,
                        help="Global wall-clock timeout in seconds (0 = no timeout)")
    parser.add_argument("--output-dir", default="./results", help="Directory to write report files")
    parser.add_argument("--debug", action="store_true", help="Enable debug printing")
    parser.add_argument("--dump-statements", action="store_true",
                        help="Print statement count + previews before running")

    args = parser.parse_args()
    make_output_dir(args.output_dir)

    creds = load_creds(args.creds_file)

    meta_conn = None
    query_objs: List[Dict[str, Any]] = []
    src_desc = ""

    try:
        meta_conn = create_connection(creds, debug=args.debug)

        # Load NLS formats ONCE (not in workers)
        nls_formats = load_nls_formats(meta_conn, debug=args.debug)

        if args.sql_file:
            queries = read_sql_file(args.sql_file, debug=args.debug)
            query_objs = [{"sql": q, "scope_schema": None, "source": "file"} for q in queries]
            src_desc = f"file:{args.sql_file}"
        else:
            if args.audit_hours is None or args.audit_hours <= 0:
                raise ValueError("--audit-hours must be > 0")
            query_objs = fetch_queries_from_audit(meta_conn, args.audit_hours, debug=args.debug)
            src_desc = f"audit:last_{args.audit_hours}_hours"

    finally:
        if meta_conn is not None:
            try:
                meta_conn.close()
            except Exception:
                pass

    print(f"[info] Loaded statements: {len(query_objs)} from: {src_desc}")
    print(f"[info] Default safety: ROLLBACK after each statement. Use --commit to enable commits.")

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
    conn_errors = [r.get("connection_error") for r in reports if r.get("connection_error")]

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

    if conn_errors:
        print("[warn] connection errors encountered:")
        for e in conn_errors:
            print("  -", e)

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
