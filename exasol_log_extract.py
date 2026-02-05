#!/usr/bin/env python3
"""
exasol_log_extract.py

Parse Exasol exacluster_debuginfo tar.gz bundles:
- locate node0 logs (files ending with .0)
- extract queries logged under "[Compiler - SQL]" (including multiline SQL)
- extract errors logged under "Error while SQL execution: Caught this exception:"
- write UNIQUE extracted queries to an output .sql file (streamed)
- print grouped error messages with counts (descending)

Designed for thousands of session files:
- streaming output
- dedupe with SHA-1 of normalized SQL

Usage:
  python exasol_log_extract.py \
    --tar exacluster_debuginfo_DATE-TIME.tar.gz \
    --out-dir ./parsed_output \
    --out-sql extracted_queries.sql
"""

from __future__ import annotations

import argparse
import hashlib
import re
import tarfile
import tempfile
from collections import Counter
from pathlib import Path
from typing import Iterator, List, Optional, Tuple


# --- Regex helpers -----------------------------------------------------------

ANSI_PREFIX = r'(?:\x1b\[[0-9;]*m)*'
TS_RE = re.compile(rf'^{ANSI_PREFIX}\d{{2}}\.\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}}\b')

COMPILER_SQL_RE = re.compile(r'\[Compiler\s*-\s*SQL\]\s*(.*)$')

ERROR_RE = re.compile(
    rf'{ANSI_PREFIX}\[33mError while SQL execution: Caught this exception:\s*(.*?){ANSI_PREFIX}\[0m?$'
)

STRIP_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

# Normalize SQL for de-duplication: collapse whitespace
NORM_WS_RE = re.compile(r'\s+')


def strip_ansi(s: str) -> str:
    return STRIP_ANSI_RE.sub('', s)


def is_timestamp_line(line: str) -> bool:
    return TS_RE.match(line) is not None


def normalize_sql_for_dedupe(sql: str) -> str:
    """
    Normalize SQL text for uniqueness:
    - strip leading/trailing whitespace
    - collapse internal whitespace to single spaces
    This keeps semantic similarity for dedupe even if logs wrap lines differently.
    """
    s = sql.strip()
    s = NORM_WS_RE.sub(' ', s)
    return s


def ensure_statement_ends_with_semicolon(sql: str) -> str:
    s = sql.rstrip()
    if not s:
        return sql
    if s.endswith(';'):
        return sql
    return s + ';'


# --- Finding node0 logs ------------------------------------------------------

def find_node0_exasol_log_dir(extracted_root: Path) -> Path:
    """
    Locate:
      exacluster_debuginfo/<node>/exa/logs/db/Exasol/
    Choose node0 by presence of *_SqlSession_*.0 files.
    """
    base = extracted_root / "exacluster_debuginfo"
    if not base.exists():
        raise FileNotFoundError(f"Expected directory not found: {base}")

    nodes = [p for p in base.iterdir() if p.is_dir()]
    if not nodes:
        raise FileNotFoundError(f"No node directories found under: {base}")

    for node_dir in sorted(nodes):
        log_dir = node_dir / "exa" / "logs" / "db" / "Exasol"
        if not log_dir.exists():
            continue
        if any(log_dir.glob("*_SqlSession_*.0")):
            return log_dir

    raise FileNotFoundError(
        "Could not find node0 Exasol log dir. "
        "Searched for exacluster_debuginfo/<node>/exa/logs/db/Exasol/*_SqlSession_*.0"
    )


# --- Parsing SQL session logs ------------------------------------------------

def iter_lines(path: Path) -> Iterator[str]:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield line.rstrip("\n")


def parse_sqlsession_file(path: Path) -> Tuple[List[str], List[str]]:
    """
    Returns:
      queries: list of extracted query texts (may include multiple lines)
      errors: list of extracted error messages
    """
    queries: List[str] = []
    errors: List[str] = []

    current_query_lines: Optional[List[str]] = None

    for line in iter_lines(path):
        # Query start marker
        m = COMPILER_SQL_RE.search(line)
        if m:
            if current_query_lines:
                q = "\n".join(current_query_lines).strip()
                if q:
                    queries.append(q)
            first_part = m.group(1) or ""
            current_query_lines = [first_part] if first_part.strip() else []
            continue

        # Multiline query capture until next timestamp line
        if current_query_lines is not None:
            if is_timestamp_line(line):
                q = "\n".join(current_query_lines).strip()
                if q:
                    queries.append(q)
                current_query_lines = None
                # fallthrough to also inspect this line for errors
            else:
                current_query_lines.append(line)
                continue

        # Error capture (ANSI-aware)
        em = ERROR_RE.search(line)
        if em:
            msg = strip_ansi(em.group(1)).strip()
            if msg:
                errors.append(msg)
            continue

        # Conservative fallback if ANSI differs
        marker = "Error while SQL execution: Caught this exception:"
        if marker in line:
            parts = line.split(marker, 1)
            if len(parts) == 2:
                msg = strip_ansi(parts[1]).strip()
                if msg:
                    errors.append(msg)

    # finalize if file ends mid-query
    if current_query_lines is not None:
        q = "\n".join(current_query_lines).strip()
        if q:
            queries.append(q)

    return queries, errors


# --- Main orchestration ------------------------------------------------------

def extract_tar_to_temp(tar_path: Path, temp_dir: Path) -> None:
    with tarfile.open(tar_path, "r:*") as tf:
        tf.extractall(path=temp_dir)


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="replace")).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tar", required=True, help="Path to exacluster_debuginfo_DATE-TIME.tar(.gz)")
    ap.add_argument("--out-dir", required=True, help="Output directory (created if needed)")
    ap.add_argument("--out-sql", default="extracted_queries_unique.sql", help="Output .sql filename")
    ap.add_argument("--max-files", type=int, default=0, help="For testing: cap session files processed (0 = no cap)")
    ap.add_argument("--max-queries", type=int, default=0, help="For testing: cap unique queries written (0 = no cap)")
    ap.add_argument("--progress-every", type=int, default=500, help="Print progress every N files")
    args = ap.parse_args()

    tar_path = Path(args.tar).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_sql_path = out_dir / args.out_sql

    if not tar_path.exists():
        raise FileNotFoundError(f"Tar file not found: {tar_path}")

    error_counter: Counter[str] = Counter()
    seen_query_hashes = set()

    files_processed = 0
    unique_written = 0
    total_queries_seen = 0

    with tempfile.TemporaryDirectory(prefix="exa_debuginfo_") as td:
        temp_root = Path(td)
        extract_tar_to_temp(tar_path, temp_root)

        node0_log_dir = find_node0_exasol_log_dir(temp_root)
        session_files = sorted(node0_log_dir.glob("*_SqlSession_*.0"))

        if args.max_files and args.max_files > 0:
            session_files = session_files[: args.max_files]

        if not session_files:
            print(f"No SqlSession logs found in: {node0_log_dir}")
            return

        # Stream output: write unique queries as we go
        with out_sql_path.open("w", encoding="utf-8") as out:
            for f in session_files:
                files_processed += 1
                if args.progress_every and (files_processed % args.progress_every == 0):
                    print(f"[progress] files={files_processed} unique_queries={unique_written} errors={sum(error_counter.values())}")

                queries, errors = parse_sqlsession_file(f)

                # errors: normalize whitespace for better grouping
                for e in errors:
                    norm_err = " ".join(e.split())
                    error_counter[norm_err] += 1

                # queries: dedupe + write unique
                for q in queries:
                    total_queries_seen += 1
                    norm = normalize_sql_for_dedupe(q)
                    h = sha1_hex(norm)
                    if h in seen_query_hashes:
                        continue
                    seen_query_hashes.add(h)

                    unique_written += 1
                    out.write(f"-- EXTRACTED_UNIQUE_Q{unique_written:06d}\n")
                    out.write(ensure_statement_ends_with_semicolon(q))
                    out.write("\n\n")

                    if args.max_queries and unique_written >= args.max_queries:
                        print(f"[info] max unique queries reached ({args.max_queries}); stopping early.")
                        break

                if args.max_queries and unique_written >= args.max_queries:
                    break

    # Summary
    print("=" * 80)
    print(f"Session files processed        : {files_processed}")
    print(f"Total queries encountered      : {total_queries_seen}")
    print(f"Unique queries written         : {unique_written}")
    print(f"Output SQL file                : {out_sql_path}")
    print("=" * 80)

    if not error_counter:
        print("No errors found in logs.")
        return

    print("Error messages grouped by count (descending):")
    print("-" * 80)
    for msg, cnt in error_counter.most_common():
        print(f"[{cnt:6d}] {msg}")
    print("-" * 80)


if __name__ == "__main__":
    main()
