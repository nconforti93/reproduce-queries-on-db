# Exasol Parallel Runner

`exasol_parallel_runner.py` is a utility to execute many SQL statements in parallel against an Exasol cluster using `pyexasol`. It supports executing either:

- SQL statements read from a file, or
- SQL statements fetched from the Exasol audit table (`EXA_DBA_AUDIT_SQL`) for the last N hours.

It also supports server-side prepared statements (questions marks `?`), context-aware parameter value generation (for `to_date`/`to_timestamp` contexts), and optional insertion of grouped error summaries into an Exasol table.

---

## Features

- Parallel execution using threads (configurable sessions).
- Prepared-statement support: the script prepares the statement once, inspects parameter metadata and generates values for execution.
- `to_date(?, ...)` and `to_timestamp(?, ...)` aware: generated bind values respect DB `NLS_DATE_FORMAT` and `NLS_TIMESTAMP_FORMAT` values.
- Robust handling of `?` inside comments or string literals (those do **not** count as parameters).
- Optional insertion of grouped error rows into an Exasol table (created automatically if missing).
- Disables SQL preprocessor script on every new session (`ALTER SESSION SET SQL_PREPROCESSOR_SCRIPT=''`).
- Rollback by default after each executed statement to avoid accidental DDL/DML changes (use `--commit` to commit).
- Final human-readable summary + JSON report and (optional) CSV of grouped errors.

---

## Requirements

- Python 3.8+
- `pyexasol` installed:  
  ```bash
  pip install pyexasol
  ```

## Setup

Create a JSON file (e.g. creds.json) containing PyExasol connection options (NOTE: The user you select should by either SYS or a user with DBA permissions because it will run queries on potentially any object in the DB):

```
{
  "user": "SYS",
  "password": "exasol",
  "address": "192.168.56.101",
  "port": 8563,
  "schema": "RETAIL",
  "encryption": true
}
```

## Usage

The script accepts the following parameters:

Choose either sql-file or audit-hours to choose where to get the list of queries.

```
--sql-file /link/to/file.sql
Read statements from a .sql file and execute them.

--audit-hours <n>
Pull distinct sql_text, scope_schema from EXA_DBA_AUDIT_SQL for the last n hours, ignoring all TRANSACTION, DCL, and OTHER command classes where the query did not originally return an error.
```

Other parameters:
```
--ignore-import-export
IMPORT or EXPORT Statements from EXA_DBA_AUDIT_SQL are ignored and not executed.

--sessions <n>
Number of parallel worker sessions (default: 4).

--commit
Commit successful statements.
Default behavior is ROLLBACK after every statement.

--timeout-sec <seconds>
Stop the run if the overall runtime exceeds this timeout.

--output-dir <dir>
Output directory for run artifacts (default: ./results).

--dump-statements
Print the first 10 parsed statements (useful for sanity checks).

--debug
Verbose debug output.

--error-target <SCHEMA.TABLE>
Insert grouped errors into an Exasol table (created if missing).
```

Important note: If --error-target is specitifed, the results on queries which ran with an error is written into the specified table and committed, even if the script is run without the --commit parameter.

### Examples:
Executing the script with queries from auditing:
```
exasol_parallel_runner.py --creds-file exasol_creds.json --audit-hours 24 --sessions 10 --timeout-sec 1800 --output-dir ./results --error-target "TEST.ERRORS" --ignore-import-export
```

Executing the script with a list of queries in a file:
```
exasol_parallel_runner.py --creds-file exasol_creds.json --sql-file test.sql --sessions 10 --timeout-sec 1800 --output-dir ./results --error-target "TEST". ERRORS" --ignore-import-export
```


## Output
The script generates a JSON report for each query that was executed, including the query executed, if it was a prepared statement, the parameters used, the query executed (with ? replaced), error message, and the duration.

Additionally, a CSV report is prepared with a summary of each error with the following columns:
* run_id
* run_ts (Run Timestamp)
* query
* sql_executed (including replaced parameter values)
* error_message
* error_type (either script or database depending on the error source)
* count

if `--error-target` is specified, this data is also saved into a database table for easy querying. 

After execution, the script prints a summary of all activities including:
* statements loaded / executed
* number of prepared attempts
* successes / failures
* commits / rollbacks
* number of error groups
* whether error groups were inserted into the database

## Limitations
* Queries with duplicate column names in the result set can fail when executed as prepared statements, because prepare-time metadata retrieval may throw an error.
* The current implementation does not attempt advanced workarounds.
* Prepared statement values are generated heuristically from parameter_data.
* Some values may still cause cast/validation errors; those are expected and are captured.
* In file mode, IMPORT/EXPORT often fail due to missing files/paths. In audit mode, you can filter them with `--ignore-import-export`.