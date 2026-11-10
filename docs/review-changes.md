# Dependency, correctness, and dashboard update

This implements the approved code-review items F1–F16, D1–D5, and U1–U4.

## Compatibility and installation

Rill now requires Python 3.10 or newer. DuckDB 1.5.4 supports Python 3.10+;
DuckDB 2.0 requires Python 3.11+. The pinned modern development profile uses
Python 3.12 because NumPy 2.5 requires it. Declared runtime bounds are
`duckdb>=1.5.4,<2.1` and `pyarrow>=14,<27`; unsupported future releases need a
compatibility check before expanding those bounds.

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]' -c constraints/test-duckdb20.txt
python -m pytest --timeout=30
cd dashboard
npm ci --ignore-scripts
npm test
npm start
```

For Python 3.10 or the older supported DuckDB, use
`constraints/test-duckdb15.txt`. Do not reuse a virtual environment copied from
another computer: its interpreter symlinks may be invalid.

Chart.js, Hammer.js, and the zoom plugin are served locally at lockfile-pinned
versions. The unused native Node DuckDB dependency has been removed; the Python
worker uses the Python environment selected by `RILL_PYTHON`, `VIRTUAL_ENV`, or
`CONDA_PREFIX`, falling back to `python3` on PATH.
`RILL_EXTENSION_DIRECTORY` optionally selects the worker's DuckDB extension
cache, useful for isolated environments with a preinstalled Quack extension.

## Data and lifecycle contracts

- **F1:** Arrow query APIs return `pyarrow.Table` on DuckDB 1.5.4 and 2.0 using
  relational result conversion. Internal scalar reads consume their entire
  result before the connection is reused.
- **F2:** Engine-owned database operations share the engine lock. Quack startup
  has a separate cursor/connection. `quack_ready` signals successful serving;
  `quack_error` describes startup failure. An unexpected loop failure sets
  `last_error` and clears the running flag. There is no automatic restart.
  Direct access to a caller-supplied/raw connection still requires caller-side
  synchronization and complete result consumption.
- **F3:** Timestamp lookup explicitly restores input row order. Deduplication
  keeps the last occurrence of each key and orders surviving incoming records
  by their input position. `z_insert_ts` stays associated with its key;
  `z_update_ts` is updated as before.
  This prevents future misassociation; it cannot reconstruct timestamps already
  corrupted in existing state without a trustworthy source or backup.
- **F4:** Scheduled tasks execute in registration order. A later due task sees
  output published by an earlier task in the same step. No dependency graph or
  automatic reordering is added.
- **F5:** Metrics and dashboard queries accept exactly one parsed read-only
  statement: SELECT/WITH, SHOW, DESCRIBE, PRAGMA show_tables, or EXPLAIN of an
  allowed query. This does not sandbox SQL functions or their file access;
  dashboard users are trusted administrators.
- **F7:** Alert batches remain pending until their database transaction commits.
  Failed writes retry on the next step; newly arriving alerts stay queued.
- **F10–F11:** Row dictionaries retain fields appearing after the first row.
  Error-mode connector pushes are atomic. JSON chunks must contain complete
  NDJSON records. Record and byte caps are both enforced; byte caps include
  newline delimiters. Drop modes discard whole records and discard individually
  oversized records. Error mode raises without changing the buffer.
- **F12:** Explicit limits must be positive; age limits must also be finite.
  Missing or invalid retention time columns raise instead of disabling pruning.
- **F13–F14:** Alerts expire after one hour by default (`alert_ttl_seconds=3600`),
  even during quiet ticks. Telemetry and task-latency retention defaults to ten
  minutes (`metrics_ttl_seconds=600`). Scheduled output history has a separate
  ten-minute default (`query_history_ttl_seconds=600`) and is pruned on every
  step after the output table is managed by a task. Reusing an existing output
  table enrolls its older history in that policy. Set the desired history
  window before starting an engine against an existing database. Other
  unrelated persisted tables are not purged. All three periods are configurable
  positive finite seconds. The discovery catalog includes persisted task outputs.
- **F15:** DuckDB memory metrics use numeric byte measurements. Unavailable
  readings are null and the UI displays “Unavailable”, not zero. Named DuckDB
  database instances report their own aggregate allocations; these are not an
  attribution of Arrow buffers or total process RAM.
- **F16:** `stop()` closes owned connections even for synchronous engines and is
  idempotent and terminal. Create another engine to restart after stop. An
  externally supplied live connection remains open. A timed-out worker retains
  its connections; retry stop after it finishes. Pending alerts after a failed
  shutdown flush also keep the engine open for a retry. Context-manager usage
  closes resources automatically.

## Dashboard behavior and access

- **F6:** External values are escaped before HTML rendering; application-owned
  markup is kept separate. A restrictive script policy also limits scripts to
  the local origin.
- **F8–F9:** Query configurations/results and rule cooldowns belong to each
  connection. Background connections and table-only scheduled queries still
  evaluate business alerts. Switching tabs renders the selected connection's
  cached results. Reconnecting restores manual schedules and stopped-query
  choices for the current browser session. Reloading the page does not persist
  connection credentials or running schedules; saved SQL remains separate.
  Failed or empty scheduled results clear that query's previous alert values.
- **U1–U4:** A compact header contains connection context and an Options menu.
  Throughput, latency, CPU, and system memory are the primary overview cards.
  Arrow/DuckDB memory details collapse by default. Saved queries, scheduling,
  and chart configuration use expandable sections. Widget grids, forms, and
  navigation adapt to narrow screens. The theme picker has been removed; the
  default theme applies consistently, including for users with old saved colors.

The server binds to **127.0.0.1 by default**. Remote binding requires
`RILL_DASHBOARD_TOKEN`; opening the dashboard exchanges that token for an
HttpOnly, SameSite=Strict session cookie lasting eight hours. Tokens are not
stored in localStorage or URL parameters. For remote deployment, use HTTPS at a
trusted reverse proxy and configure the exact public host/origin:

```sh
HOST=0.0.0.0 \
RILL_DASHBOARD_TOKEN='<a-long-random-secret>' \
RILL_ALLOWED_HOSTS='rill.example.com' \
RILL_ALLOWED_ORIGINS='https://rill.example.com' \
RILL_COOKIE_SECURE=1 \
npm start
```

Comma-separated host/origin values are exact matches, not wildcards. Host and
Origin checks apply to WebSocket upgrades; API mutations also check Origin.
Loopback access can optionally require the same token by setting it locally.

Webhook destinations must use HTTPS and match `RILL_WEBHOOK_ORIGINS` (defaults:
`https://hooks.slack.com,https://chat.googleapis.com`). To use a custom webhook,
add its exact HTTPS origin to this administrator-controlled list. DNS results are
checked and pinned for the request; private/reserved addresses and redirects are
rejected. Requests time out after ten seconds. These checks mean that arbitrary
or private-network webhook destinations previously accepted are now rejected.

## Verification

`pytest` collects only `tests/`; import-time diagnostic scripts in ignored
`local_test/` are excluded. Unexpected thread exceptions fail tests. Regression
coverage includes key/timestamp association and TTL, chained tasks, transactional
alerts, statement validation, bounded ingestion, cleanup, frontend rendering,
server isolation, reconnects, and HTTP/WebSocket access controls.

Real Quack tests are explicit because they download a platform/version-specific
extension and bind a local TCP listener:

```sh
RILL_TEST_QUACK=1 python -m pytest tests/test_quack_integration.py --timeout=60
```

Once opted in, unavailable extensions fail that test rather than quietly skip.
CI runs the dependency matrix and dashboard tests; manual workflow dispatch also
runs Quack integration for both DuckDB versions.

### Local verification results

Verified on Python 3.12:

| Environment | Result |
| --- | --- |
| DuckDB 2.0 / Arrow 26 | 89 passed, 1 opt-in integration test skipped |
| DuckDB 1.5.4 / Arrow 25 | 89 passed, 1 opt-in integration test skipped |
| DuckDB 1.5.4 / Arrow 14 / NumPy 1.26 | 89 passed, 1 opt-in integration test skipped |
| Real Quack integration, separately enabled | 3 passed on each DuckDB version |
| Dashboard tests | 10 passed |

The actual Node → Python → Quack bridge also received live metrics and returned
`42` from a read-only test query against a disposable engine. No user database
was used for that check. Python 3.10 is configured in CI but was not available
for local execution.

The final 0.1.2 browser check verified the default theme and Options menu after
removing the theme picker. The earlier preview connectivity issue was resolved
for that check. DOM behavior and access-control tests passed; the complete
connected dashboard and responsive layouts have not yet been visually verified.

See [operations and benchmark notes](operations.md) for alert cooldowns, actual
memory-versus-disk measurements, and safeguards still awaiting approval.
