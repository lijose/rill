# Rill Streaming Engine (`rills`)

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Rill is a single-node micro-batching engine for streaming analytics using
PyArrow tables and DuckDB SQL. Connectors buffer incoming events; each engine
step ingests data, applies table retention, runs due SQL transformations, and
records telemetry. Arrow compute kernels and DuckDB perform the main table
operations; ingestion, coordination, alerts, and dashboard serialization also
use Python and JavaScript. Operations are not universally zero-copy.

Current repository version: **0.1.2**. Dashboard version: **1.0.1**.
A GitHub commit does not publish a package to PyPI.

## Install

To install the version currently published on PyPI:

```sh
python -m pip install rills
```

To use this repository version with reproducible development dependencies, use
Python **3.12**:

```sh
git clone https://github.com/lijose/rill.git
cd rill
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]' -c constraints/test-duckdb20.txt
```

Runtime bounds are DuckDB `>=1.5.4,<2.1` and PyArrow `>=14,<27`. For Python
3.10 or the older supported DuckDB, use `constraints/test-duckdb15.txt`.
DuckDB 2.0 requires Python 3.11+; the modern pinned development profile requires
Python 3.12 because of its NumPy version.

## Data flow

```text
Connectors → live Arrow tables → scheduled SQL → live output tables
                    │                              │
                    │                    persisted output snapshots
                    └── engine telemetry/alerts ───┤
                                                   ▼
                                            metrics DuckDB
                                                   │ Quack TCP
                                            Python worker
                                                   │
                                         Node/WebSocket → browser
```

Live input tables belong to the engine's live DuckDB connection. The separate
metrics database contains telemetry, engine/user alerts, dashboard query
configuration, and scheduled SQL output history. Quack serves this metrics
database; registering a live input table does **not** expose it to the dashboard.
TCP results are serialized, not shared-memory references across processes.

## Table modes and retention

- **Snapshot/upsert:** with a primary key, incoming rows replace matching keys.
  Duplicate keys within a batch keep the last occurrence. Without a primary key,
  incoming rows append. Retention is optional in snapshot mode.
- **Append:** incoming rows append even when a primary key is configured. A
  retention policy with `max_rows` or `max_age_seconds` is required.
- **Metadata:** `z_insert_ts` and `z_update_ts` use Unix seconds. Normal upserts
  preserve the original insertion timestamp and refresh the update timestamp.
  Explicitly supplied metadata is retained. Full SQL output replacement creates
  fresh metadata when the query does not supply it.
- **Row TTL:** `RetentionPolicy(max_age_seconds=...)` uses `z_insert_ts` in every
  mode by default. Updates do not reset that TTL. Choose
  `time_column="z_update_ts"` for update-based expiry, or a custom event-time
  column. The engine does not choose a different column based on table mode.
- **Row-count retention:** `max_rows` keeps the final rows in table order; it does
  not sort by timestamp. Limits must be positive; age limits must also be finite.

`MemoryConnector` supports a record-count buffer limit. `JSONStreamConnector`
supports record-count and byte limits, including NDJSON newline delimiters.
Overflow policies are `drop_oldest`, `drop_newest`, and `error`. JSON pushes must
contain complete NDJSON records; error-mode pushes leave the buffer unchanged.

## A minimal SQL pipeline

This synchronous example needs no Quack server and creates no database file:

```python
import pyarrow as pa
from rills import RillEngine, MemoryConnector, ScheduledSQLTask, schema

with RillEngine(metrics_db_path=":memory:") as engine:
    engine.register_table("orders", schema=schema([
        ("order_id", pa.int64()),
        ("user_tier", pa.string()),
        ("amount", pa.float64()),
    ], primary_key="order_id", mode="snapshot"))

    orders = MemoryConnector(target_table="orders")
    engine.add_connector(orders)
    engine.add_sql_task(ScheduledSQLTask(
        name="tier_revenue",
        query="""
            SELECT user_tier, SUM(amount) AS total_revenue
            FROM orders GROUP BY user_tier
        """,
        output_table="revenue_by_tier",
        interval_seconds=1.0,
    ))

    orders.push({
        "order_id": [1, 2, 3],
        "user_tier": ["Gold", "Silver", "Gold"],
        "amount": [100.50, 45.00, 250.00],
    })
    engine.step()
    print(engine.duckdb.query("SELECT * FROM revenue_by_tier").to_pylist())
    print(engine.query_metrics("SELECT * FROM revenue_by_tier").to_pylist())
```

Due tasks run in registration order, so a later task sees an earlier task's
new output in the same step. SQL output replaces its live table and appends a
snapshot, with `insert_ts`, to the metrics database.

For background processing, call `engine.start()` and eventually `engine.stop()`.
`stop()` is terminal and idempotent; construct another engine to restart.
Context-manager exit also closes owned resources. A caller-supplied live DuckDB
connection remains open. Unexpected background-loop failures are available in
`engine.last_error`.

## Dashboard and Quack

Configure Quack on the constructor, then call `start()` with no arguments:

```python
engine = RillEngine(
    quack_address="quack:127.0.0.1:9494",
    quack_token="replace-with-your-token",
)
engine.start()
# quack_ready signals successful startup; quack_error records startup failure.
```

Dashboard SQL runs in the worker against the metrics database attached as
`remote_db`. For example, after the pipeline above has published output, a
persistent, running engine can register this dashboard query:

```python
engine.add_dashboard_scheduled_query(
    name="Revenue by tier",
    sql="""
        SELECT user_tier, total_revenue
        FROM remote_db.revenue_by_tier
        WHERE insert_ts = (SELECT MAX(insert_ts) FROM remote_db.revenue_by_tier)
    """,
    interval_seconds=1.0,
    chart_type="bar",
    x_col="user_tier",
    y_col="total_revenue",
)
```

To run the included demo, activate the Python environment and execute
`python examples/quack_dashboard_demo.py`. The demo uses token `demo` and binds
Quack to all interfaces; use it in a trusted development environment.

In a second terminal, from the repository root:

```sh
. .venv/bin/activate
cd dashboard
npm ci --ignore-scripts
npm start
```

Open `http://127.0.0.1:3000` and connect to `quack:127.0.0.1:9494` with token
`demo`. The dashboard requires Node.js 18+ and the Python worker environment.
It provides **Overview**, **Queries**, **Alert settings**, and **Alert Logs**.
Primary system cards come first; detailed memory readings and advanced query
controls are expandable. The default theme is fixed.

The Node server binds to localhost by default. Remote access requires a dashboard
access token, separate from the Quack token. Webhooks must use approved public
HTTPS origins. See the [dashboard guide](dashboard/README.md) for configuration.

## Alerts, metrics, and storage

```python
engine.alert("Batch processed")
engine.warn("Order backlog is high")
engine.error("Payment processing failed", detail="Application error details")
engine.push_alert("Revenue below threshold", severity="warning")
```

Engine/user alerts are buffered, persisted to `rill_alerts`, and retried if the
write fails. These alerts have no cooldown. Browser threshold rules have
configurable cooldowns per rule and connection; those rule events stay in the
browser's local log and are not written to `rill_alerts`.

Default retention is 600 seconds for telemetry/task latency, 600 seconds for
managed SQL output history, and 3,600 seconds for engine/user alerts. The
constructor options are `metrics_ttl_seconds`, `query_history_ttl_seconds`, and
`alert_ttl_seconds`. These policies do not impose a disk-size cap.

The displayed DuckDB byte metrics measure memory allocations, not `.db` or WAL
file sizes. `memory_budget_mb` warns about Arrow allocation; it is not a hard
process-memory or disk limit. See [operations and benchmark notes](docs/operations.md)
for cooldown details, storage monitoring, and current benchmark limitations.

## Verification and examples

```sh
python -m pytest tests/ --timeout=30 -q
cd dashboard
npm test
```

The normal Python suite skips the opt-in real Quack test. To enable it:

```sh
RILL_TEST_QUACK=1 python -m pytest tests/test_quack_integration.py --timeout=60 -q
```

GitHub CI checks the supported dependency profiles and dashboard tests. Manual
workflow dispatch also runs real Quack integration. CI does not publish packages
or deploy the dashboard.

Examples in `examples/` cover live streaming, scheduled multi-stream joins,
Quack dashboarding, and append/aggregation benchmarking. The benchmark is an
exploratory script, not yet a reproducible performance regression gate.

See [compatibility and migration notes](docs/review-changes.md),
[CONTRIBUTING.md](CONTRIBUTING.md), and [CHANGELOG.md](CHANGELOG.md).

## License

[MIT](LICENSE).
