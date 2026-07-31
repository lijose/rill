# <img src="dashboard/public/logo.png" width="40" valign="middle" alt="Rill Logo"> Rill Streaming Engine (rills)

[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![PyArrow](https://img.shields.io/badge/Powered%20by-PyArrow-orange.svg)](https://arrow.apache.org/)
[![DuckDB](https://img.shields.io/badge/SQL%20Engine-DuckDB-yellow.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)


**Rill** is a lightweight, single-node micro-batching engine designed for high-performance streaming analytics using pure **PyArrow** and zero-copy **DuckDB**.

By bypassing traditional Python data structures and the Global Interpreter Lock (GIL) where possible, Rill ingests continuous event streams directly into C++ contiguous memory (`pyarrow.Table`). It leverages a scheduled processing trigger to buffer, join, aggregate, and query incoming data efficiently without the overhead of event-by-event Python loops.

From raw data ingestion to complex multi-stream joins, aggregations, scheduled SQL pipelines, and **structured alerting**, every operation is strictly contained within C++ vectorized memory, making Rill the ideal zero-infrastructure solution for live data processing.

---

## 🏛️ Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                   RILL ENGINE                                    │
│                                                                                  │
│  ┌─────────────────────────┐      Micro-Batch Loop     ┌───────────────────────┐ │
│  │    Input Connectors     │      (Trigger Interval)   │    Table Registry     │ │
│  │                         │                           │                       │ │
│  │ • Memory / JSON Stream  │ ────────────────────────> │ • Snapshot (PK Upsert)│ │
│  │ • WebSocket / Kafka     │                           │ • Append-Only (TTL)   │ │
│  │ (Backpressure Buffer)   │                           │ (z_insert/z_update)   │ │
│  └─────────────────────────┘                           └───────────┬───────────┘ │
│               │                                                    │             │
│               └─────────────────────────┐                          │             │
│                                         ▼                          ▼             │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                           Compute Transformations                           │ │
│  │                                                                             │ │
│  │ • Zero-Copy DuckDB Scheduled SQL Pipelines (Live Tables In-Memory)          │ │
│  │ • Multi-Stream Relational Joins & Aggregations                              │ │
│  │ • Vectorized TTL Pruning (Age / Row Count)                                  │ │
│  └─────────────────────────────────┬───────────────────────────────────────────┘ │
│                                    │                                             │
│                                    ▼                                             │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                      Persistent Metrics & Telemetry                         │ │
│  │                                                                             │ │
│  │ • Dual-Connection Topology: Writes business metrics & engine telemetry      │ │
│  │   to an on-disk `rill_metrics.db` for isolated serving and dashboarding.    │ │
│  │ • Structured Alerts Pipeline: Engine exceptions & user-pushed alerts        │ │
│  │   written to `rill_alerts` table for dashboard visibility.                  │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features & Governance

### 1. Table Processing Modes (`snapshot` vs `append`) & Mandatory TTL
- **Snapshot Mode (`mode="snapshot"` or `"upsert"`)**: Maintains unique state tables by atomically replacing matching rows when `primary_key` is defined (`pc.is_in` / `left anti` join). TTL (`RetentionPolicy`) is optional.
- **Append-Only Mode (`mode="append"`)**: Every incoming micro-batch is appended strictly without overwriting previous rows, even if a primary key or ID column is present.
- **Mandatory TTL Governance**: Because append-only event streams grow continuously with every event, Rill **strictly enforces** that a `RetentionPolicy` (`max_rows` or `max_age_seconds`) is provided for append-only tables (`mode="append"`), preventing unbounded streams from exhausting system RAM over time.

### 2. Automated System Metadata Columns (`z_insert_ts`, `z_update_ts`)
Every table inside Rill automatically maintains two system timestamps (`pa.float64()` unix seconds since epoch):
- **`z_insert_ts`**: Recorded when a record first arrives. During primary-key upserts, existing records preserve their original `z_insert_ts` via zero-copy C++ lookup.
- **`z_update_ts`**: Automatically refreshed to the current timestamp on every modification.
- **Governed TTL**: By default, `RetentionPolicy(max_age_seconds=...)` uses `z_insert_ts` (`time_column="z_insert_ts"`) to accurately evict aged rows during each micro-batch tick.

### 3. Schema Primary Key & Mode Embedding
Use `rills.schema([fields], primary_key="user_id", mode="append")` to embed governance properties directly inside PyArrow schema metadata (`schema.metadata[b"primary_key"]`). When a `RillTable` is initialized with an enriched schema, it automatically extracts its primary key and operating mode without manual boilerplate.

### 4. PyArrow Memory Budget Governance
Configure `RillEngine(memory_budget_bytes=...)` or `memory_budget_mb=...` along with an optional `on_memory_warning` callback. During every micro-batch iteration (`step()`), Rill monitors `pa.total_allocated_bytes()` across all C++ memory pools and emits a `ResourceWarning` if the memory threshold is crossed.

### 5. Multi-Stream Joins & Aggregations (`TableJoinTask`)
Continuously join two live stream tables (`left_table`, `right_table`) via C++ relational hash-joins (`pc.is_in` / `Table.join`) and calculate real-time aggregations (`sum`, `count`, `avg`, `min`, `max`) without exiting PyArrow memory.

### 6. Bounded Connector Backpressure
Input connectors (`MemoryConnector`, `JSONStreamConnector`) enforce configurable limits (`max_buffer_records`, `max_buffer_bytes`) paired with overflow strategies (`"drop_oldest"`, `"drop_newest"`, `"error"`) that slice excess data cleanly across batch boundaries.

### 7. DuckDB Zero-Copy SQL Pipelines
Execute standard SQL queries across any live `pyarrow.Table` using zero-copy DuckDB (`duckdb.query`). Attach **Scheduled SQL Tasks** (`ScheduledSQLTask`) to continuously transform live data streams into new dynamic tables at precise intervals.

### 8. Quack Server & WebSocket Bridge (`quack:0.0.0.0:9494`)
Expose all internal Rill tables and real-time engine telemetry over native TCP/IP using the **Quack Protocol**. When enabled via `engine.start(quack_port=9494, quack_token="secure_token")`, Rill starts a background server thread that allows external tools, worker processes, and dashboards to securely query live PyArrow memory with zero-copy shared memory semantics.

### 9. Dynamic KPI Dashboards via Scheduled Queries
Define Scheduled SQL Tasks (`engine.add_dashboard_scheduled_query(...)`) to dynamically extract and aggregate domain KPIs (e.g., maximum order tickets, cumulative revenue) in real-time. The dashboard instantly renders these live views as business metrics and evaluates threshold alerts against them.

### 10. Structured Alerts & Observability Pipeline
Rill includes a full-stack **alerts and observability pipeline** that captures engine exceptions, user-pushed alerts, and threshold-based rule triggers into a persistent `rill_alerts` DuckDB table — visible in real-time on the dashboard.

- **Structured Logging**: Every module uses `logging.getLogger(__name__)` with proper Python logging. No silent `except: pass` blocks — all errors are logged and recorded.
- **Server-Side Alert Recording**: All internal engine exceptions (connector failures, SQL task errors, memory budget warnings) are automatically recorded as structured alerts with severity (`error`, `warning`, `info`), source component, and traceback detail.
- **User-Facing Alert API**: Push custom alerts directly from application code:
  ```python
  # Primary method with full control
  engine.push_alert("Revenue dropped below threshold", severity="warning", detail="Current: $847")
  engine.push_alert("Payment gateway timeout", severity="error", detail=traceback.format_exc())

  # Convenience methods
  engine.alert("Batch processed successfully")       # severity="info"
  engine.warn("High latency detected on orders")     # severity="warning"
  engine.error("Database connection failed")          # severity="error"
  ```
- **Dashboard Integration**: All alerts (engine, user, and rule-based) appear in the Alert Logs view with severity badges (🔴 Error, 🟡 Warning, 🔵 Info), source badges (👤 User, ⚙️ Engine, ⚡ Rule), filter toggles, and toast notifications.
- **Automatic Retention**: Alerts older than 1 hour are automatically pruned during each metrics flush cycle.

---

## 📊 Live Business Intelligence & Quack Dashboard

Rill includes a state-of-the-art, dark-themed **Live Business Intelligence & System Monitoring Dashboard** built with Node.js, WebSocket Quack Bridge (`quack_worker.py`), and Chart.js.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        NODE.JS / BROWSER DASHBOARD                              │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │ 💎 Key Business Intelligence & Scheduled Queries (Live Streaming)         │  │
│  │  [ max_order_amount ($): $497.34 ]  [ total_revenue ($): $13,025.21 ]     │  │
│  │  [ cancelled_ratio: 26.5% ]                                               │  │
│  │  📈 Scheduled Line & Bar Charts evaluating Live SQL Data                  │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │ ⚙️ Engine Infrastructure & Resource Utilization                           │  │
│  │  💻 CPU: 37.6% | 💾 RAM: 3.6 GB | 🏹 PyArrow: 12.47 MB | ⚡ Latency: 17ms│  │
│  │  📊 System Resource History (CPU / Memory Trend Graph)                    │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────▲────────────────────────────────────────┘
                                         │ WebSocket JSON Stream
┌────────────────────────────────────────▼────────────────────────────────────────┐
│                        NODE.JS QUACK BRIDGE SERVER                              │
│                     (dashboard/server.js + quack_worker.py)                     │
└────────────────────────────────────────▲────────────────────────────────────────┘
                                         │ Native Quack Protocol (TCP Socket)
┌────────────────────────────────────────▼────────────────────────────────────────┐
│                     RILL STREAMING ENGINE (`quack:9494`)                        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Dashboard Highlights:
- **📊 Multi-View Glassmorphic Navigation**: Seamlessly switch between the **Metrics Console**, **Custom Queries**, **Alert Rules & Settings**, and **Alert Logs** right from the top bar.
- **💎 Business Intelligence Centerpiece**: Highlights Custom Scheduled Queries mapped to Live Charts and extracts numeric results into business KPI metrics that trigger alerts instantaneously.
- **🔍 Custom SQL Queries & Chart Mapping**: Write, execute, and schedule ad-hoc SQL queries against the on-disk `rill_metrics` database. Map query results on the fly to **Interactive Line, Bar, and Scatter Charts** with zoom/pan capabilities powered by `chartjs-plugin-zoom`.
- **⚙️ Separated System Infrastructure**: Cleanly separates host CPU, System Memory, PyArrow C++ pool allocations, DuckDB storage size (`Live` vs `Metrics` DB), throughput (RPS), and step latency into a dedicated secondary monitoring section.
- **🔔 Unified Alert Logs**: Displays engine-side exceptions, user-pushed alerts, and threshold rule triggers in a single unified view with **severity badges** (🔴🟡🔵), **source badges** (👤 User / ⚙️ Engine / ⚡ Rule), **filter toggles** (All / Errors / Warnings / User / Engine / Rules), and severity-colored **toast notifications** for real-time visibility.
- **🔊 Browser-Native Sound Alerts**: Real-time Web Audio API acoustic engine with selectable tones (`Classic Beep`, `Warning Siren`, `Gentle Chime`, `Urgent Buzzer`), customizable duration sliders (1s–30s), and instant sound preview.
- **⚪ Offline / Disconnected Grey Tabs**: Multi-tab connection bar with automatic health tracking. If a remote Quack server drops connection or exits, its tab instantly turns grey with a grey status dot and hover failure details.
- **🔗 Server-Side Webhook Relay (`POST /api/webhook/send`)**: Bypasses browser CORS restrictions by routing real-time alert notifications directly to **Slack Incoming Webhooks**, **Google Chat / Google Meet Cards**, or **Custom JSON endpoints**.
- **⚡ Real-Time Threshold Rule Engine**: Evaluates custom trigger rules (`>`, `>=`, `<`, `<=`, `==`) on system and business metrics with cooldown protection. Rules and settings persist automatically in `localStorage`.

---

## 🚀 Quickstart Guide

### Installation

Install `rills` directly from PyPI using pip:

```bash
pip install rills
```

**Development / Source Installation:**
If you want to contribute or run the dashboard/examples directly from the source code:

```bash
git clone https://github.com/lijose/rill.git
cd rill
python3 -m venv .env
source .env/bin/activate 
pip install -e .
```

### 1. Running the Live Dashboard & Quack Demo

Step 1: Launch the Rill engine with live simulated orders and custom business metrics:
```bash
python3 examples/quack_dashboard_demo.py
```
*(Note your `quack:127.0.0.1:9494` address and auth token printed in the terminal).*

Step 2: In a separate terminal, start the Node.js Dashboard Bridge:
```bash
cd dashboard
npm install
npm start
```

Step 3: Open your browser to `http://localhost:3000`, enter `quack:127.0.0.1:9494` and your token, and click **Connect Console**!

---

### 2. Basic Micro-Batching & DuckDB SQL Pipeline

```python
import time
import pyarrow as pa
import pyarrow.compute as pc
from rills import RillEngine, MemoryConnector, ScheduledSQLTask, RetentionPolicy, schema

# 1. Initialize Rill Engine with a 200ms micro-batch interval and 500 MB memory budget
engine = RillEngine(
    trigger_interval_ms=200, 
    memory_budget_mb=500.0,
    quack_address="quack:0.0.0.0:9494", 
    quack_token="secret_token"
)

# 2. Define schema with primary key and register table
orders_schema = schema([
    ("order_id", pa.int64()),
    ("user_tier", pa.string()),
    ("amount", pa.float64())
], primary_key="order_id", mode="snapshot")

engine.register_table("orders", schema=orders_schema)

# 3. Schedule a live dashboard query to compute business KPIs
engine.add_dashboard_scheduled_query(
    name="Max Order Amount",
    sql="SELECT MAX(amount) as max_amount FROM orders",
    interval_seconds=1.0,
    chart_type="bar",
    x_col="ts",
    y_col="max_amount"
)

# 4. Attach memory connector to 'orders' table
connector = MemoryConnector(target_table="orders")
engine.add_connector(connector)

# 5. Add a Scheduled DuckDB SQL Query running every 1 second
sql_task = ScheduledSQLTask(
    name="tier_revenue",
    query="SELECT user_tier, SUM(amount) as total_revenue, COUNT(*) as order_count FROM orders GROUP BY user_tier",
    output_table="revenue_by_tier",
    interval_seconds=1.0
)
engine.add_sql_task(sql_task)

# 6. Start engine and Quack server (`quack:0.0.0.0:9494`)
engine.start()

# Push incoming events directly to connector
connector.push({
    "order_id": [1, 2, 3],
    "user_tier": ["Gold", "Silver", "Gold"],
    "amount": [100.50, 45.00, 250.00]
})

time.sleep(1.2)

# The live data is fully isolated to prevent locking issues. 
# Telemetry, Scheduled Queries, and Alerts can be read safely via query_metrics:
alerts = engine.query_metrics("SELECT * FROM rill_alerts LIMIT 5").to_pylist()
print("Recent System Alerts:\n", alerts)

engine.stop()
```

---

### 3. Alerts & Observability

```python
from rills import RillEngine, MemoryConnector
import traceback

engine = RillEngine(
    trigger_interval_ms=200,
    quack_address="quack:0.0.0.0:9494"
)

engine.register_table("orders", primary_key="order_id")
connector = MemoryConnector(target_table="orders")
engine.add_connector(connector)

# Push custom alerts from your application code
engine.alert("Engine started successfully")           # info severity
engine.warn("Order queue backlog exceeds threshold")   # warning severity

try:
    # Your business logic here
    result = process_payment(order)
except Exception:
    engine.error("Payment processing failed", detail=traceback.format_exc())

# Alerts with full control
engine.push_alert(
    "Revenue dropped below $1,000",
    severity="warning",
    detail="Current daily revenue: $847.23. Threshold: $1,000."
)

engine.start()
# Engine exceptions (connector failures, SQL errors, memory warnings) are
# automatically recorded as alerts — no extra code needed.
# All alerts appear in the dashboard Alert Logs view.
```

Alerts are stored in the `rill_alerts` DuckDB table and can also be queried directly:

```python
results = engine.query_metrics("""
    SELECT ts, severity, source, message 
    FROM rill_alerts 
    WHERE severity = 'error' 
    ORDER BY ts DESC LIMIT 10
""").to_pylist()
```

---

## 🌟 Advanced Examples

See our complete end-to-end demos inside `examples/`:

- **`live_streaming_demo.py`**: Simulates continuous high-frequency order events, performs vectorized upserts in PyArrow, runs scheduled DuckDB SQL transformations, tracks real-time system performance, and sinks outputs.
- **`multi_stream_join_demo.py`**: Joining a live `user_profiles` table with a continuous `orders` stream into `orders_enriched`. Calculating regional revenue aggregations via DuckDB SQL every second.
- **`benchmark_streams.py`**: Evaluates performance and identifies bottlenecks in high-frequency streaming analytics across 1K, 10K, and 1M products.
- **`quack_dashboard_demo.py`**: Starts Rill with a background Quack server enabled, simulating dynamic memory allocation variations for dashboard visualization.

---

## 🧪 Running Tests

Rill is verified by a comprehensive test suite (`pytest`) covering schema enrichment, primary key extraction, table processing modes, mandatory TTL validation, memory governance warnings, backpressure slicing, zero-copy joins, alerts pipeline, bug regression tests, and connector edge cases:

```bash
pytest tests/ -v
```

Test coverage includes:
- **`test_alerts_pipeline.py`** — Alert recording, DuckDB persistence, user API (`push_alert`, `alert`, `warn`, `error`), connector error alert generation, alert retention
- **`test_bug_fixes.py`** — Import sanity, connector edge cases (inconsistent rows, empty pushes, deque buffers), O(N²) buffer fix, streaming JSON export, resource cleanup
- **`test_engine.py`** — Step execution, callbacks, start/stop lifecycle
- **`test_connectors.py`** — Memory, JSON stream, and file connectors
- **`test_backpressure.py`** — Bounded buffer overflow strategies
- **`test_duckdb_sql.py`** — Zero-copy queries, scheduled SQL tasks
- **`test_stream_joins.py`** — Multi-stream join tasks with and without aggregation
- **`test_schema.py`** — Schema enrichment, primary key extraction, row size estimation
- **`test_retention.py`** — Max rows and max age TTL pruning
- **`test_table_modes.py`** — Snapshot vs append mode behavior
- **`test_upsert.py`** — Single/composite primary key upserts, append mode
- **`test_metrics.py`** — System metrics tracking
- **`test_memory_budget.py`** — Memory budget warnings and callbacks
- **`test_optimizations.py`** — Benchmark optimizations, dictionary encoding
- **`test_quack_integration.py`** — Quack background server testing

---

## 🤝 Contributing

We welcome community contributions! Please review our [CONTRIBUTING.md](CONTRIBUTING.md) guide before opening pull requests or reporting issues.

## 📄 License

Rill is open-sourced under the [MIT License](LICENSE).
