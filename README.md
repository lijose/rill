# Rill Streaming Engine (`rill`)

[![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![PyArrow](https://img.shields.io/badge/Powered%20by-PyArrow-orange.svg)](https://arrow.apache.org/)
[![DuckDB](https://img.shields.io/badge/SQL%20Engine-DuckDB-yellow.svg)](https://duckdb.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI Status](https://github.com/lijose/rill/actions/workflows/tests.yml/badge.svg)](.github/workflows/tests.yml)

**Rill** is a lightweight, single-node micro-batching engine designed for high-performance streaming analytics using pure **PyArrow** and zero-copy **DuckDB**.

By bypassing traditional Python data structures and the Global Interpreter Lock (GIL) where possible, Rill ingests continuous event streams directly into C++ contiguous memory (`pyarrow.Table`). It leverages a scheduled processing trigger to buffer, join, aggregate, and query incoming data efficiently without the overhead of event-by-event Python loops.

From raw data ingestion to complex multi-stream joins, aggregations, and scheduled SQL pipelines, every operation is strictly contained within C++ vectorized memory, making Rill the ideal zero-infrastructure solution for live data processing.

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
│  │ • Zero-Copy DuckDB Scheduled SQL Pipelines                                  │ │
│  │ • Multi-Stream Relational Joins & Aggregations                              │ │
│  │ • Vectorized TTL Pruning (Age / Row Count)                                  │ │
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
Use `rill.schema([fields], primary_key="user_id", mode="append")` to embed governance properties directly inside PyArrow schema metadata (`schema.metadata[b"primary_key"]`). When a `RillTable` is initialized with an enriched schema, it automatically extracts its primary key and operating mode without manual boilerplate.

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

### 9. Dynamic Business Metrics & KPIs (`register_business_metric`)
Register custom PyArrow analytical formulas (`engine.register_business_metric(name, formula_fn)`) that evaluate in real-time on every micro-batch tick. These metrics automatically calculate domain KPIs—such as maximum order tickets, cumulative revenue, and cancellation ratios—without writing ad-hoc polling loops.

---

## 📊 Live Business Intelligence & Quack Dashboard

Rill includes a state-of-the-art, dark-themed **Live Business Intelligence & System Monitoring Dashboard** built with Node.js, WebSocket Quack Bridge (`quack_worker.py`), and Chart.js.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        NODE.JS / BROWSER DASHBOARD                              │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │ 💎 Key Business Intelligence & Formulas (Live PyArrow Evaluated)          │  │
│  │  [ max_order_amount ($): $497.34 ]  [ total_revenue ($): $13,025.21 ]     │  │
│  │  [ cancelled_ratio: 26.5% ]                                               │  │
│  │  📈 Business Trend Line Plot          📊 Business Distribution Histogram  │  │
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
- **💎 Business Intelligence Centerpiece**: Prominently displays custom KPIs (`max_order_amount`, `total_revenue`, `cancelled_ratio`) right at the top.
- **Interactive Business Charts**: Includes a real-time **Line Chart** plotting numerical KPI trends over time, alongside a **Histogram / Bar Chart** comparing snapshot distributions across formulas.
- **⚙️ Separated System Infrastructure**: Cleanly separates host CPU, System Memory, PyArrow C++ pool allocations, throughput (RPS), and step latency into a dedicated secondary monitoring section.
- **Multi-Tab Connections**: Connect to and switch between multiple remote or local Rill/Quack instances (`quack:ip:port`) inside a single UI.
- **Dynamic Time Range Control**: Filter live charts across flexible historical intervals (`Last 30s`, `Last 60s`, `Last 90s`, `Last 180s`, `Last 300s`, or `All stored`).

---

## 🚀 Quickstart Guide

### Installation

Install Rill locally or in editable mode with development & connector dependencies:

```bash
git clone https://github.com/lijose/rill.git
cd rill
pip install -e .[dev,connectors]
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
from rill import RillEngine, MemoryConnector, ScheduledSQLTask, RetentionPolicy, schema

# 1. Initialize Rill Engine with a 200ms micro-batch interval and 500 MB memory budget
engine = RillEngine(trigger_interval_ms=200, memory_budget_mb=500.0)

# 2. Define schema with primary key and register table
orders_schema = schema([
    ("order_id", pa.int64()),
    ("user_tier", pa.string()),
    ("amount", pa.float64())
], primary_key="order_id", mode="snapshot")

engine.register_table("orders", schema=orders_schema)

# 3. Register custom live PyArrow Business Metrics evaluated on every tick
def calc_max_order(engine):
    table = engine.get_table("orders").to_arrow()
    if table.num_rows == 0:
        return 0.0
    return pc.max(table.column("amount")).as_py()

engine.register_business_metric("max_order_amount ($)", calc_max_order)

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
engine.start(quack_port=9494, quack_token="secret_token")

# Push incoming events directly to connector
batch = pa.RecordBatch.from_pydict({
    "order_id": [1, 2, 3],
    "user_tier": ["Gold", "Silver", "Gold"],
    "amount": [100.50, 45.00, 250.00]
}, schema=orders_schema)
connector.push(batch)

time.sleep(1.2)

# Retrieve finalized zero-copy C++ PyArrow tables and business metrics
revenue_summary = engine.get_table("revenue_by_tier").to_arrow()
print("Live Revenue Summary:\n", revenue_summary.to_pandas())
print("Live Business Metrics:", engine.evaluate_business_metrics())

engine.stop()
```

---

## 🌟 Advanced Example: Multi-Stream Joins

See our complete end-to-end multi-stream demo inside `examples/`:

```bash
python3 examples/multi_stream_join_demo.py
```

This demo illustrates:
- Joining a live `user_profiles` table with a continuous `orders` stream into `orders_enriched`.
- Calculating regional revenue aggregations via DuckDB SQL every second.

---

## 🧪 Running Tests

Rill is verified by a thorough unit and integration test suite (`pytest`) covering schema enrichment, primary key extraction, table processing modes, mandatory TTL validation, memory governance warnings, backpressure slicing, and zero-copy joins:

```bash
pytest tests/ -v
```

---

## 🤝 Contributing

We welcome community contributions! Please review our [CONTRIBUTING.md](CONTRIBUTING.md) guide and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before opening pull requests or reporting issues.

## 📄 License

Rill is open-sourced under the [MIT License](LICENSE).
