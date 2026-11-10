# Alerts, storage, and benchmark review

These notes describe Rill 0.1.2. Proposed safeguards below are **not implemented**
and require approval before changing requirements.

## Alert cooldowns

| Alert source | Cooldown | Persistence |
| --- | --- | --- |
| Browser threshold rule | Configurable per rule and connection | Rule settings and local event log in browser localStorage |
| `engine.alert/warn/error/push_alert` | None | Buffered, then persisted to `rill_alerts` |
| Automatically recorded engine errors/warnings | None | Buffered, then persisted to `rill_alerts` |

Default CPU/memory rules use 15 seconds. The new-rule form starts at 10 seconds;
zero disables suppression. Timers last for the current page session, survive
socket reconnects on that page, and reset on page reload. Different browsers have
independent timers. A continuously violated threshold may trigger again once its
cooldown expires; this is not a recovery/incident state machine.

Repeated engine errors can generate a record every step. The pending alert queue
has no count/byte bound and retains records during database write failures.
The worker polls up to 50 engine alerts per iteration, so high alert volume can
also create a display backlog. A one-hour retention period is not a rate limit.

Known UI limitation: engine error events invoke sound without checking the
sound-enabled preference. Browser threshold rules do check that preference.
This review documents the issue without changing runtime behavior.

A possible follow-up is optional engine-side duplicate suppression keyed by
source/severity/message, with first-event delivery and a suppressed-count summary.
Any such policy must define which events may be suppressed and how unique events
and database failures are handled before implementation.

## What the memory readings mean

`duckdb_live_bytes` and `duckdb_metrics_bytes` come from
`SUM(memory_usage_bytes)` in `duckdb_memory()` on their respective connections.
They report tracked DuckDB memory allocation, not database file sizes, and do not
represent total process RSS. Unavailable measurements are null.

`pyarrow_allocated_bytes` reports Arrow pool allocation. `memory_budget_mb` and
`memory_budget_bytes` warn when this allocation crosses the configured threshold;
they do not enforce a hard process-memory or disk quota.

## Metrics database growth

The default database is `rill_metrics.db` in the process working directory.
`metrics_db_path` overrides it; `:memory:` avoids a persistent file.

| Stored data | Default retention | Constructor option |
| --- | --- | --- |
| Telemetry and task-latency rows | 600 seconds | `metrics_ttl_seconds` |
| Managed scheduled-output snapshots | 600 seconds | `query_history_ttl_seconds` |
| Engine/user alerts | 3,600 seconds | `alert_ttl_seconds` |

Retention runs during steps, including quiet steps. SQL-output pruning applies
to tables enrolled by tasks in the current engine. Unrelated persisted tables,
and output tables no longer enrolled after a restart, are not automatically
purged. There is no hard row-count or byte cap on these persisted histories.

Telemetry writes roughly `12 + connector count + task count` rows per step,
plus a separate latency row per task. At ten steps/second and a 600-second
window, the 12 base metrics alone account for about 72,000 retained rows. This
is a row-count estimate, not a disk-byte estimate. Full SQL-output snapshots and
bursts of alerts can dominate storage even within the retention window.

Rill does **not** currently collect `.db` size, WAL size, temporary spill usage,
or free filesystem space. For a read-only database-level inspection in Python:

```python
sizes = engine.query_metrics("""
    SELECT database_name,
           block_size * total_blocks AS allocated_block_bytes,
           block_size * used_blocks AS used_block_bytes,
           block_size * free_blocks AS reusable_block_bytes,
           wal_size
    FROM pragma_database_size()
""").to_pylist()
```

The block counts describe database storage allocation; `wal_size` is a formatted
string. Exact filesystem byte monitoring should separately inspect the database
file and its `.wal` file, tolerate file creation/removal races, and track the
configured spill directory and filesystem free space. See DuckDB's
[database-size documentation](https://www.duckdb.org/docs/2.0/configuration/pragmas).

Deleting expired rows does not guarantee immediate file shrinkage. Checkpointing
partially reclaims deleted storage; `VACUUM` does not reclaim disk space in
DuckDB. Plan maintenance independently from retention, considering active
transactions and readers. See [CHECKPOINT](https://www.duckdb.org/docs/current/sql/statements/checkpoint)
and [reclaiming space](https://www.duckdb.org/docs/2.0/operations_manual/footprint_of_duckdb/reclaiming_space).

For a long-running deployment, a configurable storage budget is useful as a
safeguard, but TTL alone is not a byte budget. Recommended follow-up, pending
approval: track database/WAL bytes and free space; warn near a configured budget;
define an explicit over-budget policy for telemetry/history writes and the alert
queue. A hard filesystem quota by itself can cause writes to fail while the
pending alert queue continues growing. Do not silently discard alerts or delete
unrelated tables to meet a cap. No storage cap or new deletion policy was added
by this documentation update.

## Benchmark status

`examples/benchmark_streams.py` is an exploratory append/aggregation benchmark.
A smoke check on DuckDB 2.0 / Arrow 26 used 1,000 impressions, 1,000 possible
product IDs, and five batches, in a separate temporary working directory for
each run. Default and dictionary modes both exited successfully with no stderr;
each processed 1,150 events including clicks. This verifies execution only,
not comparative performance, sustained throughput, or correctness at scale.

The current benchmark needs the following checks before its output can support
performance claims:

1. **Upserts and timestamp correctness:** current tables use append mode. Add
   single/composite-key updates, duplicates, shuffled keys, and assertions that
   timestamps remain associated with keys. Measure the cost of per-batch DuckDB
   deduplication and Arrow timestamp lookup/sorting introduced by the fixes.
2. **TTL eviction:** generated events are within five minutes, while TTL is ten
   minutes. The current TTL timing mostly measures filtering with no expiry.
   Include mixed expired/fresh rows and verify survivor counts and timestamps.
3. **Fair comparisons:** seed data generation; reuse equivalent inputs across
   supported dependency profiles; add warmups and repeated runs with median and
   tail latency. Record Python/dependency versions, CPU, threads, and workload.
4. **Timing scope:** reported ingestion includes list-to-Arrow conversion,
   engine steps, retention, and telemetry/database writes. Report these costs
   separately when identifying a bottleneck. The data generator turns NumPy
   arrays into Python lists, so Arrow allocation is not total workload memory.
5. **Prepared statements:** the command-line flag defaults true and has no
   disable option; the benchmark sets a bridge flag but not the task flags.
   The existing task bookkeeping stores query strings, not explicit prepared
   statement handles. Do not interpret this as a prepared-versus-unprepared test.
6. **Isolation and cleanup:** the script uses the default metrics database in
   its working directory and does not call `engine.stop()` before returning.
   Multiple scenarios can share disk history and retained engine resources.
   Use isolated database paths and deterministic cleanup for meaningful results.
7. **Correctness and sustained load:** check aggregation output against known
   counts, exercise actual joins (the current script does not join the streams),
   and measure process RSS, database/WAL growth, burst ingestion, and alert storms
   over multiple retention windows. Product pool size is not guaranteed observed
   distinct cardinality; integer batch division can omit remainder impressions.

Until those changes are approved and implemented, run a smoke check from a fresh
working directory to avoid touching an existing metrics database:

```sh
# From the repository root, with the development environment installed:
repo_dir="$PWD"
bench_dir="$(mktemp -d)"
(
    cd "$bench_dir"
    "$repo_dir/.venv/bin/python" "$repo_dir/examples/benchmark_streams.py" \
        --products 1000 --impressions 1000
)
# The temporary directory contains only this run's benchmark database.
```

No benchmark implementation or performance threshold was changed in this review.

## Portable dashboard paths

The dashboard has no hard-coded `/home/lijo/...` interpreter or UI asset path.
`server.js` resolves static assets and the worker relative to its own `__dirname`.
The worker locates the repository package relative to `__file__`. Python is chosen
from `RILL_PYTHON`, `VIRTUAL_ENV`, `CONDA_PREFIX`, or `python3` on PATH. Configured
environment values may be absolute paths, but the application does not bake in a
particular developer's home directory.
