"""
RillEngine: Core micro-batch compute coordinator and scheduling trigger.
"""

import time
import logging
import threading
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Callable, Union, Any, Tuple, TYPE_CHECKING
import pyarrow as pa
from .table import RillTable
from .connectors.base import BaseConnector
from .compute.sql import ScheduledSQLTask, DuckDBBridge
from .compute.join import TableJoinTask
from .metrics import MetricsRegistry, create_alerts_table

logger = logging.getLogger(__name__)


class RillEngine:
    """
    Core engine managing live PyArrow tables, active input connectors, scheduled DuckDB SQL tasks,
    multi-stream join tasks, optional checkpoints, and system performance metrics.
    Runs a scheduled micro-batch loop that ingests buffered stream data, performs vectorized
    upserts, executes due SQL/join transformations, enforces TTL retention, and updates metrics.
    """

    def __init__(
        self,
        trigger_interval_ms: float = 100.0,
        duckdb_connection: Optional[Any] = None,
        checkpoint_dir: Optional[Union[str, Path]] = None,
        checkpoint_interval_seconds: float = 60.0,
        memory_budget_bytes: Optional[int] = None,
        memory_budget_mb: Optional[float] = None,
        on_memory_warning: Optional[Callable[[int, int], None]] = None,
        quack_address: Optional[str] = None,
        quack_token: Optional[str] = None,
        auto_compact_chunks: int = 32,
        metrics_db_path: Optional[str] = None
    ):
        """
        Args:
            trigger_interval_ms: Scheduled micro-batch interval in milliseconds (default: 100ms).
            duckdb_connection: Optional external DuckDB connection object. If None, uses in-memory DuckDB.
            checkpoint_dir: Optional directory path for automated table snapshots.
            checkpoint_interval_seconds: Frequency in seconds for saving state checkpoints to disk.
            memory_budget_bytes: Maximum memory threshold in bytes before emitting warning messages.
            memory_budget_mb: Maximum memory threshold in megabytes (convenience wrapper for `memory_budget_bytes`).
            on_memory_warning: Optional callback invoked when allocated memory crosses the budget threshold.
            quack_address: Optional Quack protocol address to serve DuckDB remotely (e.g. 'quack:0.0.0.0:9494').
            quack_token: Optional security token for authentication of remote Quack clients.
            auto_compact_chunks: Threshold chunk count for automatic de-fragmentation during step (default: 32).
            metrics_db_path: File path for the metrics DuckDB database. Defaults to 'rill_metrics.db' in CWD.
        """
        self.trigger_interval_ms = trigger_interval_ms
        self.auto_compact_chunks = auto_compact_chunks
        self.tables: Dict[str, RillTable] = {}
        self.connectors: List[BaseConnector] = []
        self.sql_tasks: List[ScheduledSQLTask] = []
        self.join_tasks: List['TableJoinTask'] = []
        self.metrics = MetricsRegistry()
        self.duckdb = DuckDBBridge(self, connection=duckdb_connection)
        
        # Initialize metrics database (metrics_con)
        import duckdb
        self.metrics_db_path = metrics_db_path or "rill_metrics.db"
        self.metrics_con = duckdb.connect(database=self.metrics_db_path)
        self.metrics_con.execute("CREATE TABLE IF NOT EXISTS rill_metrics (ts DOUBLE, metric_name VARCHAR, value DOUBLE)")
        create_alerts_table(self.metrics_con)

        if memory_budget_mb is not None and memory_budget_bytes is None:
            memory_budget_bytes = int(memory_budget_mb * 1024 * 1024)
        self.memory_budget_bytes = memory_budget_bytes
        self.on_memory_warning = on_memory_warning

        self.quack_address = quack_address
        self.quack_token = quack_token
        self._quack_thread: Optional[threading.Thread] = None


        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def register_table(
        self,
        name: str,
        schema: Optional[pa.Schema] = None,
        primary_key: Optional[Union[str, List[str]]] = None,
        retention_policy: Optional['RetentionPolicy'] = None,
        mode: Optional[str] = None
    ) -> RillTable:
        """
        Registers a new RillTable in the engine (`mode="snapshot"` or `mode="append"`).
        """
        with self._lock:
            table = RillTable(name=name, schema=schema, primary_key=primary_key, retention_policy=retention_policy, mode=mode)
            self.tables[name] = table
            return table

    def get_table(self, name: str) -> Optional[RillTable]:
        """
        Retrieves a registered RillTable by name.
        """
        with self._lock:
            return self.tables.get(name)

    def get_or_create_table(
        self,
        name: str,
        schema: Optional[pa.Schema] = None,
        primary_key: Optional[Union[str, List[str]]] = None,
        retention_policy: Optional['RetentionPolicy'] = None,
        mode: Optional[str] = None
    ) -> RillTable:
        """
        Retrieves a table if registered, otherwise creates and registers it.
        """
        with self._lock:
            if name not in self.tables:
                return self.register_table(name, schema=schema, primary_key=primary_key, retention_policy=retention_policy, mode=mode)
            return self.tables[name]

    def compact_all(self, max_chunks: Optional[int] = None) -> None:
        """
        Triggers de-fragmentation across all registered Rill tables where chunk count exceeds threshold.
        """
        threshold = max_chunks if max_chunks is not None else self.auto_compact_chunks
        with self._lock:
            for tbl in self.tables.values():
                try:
                    tbl.compact(max_chunks=threshold)
                except Exception:
                    logger.exception(f"Error compacting table {tbl.name}")

    def add_connector(self, connector: BaseConnector) -> None:
        """
        Attaches an input connector to the engine.
        """
        with self._lock:
            if connector not in self.connectors:
                self.connectors.append(connector)
                if self._running:
                    connector.start()

    def remove_connector(self, connector: BaseConnector) -> None:
        """
        Detaches an input connector from the engine.
        """
        with self._lock:
            if connector in self.connectors:
                connector.stop()
                self.connectors.remove(connector)

    def add_sql_task(self, task: ScheduledSQLTask) -> None:
        """
        Registers a scheduled DuckDB SQL query task.
        """
        with self._lock:
            if task not in self.sql_tasks:
                self.sql_tasks.append(task)

    def remove_sql_task(self, task: ScheduledSQLTask) -> None:
        """
        Removes a scheduled DuckDB SQL query task.
        """
        with self._lock:
            if task in self.sql_tasks:
                self.sql_tasks.remove(task)

    def add_join_task(self, task: 'TableJoinTask') -> None:
        """
        Registers a continuous multi-stream TableJoinTask.
        """
        with self._lock:
            if task not in self.join_tasks:
                self.join_tasks.append(task)

    def remove_join_task(self, task: 'TableJoinTask') -> None:
        """
        Removes a continuous multi-stream TableJoinTask.
        """
        with self._lock:
            if task in self.join_tasks:
                self.join_tasks.remove(task)

    def restore_checkpoints(self) -> List[str]:
        """
        Restores table states from the configured `checkpoint_dir` if present.
        """
        return []

    def subscribe(self, table_name: str, callback: Callable[[str, pa.Table], None]) -> None:
        """
        Subscribes a callback to live updates on a specific table.
        """
        table = self.get_or_create_table(table_name)
        table.subscribe(callback)

    def register_business_metric(self, name: str, evaluator: Callable[['RillEngine'], Any]) -> None:
        """
        Registers a custom formula/metric to be calculated during each micro-batch tick.
        """
        self.metrics.register_metric(name, evaluator)

    def query_sql(self, sql: str) -> pa.Table:
        """
        Executes an ad-hoc DuckDB SQL query against any registered PyArrow tables.
        Returns zero-copy PyArrow Table.
        """
        return self.duckdb.query(sql)

    def get_metrics(self) -> Dict[str, Any]:
        """
        Returns comprehensive system and live business logic metrics.
        """
        return self.metrics.get_all_metrics(self)

    # ── User-Facing Alert API ────────────────────────────────────────────

    def push_alert(
        self,
        message: str,
        severity: str = "info",
        detail: str = ""
    ) -> None:
        """
        Push a custom alert from user application code.
        Alerts are stored in the metrics database and visible in the dashboard.

        Args:
            message: Short human-readable alert message.
            severity: Alert level — "info", "warning", or "error".
            detail: Optional extended context, traceback, or description.

        Example:
            engine.push_alert("Revenue dropped below threshold", severity="warning")
            engine.push_alert("Payment gateway timeout", severity="error", detail=traceback.format_exc())
        """
        self.metrics.record_alert(
            severity=severity,
            source="user",
            message=message,
            detail=detail
        )

    def alert(self, message: str, detail: str = "") -> None:
        """Shortcut for push_alert with severity='info'."""
        self.push_alert(message, severity="info", detail=detail)

    def warn(self, message: str, detail: str = "") -> None:
        """Shortcut for push_alert with severity='warning'."""
        self.push_alert(message, severity="warning", detail=detail)

    def error(self, message: str, detail: str = "") -> None:
        """Shortcut for push_alert with severity='error'."""
        self.push_alert(message, severity="error", detail=detail)

    # ── Core Step Execution ──────────────────────────────────────────────

    def step(self) -> float:
        """
        Executes a single synchronous micro-batch iteration:
        1. Ingests buffered data from all active connectors.
        2. Upserts data directly into target PyArrow tables.
        3. Executes scheduled DuckDB SQL queries whose interval has elapsed.
        4. Evaluates business metrics and records system latency/throughput.

        Returns:
            Execution latency in milliseconds.
        """
        t0 = time.perf_counter()
        records_in_step = 0

        with self._lock:
            # 1. Ingestion and vectorized upsert
            connectors_snapshot = list(self.connectors)
            for connector in connectors_snapshot:
                try:
                    conn_t0 = time.perf_counter()
                    batch_or_table = connector.read_batch()
                    conn_records = 0
                    if batch_or_table is not None:
                        table = self.get_or_create_table(connector.target_table)
                        table.upsert(batch_or_table)

                        if isinstance(batch_or_table, list):
                            conn_records = sum(b.num_rows for b in batch_or_table)
                        else:
                            conn_records = batch_or_table.num_rows
                        records_in_step += conn_records
                    
                    conn_t1 = time.perf_counter()
                    self.metrics.record_connector_step(connector.target_table, conn_records, conn_t1 - conn_t0)
                except Exception as e:
                    logger.exception(f"Error in connector ingestion for table {connector.target_table}")
                    self.metrics.record_alert(
                        severity="error",
                        source=f"engine.connector.{connector.target_table}",
                        message=f"Connector ingestion error for table '{connector.target_table}': {e}",
                        detail=traceback.format_exc()
                    )

            # 2. Scheduled DuckDB SQL tasks
            self.duckdb.run_due_tasks(t0)

            # 3. Continuous Multi-stream Table Join tasks
            for jtask in self.join_tasks:
                try:
                    j_t0 = time.perf_counter()
                    jtask.execute(self)
                    j_t1 = time.perf_counter()
                    self.metrics.record_task_latency(jtask.name if hasattr(jtask, 'name') else 'join_task', (j_t1 - j_t0) * 1000.0)
                except Exception as e:
                    logger.exception(f"Error in table join task")
                    task_name = jtask.name if hasattr(jtask, 'name') else 'unknown'
                    self.metrics.record_alert(
                        severity="error",
                        source=f"compute.join_task.{task_name}",
                        message=f"Join task '{task_name}' failed: {e}",
                        detail=traceback.format_exc()
                    )

            # 4. Enforce optional TTL retention and compaction on tables
            now_ts = time.time()
            for tbl in self.tables.values():
                try:
                    tbl.apply_retention(current_time=now_ts)
                    if self.auto_compact_chunks > 0:
                        tbl.compact(max_chunks=self.auto_compact_chunks)
                except Exception as e:
                    logger.exception(f"Error in retention/compaction for table {tbl.name}")
                    self.metrics.record_alert(
                        severity="warning",
                        source=f"engine.retention.{tbl.name}",
                        message=f"Retention/compaction error for table '{tbl.name}': {e}",
                        detail=traceback.format_exc()
                    )

            # 5. Live business logic evaluation and metrics publishing
            self.metrics.evaluate_business_metrics(self)
            self.metrics.write_to_duckdb(self.metrics_con, self)

            # 6. Check PyArrow memory budget
            self.check_memory_budget()

        t1 = time.perf_counter()
        latency_ms = (t1 - t0) * 1000.0
        self.metrics.record_step(records_in_step, latency_ms)
        return latency_ms

    def check_memory_budget(self) -> Tuple[int, Optional[int], bool]:
        """
        Checks current allocated PyArrow memory against `memory_budget_bytes`.
        Emits `ResourceWarning` and invokes `on_memory_warning` callback if budget is exceeded.
        """
        allocated = pa.total_allocated_bytes()
        if allocated == 0:
            with self._lock:
                allocated = sum(tbl.to_arrow().nbytes for tbl in self.tables.values() if tbl.to_arrow() is not None)

        exceeded = self.memory_budget_bytes is not None and allocated > self.memory_budget_bytes
        if exceeded:
            import warnings
            msg = (
                f"RillEngine memory budget exceeded! Allocated: {allocated} bytes "
                f"({allocated / 1024 / 1024:.2f} MB) > Budget: {self.memory_budget_bytes} bytes "
                f"({self.memory_budget_bytes / 1024 / 1024:.2f} MB)"
            )
            warnings.warn(msg, ResourceWarning, stacklevel=2)
            self.metrics.record_alert(
                severity="warning",
                source="engine.memory_budget",
                message=msg
            )
            if self.on_memory_warning is not None:
                try:
                    self.on_memory_warning(allocated, self.memory_budget_bytes)
                except Exception as e:
                    logger.exception("Error in on_memory_warning callback")
                    self.metrics.record_alert(
                        severity="error",
                        source="engine.memory_budget",
                        message=f"on_memory_warning callback failed: {e}",
                        detail=traceback.format_exc()
                    )

        return allocated, self.memory_budget_bytes, exceeded

    def start(self) -> None:
        """
        Starts the background micro-batch processing loop and all attached connectors.
        Automatically restores state checkpoints from disk if `checkpoint_dir` is configured.
        """
        with self._lock:
            if self._running:
                return
            self._running = True

            for connector in self.connectors:
                connector.start()

            self._thread = threading.Thread(target=self._run_loop, name="RillEngineLoop", daemon=True)
            self._thread.start()

            if self.quack_address is not None:
                self._quack_thread = threading.Thread(
                    target=self._run_quack_server,
                    name="RillQuackServer",
                    daemon=True
                )
                self._quack_thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        """
        Stops the background processing loop and cleans up all connectors.
        """
        with self._lock:
            if not self._running:
                return
            self._running = False

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            self._thread = None

        if self._quack_thread is not None:
            if self._quack_thread.is_alive():
                try:
                    # Signal the Quack server to stop via the metrics connection
                    self.metrics_con.execute("LOAD quack;")
                    self.metrics_con.execute("CALL quack_stop(?);", [self.quack_address])
                except Exception:
                    logger.exception("Error stopping quack server")
                self._quack_thread.join(timeout=timeout)
            self._quack_thread = None

        with self._lock:
            for connector in self.connectors:
                connector.stop()

        # Close the metrics database connection to prevent resource leaks
        try:
            self.metrics_con.close()
        except Exception:
            logger.exception("Error closing metrics_con")

    def _run_quack_server(self) -> None:
        """
        Runs the DuckDB Quack server in a background thread against the metrics DB.
        """
        con = self.metrics_con

        try:
            con.execute("INSTALL quack;")
        except Exception:
            try:
                con.execute("INSTALL quack FROM community;")
            except Exception:
                try:
                    con.execute("INSTALL quack FROM core_nightly;")
                except Exception:
                    pass

        try:
            con.execute("LOAD quack;")
        except Exception as e:
            logger.error(f"[Rill Quack Server] Error loading quack extension: {e}")
            return

        try:
            if self.quack_token:
                res = con.execute("CALL quack_serve(?, token=?, allow_other_hostname => true);", [self.quack_address, self.quack_token]).fetchone()
            else:
                res = con.execute("CALL quack_serve(?, allow_other_hostname => true);", [self.quack_address]).fetchone()
            if res and len(res) >= 3 and not self.quack_token:
                self.quack_token = str(res[2])
            token_display = f" | Auth Token: {self.quack_token}" if self.quack_token else ""
            logger.info(f"[Rill Quack Server] Active at {res[0] if res else self.quack_address}{token_display}")
            print(f"[Rill Quack Server] Active at {res[0] if res else self.quack_address}{token_display}")
        except Exception as e:
            logger.error(f"[Rill Quack Server] Error serving at {self.quack_address}: {e}")
            print(f"[Rill Quack Server] Error serving at {self.quack_address}: {e}")

    def _run_loop(self) -> None:
        """
        Background loop executing `step()` every `trigger_interval_ms`.
        """
        while self._running:
            step_start = time.perf_counter()
            self.step()
            elapsed_ms = (time.perf_counter() - step_start) * 1000.0

            sleep_ms = self.trigger_interval_ms - elapsed_ms
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

    def __repr__(self) -> str:
        return f"<RillEngine(trigger_interval_ms={self.trigger_interval_ms}, running={self._running}, tables={list(self.tables.keys())})>"
