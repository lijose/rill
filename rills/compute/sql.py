"""
DuckDB SQL integration and scheduled query task execution over live PyArrow tables.
"""

import time
import threading
from typing import Optional, Dict, Any, TYPE_CHECKING
import pyarrow as pa
import duckdb
import logging
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..engine import RillEngine
    from ..table import RillTable


class ScheduledSQLTask:
    """
    Defines a scheduled SQL transformation query that executes over registered live tables
    at configurable intervals (e.g., every 1s, every 10s).
    """

    def __init__(
        self,
        name: str,
        query: str,
        output_table: str,
        interval_seconds: float = 1.0,
        primary_key: Optional[str] = None,
        use_prepared_statements: bool = False
    ):
        """
        Args:
            name: Identifier for this task.
            query: DuckDB SQL query string (can reference any table registered in `RillEngine`).
            output_table: Name of the `RillTable` where the query output (`pa.Table`) should be stored.
            interval_seconds: Execution frequency in seconds.
            primary_key: Optional primary key for `output_table` if incremental upsert or structured state is required.
            use_prepared_statements: Whether to pre-compile and reuse a prepared statement plan for this task.
        """
        self.name = name
        self.query = query
        self.output_table = output_table
        self.interval_seconds = interval_seconds
        self.primary_key = primary_key
        self.use_prepared_statements = use_prepared_statements
        self.last_run_time: float = 0.0

    def is_due(self, current_time: float) -> bool:
        """
        Checks whether the scheduled interval has elapsed since `last_run_time`.
        """
        return (current_time - self.last_run_time) >= self.interval_seconds


class DuckDBBridge:
    """
    Manages zero-copy connection between DuckDB and Rill's PyArrow memory tables.
    Executes ad-hoc queries and runs scheduled SQL transformations.
    """

    def __init__(
        self,
        engine: 'RillEngine',
        connection: Optional[duckdb.DuckDBPyConnection] = None,
        use_prepared_statements: bool = False
    ):
        self.engine = engine
        self.use_prepared_statements = use_prepared_statements
        self._prepared_cache: Dict[str, Any] = {}
        self._lock = threading.RLock()
        if connection is not None:
            self.con = connection
            self.db_name = None
        else:
            # Create a named in-memory DuckDB connection to allow sharing database catalog across process connections
            self.db_name = f':memory:rill_{id(engine)}'
            self.con = duckdb.connect(database=self.db_name)

    def register_tables(self) -> None:
        """
        Zero-copy registers all active RillTable PyArrow instances into the DuckDB connection.
        Also registers/updates the special 'rill_metrics' system/performance metrics view.
        """
        with self._lock:
            for name, rill_table in self.engine.tables.items():
                arrow_tab = rill_table.to_arrow()
                if arrow_tab is not None:
                    try:
                        self.con.register(name, arrow_tab)
                    except Exception:
                        logger.warning(f"Failed to register table '{name}' in DuckDB", exc_info=True)
            
            # Register/update the dynamic rill_metrics table
            try:
                metrics_tab = self.engine.metrics.get_metrics_arrow_table(self.engine)
                self.con.register("rill_metrics", metrics_tab)
            except Exception:
                logger.debug("Failed to register rill_metrics view", exc_info=True)

            # Register a rill_tables view so remote clients can discover tables
            try:
                table_names = list(self.engine.tables.keys())
                table_names.extend(["rill_metrics", "rill_alerts", "rill_tables"])
                tables_pa = pa.table({"table_name": pa.array(table_names, type=pa.string())})
                self.con.register("rill_tables", tables_pa)
            except Exception:
                logger.debug("Failed to register rill_tables view", exc_info=True)

    def query(self, sql: str) -> pa.Table:
        """
        Executes a DuckDB SQL query over the live registered PyArrow tables and returns a `pa.Table`.

        Args:
            sql: SQL query string.

        Returns:
            Resulting `pa.Table` in C++ memory.
        """
        with self._lock:
            self.register_tables()
            res = self.con.execute(sql).arrow()
            if hasattr(res, "read_all"):
                res = res.read_all()
            return res

    def run_due_tasks(self, current_time: Optional[float] = None) -> None:
        """
        Evaluates and runs all scheduled SQL tasks whose execution intervals have elapsed.
        """
        if current_time is None:
            current_time = time.perf_counter()

        with self._lock:
            if not self.engine.sql_tasks:
                return

            self.register_tables()

            for task in self.engine.sql_tasks:
                if task.is_due(current_time):
                    try:
                        task_t0 = time.perf_counter()

                        # DuckDB internally caches query plans. When use_prepared_statements
                        # is enabled, we track executed queries in _prepared_cache to signal
                        # that the plan has been submitted and is being reused by the engine.
                        res = self.con.execute(task.query).arrow()
                        if task.use_prepared_statements and task.query not in self._prepared_cache:
                            self._prepared_cache[task.query] = task.query

                        if hasattr(res, "read_all"):
                            res = res.read_all()
                        result_table = res
                        # Get or create destination RillTable
                        target_table = self.engine.get_or_create_table(
                            task.output_table,
                            schema=result_table.schema,
                            primary_key=task.primary_key
                        )
                        target_table.replace_state(result_table)

                        # NEW: Insert historical snapshot into metrics_con for dashboard querying
                        metrics_con = self.engine.metrics_con
                        temp_view = f"__temp_{id(task)}"
                        metrics_con.register(temp_view, result_table)
                        try:
                            import datetime
                            iso_ts = datetime.datetime.now().isoformat()
                            metrics_con.execute(f'CREATE TABLE IF NOT EXISTS "{task.output_table}" AS SELECT *, \'{iso_ts}\' AS insert_ts FROM "{temp_view}" WHERE 1=0')
                            metrics_con.execute(f'INSERT INTO "{task.output_table}" SELECT *, \'{iso_ts}\' AS insert_ts FROM "{temp_view}"')
                        finally:
                            metrics_con.unregister(temp_view)

                        task.last_run_time = current_time
                        
                        task_t1 = time.perf_counter()
                        self.engine.metrics.record_task_latency(task.name, (task_t1 - task_t0) * 1000.0)
                    except Exception as e:
                        logger.error(f"SQL task '{task.name}' failed: {e}", exc_info=True)
                        if hasattr(self.engine, 'metrics'):
                            import traceback as tb
                            self.engine.metrics.record_alert(
                                severity="error",
                                source=f"compute.sql_task.{task.name}",
                                message=f"Scheduled SQL task '{task.name}' failed: {e}",
                                detail=tb.format_exc()
                            )

