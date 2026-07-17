"""
DuckDB SQL integration and scheduled query task execution over live PyArrow tables.
"""

import time
import threading
from typing import Optional, Dict, Any, TYPE_CHECKING
import pyarrow as pa
import duckdb

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
        primary_key: Optional[str] = None
    ):
        """
        Args:
            name: Identifier for this task.
            query: DuckDB SQL query string (can reference any table registered in `RillEngine`).
            output_table: Name of the `RillTable` where the query output (`pa.Table`) should be stored.
            interval_seconds: Execution frequency in seconds.
            primary_key: Optional primary key for `output_table` if incremental upsert or structured state is required.
        """
        self.name = name
        self.query = query
        self.output_table = output_table
        self.interval_seconds = interval_seconds
        self.primary_key = primary_key
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

    def __init__(self, engine: 'RillEngine', connection: Optional[duckdb.DuckDBPyConnection] = None):
        self.engine = engine
        self._lock = threading.RLock()
        if connection is not None:
            self.con = connection
        else:
            # Create an in-memory DuckDB connection
            self.con = duckdb.connect(database=':memory:')

    def register_tables(self) -> None:
        """
        Zero-copy registers all active RillTable PyArrow instances into the DuckDB connection.
        """
        with self._lock:
            for name, rill_table in self.engine.tables.items():
                arrow_tab = rill_table.to_arrow()
                if arrow_tab is not None:
                    # con.register creates a view directly over PyArrow C++ data without data duplication
                    try:
                        self.con.register(name, arrow_tab)
                    except Exception as e:
                        # Fallback or re-register if duckdb connection reset
                        try:
                            self.con.unregister(name)
                            self.con.register(name, arrow_tab)
                        except Exception:
                            pass

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
            return self.con.execute(sql).arrow()

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
                        result_table = self.con.execute(task.query).arrow()
                        # Get or create destination RillTable
                        target_table = self.engine.get_or_create_table(
                            task.output_table,
                            schema=result_table.schema,
                            primary_key=task.primary_key
                        )
                        target_table.replace_state(result_table)
                        task.last_run_time = current_time
                    except Exception as e:
                        # Log error without stopping engine loop
                        pass
