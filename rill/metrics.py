"""
System performance tracking, live business logic evaluation metrics,
and structured alert pipeline for Rill Engine.
"""

import time
import logging
import threading
import traceback
from typing import Dict, Any, Callable, Optional, List, Tuple, TYPE_CHECKING
import json
import pyarrow as pa

logger = logging.getLogger(__name__)

try:
    import psutil
except ImportError:
    psutil = None

if TYPE_CHECKING:
    from .engine import RillEngine


if psutil is not None:
    try:
        psutil.cpu_percent()
    except Exception:
        pass


# ── System Metric Helpers (with throttling + delta-based CPU) ────────────────

_cpu_cache: Dict[str, Any] = {
    "value": 0.0,
    "last_poll": 0.0,
    # For /proc/stat delta calculation
    "prev_total": 0.0,
    "prev_idle": 0.0,
}

_mem_cache: Dict[str, Any] = {
    "total": 0,
    "used": 0,
    "last_poll": 0.0,
}

_SYSTEM_POLL_INTERVAL = 1.0  # Max 1 poll per second


def get_system_cpu_usage() -> float:
    now = time.monotonic()
    if now - _cpu_cache["last_poll"] < _SYSTEM_POLL_INTERVAL:
        return _cpu_cache["value"]

    _cpu_cache["last_poll"] = now

    if psutil is not None:
        try:
            val = psutil.cpu_percent()
            if val > 0:
                _cpu_cache["value"] = val
                return val
        except Exception:
            pass

    # Fallback using /proc/stat with delta-based calculation
    try:
        with open('/proc/stat', 'r') as f:
            for line in f:
                if line.startswith('cpu '):
                    parts = [float(x) for x in line.split()[1:]]
                    idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
                    total = sum(parts)

                    prev_total = _cpu_cache["prev_total"]
                    prev_idle = _cpu_cache["prev_idle"]
                    _cpu_cache["prev_total"] = total
                    _cpu_cache["prev_idle"] = idle

                    # First call: no delta available yet
                    if prev_total == 0.0:
                        _cpu_cache["value"] = 0.0
                        return 0.0

                    delta_total = total - prev_total
                    delta_idle = idle - prev_idle
                    if delta_total > 0:
                        usage = round(100.0 * (1.0 - delta_idle / delta_total), 1)
                        _cpu_cache["value"] = usage
                        return usage
                    break
    except Exception:
        pass

    return _cpu_cache["value"]


def get_system_memory() -> tuple[int, int]:
    now = time.monotonic()
    if now - _mem_cache["last_poll"] < _SYSTEM_POLL_INTERVAL:
        return _mem_cache["total"], _mem_cache["used"]

    _mem_cache["last_poll"] = now

    if psutil is not None:
        try:
            mem = psutil.virtual_memory()
            if mem.total > 0:
                _mem_cache["total"] = mem.total
                _mem_cache["used"] = mem.used
                return mem.total, mem.used
        except Exception:
            pass

    # Fallback using /proc/meminfo
    try:
        info = {}
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                parts = line.split(':')
                if len(parts) == 2:
                    info[parts[0].strip()] = int(parts[1].split()[0]) * 1024
        total = info.get('MemTotal', 0)
        available = info.get('MemAvailable', info.get('MemFree', 0))
        used = total - available if total > available else 0
        _mem_cache["total"] = total
        _mem_cache["used"] = used
        return total, used
    except Exception:
        pass

    return _mem_cache["total"], _mem_cache["used"]


# ── Alert Entry ──────────────────────────────────────────────────────────────

class AlertEntry:
    """Represents a single alert record to be flushed to DuckDB."""
    __slots__ = ('ts', 'severity', 'source', 'message', 'detail')

    def __init__(self, severity: str, source: str, message: str, detail: str = ""):
        self.ts = time.time()
        self.severity = severity
        self.source = source
        self.message = message
        self.detail = detail

    def as_tuple(self) -> Tuple:
        return (self.ts, self.severity, self.source, self.message, self.detail)


def create_alerts_table(con) -> None:
    """Creates the rill_alerts table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS rill_alerts (
            ts DOUBLE,
            severity VARCHAR,
            source VARCHAR,
            message VARCHAR,
            detail VARCHAR
        )
    """)


# ── MetricsRegistry ─────────────────────────────────────────────────────────

class MetricsRegistry:
    """
    Tracks real-time system performance metrics and evaluates user-defined live business logic
    during each scheduled micro-batch trigger interval.
    Also manages a structured alert pipeline for engine and user alerts.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.total_records_processed: int = 0
        self.last_batch_records: int = 0
        self.last_batch_latency_ms: float = 0.0
        self.avg_batch_latency_ms: float = 0.0
        self.records_per_second: float = 0.0
        self._batch_count: int = 0
        self._last_tick_time: float = time.perf_counter()

        # Business logic metric evaluators: name -> callable(engine) -> value
        self._business_evaluators: Dict[str, Callable[['RillEngine'], Any]] = {}
        # Cached latest business metric results
        self.business_metrics: Dict[str, Any] = {}

        # Alert buffer (flushed to DuckDB during write_to_duckdb)
        self._alert_buffer: List[AlertEntry] = []

    # ── Alert Pipeline ───────────────────────────────────────────────────

    def record_alert(
        self,
        severity: str,
        source: str,
        message: str,
        detail: str = ""
    ) -> None:
        """
        Records an alert to the internal buffer. Alerts are flushed to the
        rill_alerts DuckDB table during each write_to_duckdb() call.

        Args:
            severity: Alert level — "info", "warning", or "error".
            source: Component name (e.g. "engine.connector.orders", "user", "compute.sql_task.agg").
            message: Short human-readable alert summary.
            detail: Optional extended context, traceback, or description.
        """
        severity = severity.lower().strip()
        if severity not in ("info", "warning", "error"):
            severity = "info"

        entry = AlertEntry(severity=severity, source=source, message=message, detail=detail[:4096])
        with self._lock:
            self._alert_buffer.append(entry)

        # Also emit to Python logging
        log_msg = f"[Rill Alert] [{severity.upper()}] [{source}] {message}"
        if severity == "error":
            logger.error(log_msg)
        elif severity == "warning":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

    # ── Business Metrics ─────────────────────────────────────────────────

    def register_metric(self, name: str, evaluator: Callable[['RillEngine'], Any]) -> None:
        """
        Registers a custom business metric to be calculated during each trigger interval.

        Args:
            name: Metric identifier (e.g., 'real_time_revenue_by_tier').
            evaluator: Function taking `RillEngine` and returning a scalar or table summary.
        """
        with self._lock:
            self._business_evaluators[name] = evaluator

    def unregister_metric(self, name: str) -> None:
        """
        Removes a registered custom business metric.
        """
        with self._lock:
            if name in self._business_evaluators:
                del self._business_evaluators[name]
            if name in self.business_metrics:
                del self.business_metrics[name]

    def record_step(self, records_in_batch: int, latency_ms: float) -> None:
        """
        Updates system performance statistics for the completed micro-batch tick.
        """
        with self._lock:
            current_time = time.perf_counter()
            elapsed_sec = current_time - self._last_tick_time
            self._last_tick_time = current_time

            self.last_batch_records = records_in_batch
            self.total_records_processed += records_in_batch
            self.last_batch_latency_ms = latency_ms

            # Exponentially weighted or running average latency
            self._batch_count += 1
            if self._batch_count == 1:
                self.avg_batch_latency_ms = latency_ms
            else:
                self.avg_batch_latency_ms = (self.avg_batch_latency_ms * 0.9) + (latency_ms * 0.1)

            # Instantaneous / smoothed records per second
            if elapsed_sec > 0:
                instant_rps = records_in_batch / elapsed_sec
                if self._batch_count == 1:
                    self.records_per_second = instant_rps
                else:
                    self.records_per_second = (self.records_per_second * 0.8) + (instant_rps * 0.2)

    def record_connector_step(self, conn_name: str, records: int, elapsed_sec: float) -> None:
        with self._lock:
            if not hasattr(self, 'connector_rates'):
                self.connector_rates = {}
            if elapsed_sec > 0:
                instant_rps = records / elapsed_sec
                if conn_name not in self.connector_rates:
                    self.connector_rates[conn_name] = instant_rps
                else:
                    self.connector_rates[conn_name] = (self.connector_rates[conn_name] * 0.8) + (instant_rps * 0.2)

    def record_task_latency(self, task_name: str, latency_ms: float) -> None:
        with self._lock:
            if not hasattr(self, 'task_latencies'):
                self.task_latencies = {}
            if task_name not in self.task_latencies:
                self.task_latencies[task_name] = latency_ms
            else:
                self.task_latencies[task_name] = (self.task_latencies[task_name] * 0.9) + (latency_ms * 0.1)

    def evaluate_business_metrics(self, engine: 'RillEngine') -> Dict[str, Any]:
        """
        Evaluates all registered business logic evaluators against the live engine state.
        """
        with self._lock:
            evaluators = dict(self._business_evaluators)

        results = {}
        for name, evaluator in evaluators.items():
            try:
                results[name] = evaluator(engine)
            except Exception as e:
                results[name] = f"Error evaluating metric: {e}"
                self.record_alert(
                    severity="warning",
                    source=f"metrics.business.{name}",
                    message=f"Business metric '{name}' evaluation failed: {e}",
                    detail=traceback.format_exc()
                )

        with self._lock:
            self.business_metrics.update(results)
            return dict(self.business_metrics)

    def get_system_metrics(self, engine: Optional['RillEngine'] = None) -> Dict[str, Any]:
        """
        Returns a dictionary of all tracked system performance indicators and table row counts.
        """
        with self._lock:
            table_counts = {}
            if engine is not None:
                for name, table in engine.tables.items():
                    table_counts[name] = table.num_rows

            return {
                "total_records_processed": self.total_records_processed,
                "last_batch_records": self.last_batch_records,
                "last_batch_latency_ms": round(self.last_batch_latency_ms, 3),
                "avg_batch_latency_ms": round(self.avg_batch_latency_ms, 3),
                "records_per_second": round(self.records_per_second, 2),
                "table_row_counts": table_counts,
            }

    def get_all_metrics(self, engine: Optional['RillEngine'] = None) -> Dict[str, Any]:
        """
        Returns a combined dictionary containing system metrics and live business metrics.
        """
        metrics = self.get_system_metrics(engine)
        with self._lock:
            metrics["business_metrics"] = dict(self.business_metrics)
        return metrics

    def get_metrics_arrow_table(self, engine: Optional['RillEngine'] = None) -> pa.Table:
        """
        Returns a wide-format single-row PyArrow table containing the current system metrics snapshot.
        This is registered as the 'rill_metrics' virtual table in DuckDB for zero-copy SQL queries.
        """
        import json
        all_metrics = self.get_all_metrics(engine)

        cpu_usage = get_system_cpu_usage()
        total_mem, used_mem = get_system_memory()
        pyarrow_allocated = pa.total_allocated_bytes()
        pyarrow_max = pa.default_memory_pool().max_memory()

        business_metrics = all_metrics.get("business_metrics", {})
        business_json = json.dumps({k: v for k, v in business_metrics.items()
                                     if isinstance(v, (int, float, str, bool, type(None)))})

        return pa.table({
            "timestamp": [time.time()],
            "cpu_usage": [float(cpu_usage)],
            "total_memory": [float(total_mem)],
            "used_memory": [float(used_mem)],
            "pyarrow_allocated_bytes": [float(pyarrow_allocated)],
            "pyarrow_max_memory": [float(pyarrow_max)],
            "last_batch_records": [float(all_metrics.get("last_batch_records", 0))],
            "total_records_processed": [float(all_metrics.get("total_records_processed", 0))],
            "last_batch_latency_ms": [float(all_metrics.get("last_batch_latency_ms", 0.0))],
            "avg_batch_latency_ms": [float(all_metrics.get("avg_batch_latency_ms", 0.0))],
            "records_per_second": [float(all_metrics.get("records_per_second", 0.0))],
            "business_metrics": [business_json],
        })

    def write_to_duckdb(self, con: 'duckdb.DuckDBPyConnection', engine: 'RillEngine') -> None:
        """
        Writes current system and business metrics to the given DuckDB connection using a tall schema.
        Also flushes buffered alerts to rill_alerts and performs scheduled retention.
        """
        import duckdb

        cpu_usage = get_system_cpu_usage()
        total_mem, used_mem = get_system_memory()

        pyarrow_allocated = pa.total_allocated_bytes()
        pyarrow_max = pa.default_memory_pool().max_memory()

        all_metrics = self.get_all_metrics(engine)

        duckdb_live_bytes = 0
        try:
            duckdb_live_bytes = engine.duckdb.con.execute("SELECT SUM(memory_usage) FROM pragma_database_size()").fetchone()[0] or 0
        except Exception:
            pass

        duckdb_metrics_bytes = 0
        try:
            duckdb_metrics_bytes = con.execute("SELECT SUM(memory_usage) FROM pragma_database_size()").fetchone()[0] or 0
        except Exception:
            pass

        ts = time.time()

        metrics_list = [
            (ts, "cpu_usage", float(cpu_usage)),
            (ts, "total_memory", float(total_mem)),
            (ts, "used_memory", float(used_mem)),
            (ts, "pyarrow_allocated_bytes", float(pyarrow_allocated)),
            (ts, "pyarrow_max_memory", float(pyarrow_max)),
            (ts, "duckdb_live_bytes", float(duckdb_live_bytes)),
            (ts, "duckdb_metrics_bytes", float(duckdb_metrics_bytes)),
            (ts, "last_batch_records", float(all_metrics.get("last_batch_records", 0))),
            (ts, "total_records_processed", float(all_metrics.get("total_records_processed", 0))),
            (ts, "last_batch_latency_ms", float(all_metrics.get("last_batch_latency_ms", 0.0))),
            (ts, "avg_batch_latency_ms", float(all_metrics.get("avg_batch_latency_ms", 0.0))),
            (ts, "records_per_second", float(all_metrics.get("records_per_second", 0.0)))
        ]

        # Add per-connector ingestion rates
        with self._lock:
            if hasattr(self, 'connector_rates'):
                for conn_name, rate in self.connector_rates.items():
                    metrics_list.append((ts, f"ingestion_rate_{conn_name}", float(rate)))

            # Add per-task latencies
            if hasattr(self, 'task_latencies'):
                for task_name, lat in self.task_latencies.items():
                    metrics_list.append((ts, f"task_latency_ms_{task_name}", float(lat)))

        business_metrics = all_metrics.get("business_metrics", {})
        for k, v in business_metrics.items():
            try:
                metrics_list.append((ts, k, float(v)))
            except (ValueError, TypeError):
                pass

        if not metrics_list:
            return

        # Write metrics to duckdb
        import re
        def sanitize_name(name: str) -> str:
            return re.sub(r'\W+', '_', name).strip('_').lower()

        try:
            con.execute("BEGIN TRANSACTION")
            con.executemany("INSERT INTO rill_metrics VALUES (?, ?, ?)", metrics_list)

            # Scheduled retention
            cutoff = ts - getattr(engine, 'metrics_ttl_seconds', 600.0)
            con.execute("DELETE FROM rill_metrics WHERE ts < ?", [cutoff])

            # Write task metrics to individual tables
            with self._lock:
                if hasattr(self, 'task_latencies'):
                    for task_name, lat in self.task_latencies.items():
                        t_name = f"task_metrics_{sanitize_name(task_name)}"
                        con.execute(f"CREATE TABLE IF NOT EXISTS {t_name} (ts DOUBLE, latency_ms DOUBLE)")
                        con.execute(f"INSERT INTO {t_name} VALUES (?, ?)", [ts, float(lat)])
                        con.execute(f"DELETE FROM {t_name} WHERE ts < ?", [cutoff])

            # Write business metrics to individual tables
            for k, v in business_metrics.items():
                try:
                    val = float(v)
                    b_name = f"business_metrics_{sanitize_name(k)}"
                    con.execute(f"CREATE TABLE IF NOT EXISTS {b_name} (ts DOUBLE, value DOUBLE)")
                    con.execute(f"INSERT INTO {b_name} VALUES (?, ?)", [ts, val])
                    con.execute(f"DELETE FROM {b_name} WHERE ts < ?", [cutoff])
                except (ValueError, TypeError):
                    pass

            con.commit()
        except Exception:
            con.execute("ROLLBACK")
            logger.exception("Failed to write metrics to DuckDB")

        # Flush buffered alerts to rill_alerts table
        with self._lock:
            alerts_to_flush = list(self._alert_buffer)
            self._alert_buffer.clear()

        if alerts_to_flush:
            try:
                con.execute("BEGIN TRANSACTION")
                alert_rows = [a.as_tuple() for a in alerts_to_flush]
                con.executemany(
                    "INSERT INTO rill_alerts VALUES (?, ?, ?, ?, ?)",
                    alert_rows
                )
                # Alert retention
                cutoff = ts - getattr(engine, 'metrics_ttl_seconds', 600.0)
                con.execute("DELETE FROM rill_alerts WHERE ts < ?", [cutoff])
                con.commit()
            except Exception:
                con.execute("ROLLBACK")
                logger.exception("Failed to write alerts to DuckDB")
