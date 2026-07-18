"""
System performance tracking and live business logic evaluation metrics for Rill Engine.
"""

import time
import threading
from typing import Dict, Any, Callable, Optional, TYPE_CHECKING
import json
import pyarrow as pa

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


def get_system_cpu_usage() -> float:
    if psutil is not None:
        try:
            val = psutil.cpu_percent()
            if val > 0:
                return val
        except Exception:
            pass
    # Fallback using /proc/stat
    try:
        with open('/proc/stat', 'r') as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith('cpu '):
                parts = [float(x) for x in line.split()[1:]]
                idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
                total = sum(parts)
                return round(100.0 * (1.0 - idle / total), 1) if total > 0 else 0.0
    except Exception:
        pass
    return 0.0


def get_system_memory() -> tuple[int, int]:
    if psutil is not None:
        try:
            mem = psutil.virtual_memory()
            if mem.total > 0:
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
        return total, used
    except Exception:
        pass
    return 0, 0



class MetricsRegistry:
    """
    Tracks real-time system performance metrics and evaluates user-defined live business logic
    during each scheduled micro-batch trigger interval.
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

    def get_metrics_arrow_table(self, engine: 'RillEngine') -> pa.Table:
        """
        Constructs a single-row PyArrow Table of the current system, engine, and PyArrow memory metrics.
        """
        cpu_usage = get_system_cpu_usage()
        total_mem, used_mem = get_system_memory()
        
        pyarrow_allocated = pa.total_allocated_bytes()
        pyarrow_max = pa.default_memory_pool().max_memory()
        
        all_metrics = self.get_all_metrics(engine)
        business_metrics_json = json.dumps(all_metrics.get("business_metrics", {}))
        
        data = {
            "timestamp": [time.time()],
            "cpu_usage": [cpu_usage],
            "total_memory": [total_mem],
            "used_memory": [used_mem],
            "pyarrow_allocated_bytes": [pyarrow_allocated],
            "pyarrow_max_memory": [pyarrow_max],
            "last_batch_records": [all_metrics.get("last_batch_records", 0)],
            "total_records_processed": [all_metrics.get("total_records_processed", 0)],
            "last_batch_latency_ms": [all_metrics.get("last_batch_latency_ms", 0.0)],
            "avg_batch_latency_ms": [all_metrics.get("avg_batch_latency_ms", 0.0)],
            "records_per_second": [all_metrics.get("records_per_second", 0.0)],
            "business_metrics": [business_metrics_json],
        }
        
        schema = pa.schema([
            ("timestamp", pa.float64()),
            ("cpu_usage", pa.float64()),
            ("total_memory", pa.int64()),
            ("used_memory", pa.int64()),
            ("pyarrow_allocated_bytes", pa.int64()),
            ("pyarrow_max_memory", pa.int64()),
            ("last_batch_records", pa.int64()),
            ("total_records_processed", pa.int64()),
            ("last_batch_latency_ms", pa.float64()),
            ("avg_batch_latency_ms", pa.float64()),
            ("records_per_second", pa.float64()),
            ("business_metrics", pa.string()),
        ])
        
        return pa.Table.from_pydict(data, schema=schema)
