"""
System performance tracking and live business logic evaluation metrics for Rill Engine.
"""

import time
import threading
from typing import Dict, Any, Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .engine import RillEngine


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
