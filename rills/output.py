"""
Output API for Rill Streaming Engine.
Exposes finalized PyArrow tables, scalar values, callback subscriptions, and data sinks
for integration with frontend dashboards (Streamlit, Tkinter) and downstream consumers.
"""

from pathlib import Path
import logging
from typing import Optional, Dict, Any, Callable, Union, TYPE_CHECKING
import pyarrow as pa

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .engine import RillEngine


class OutputAPI:
    """
    Exposes clean accessors and callback mechanisms to extract live tables and scalar
    metrics from a running `RillEngine`.
    """

    def __init__(self, engine: 'RillEngine'):
        self.engine = engine

    def query_metrics(self, sql: str) -> pa.Table:
        """
        Executes a read-only SQL query against the metrics database.
        """
        return self.engine.query_metrics(sql)

    def get_scalar(self, metric_name: str, default: Any = None) -> Any:
        """
        Retrieves a specific scalar metric (system KPI or calculated business metric).
        """
        all_metrics = self.engine.get_metrics()
        return all_metrics.get(metric_name, default)

    def get_all_scalars(self) -> Dict[str, Any]:
        """
        Retrieves all system metrics.
        """
        return self.engine.get_metrics()
