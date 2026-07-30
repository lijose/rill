"""
Base Connector class for Rill Input API.
Connectors capture high-frequency raw bytes or events and buffer them for extraction
during scheduled micro-batch ticks.
"""

from abc import ABC, abstractmethod
from typing import Union, List, Optional
import pyarrow as pa


class BaseConnector(ABC):
    """
    Abstract base class for all Rill connectors.
    Connectors ingest external streams into an internal buffer and yield C++ PyArrow
    Tables/RecordBatches when `read_batch()` is invoked by the `RillEngine` during each micro-batch tick.
    Supports optional backpressure and bounded buffer limits (`max_buffer_records`).
    """

    def __init__(
        self,
        target_table: str,
        max_buffer_records: Optional[int] = None,
        on_overflow: str = "drop_oldest"
    ):
        """
        Args:
            target_table: Name of the `RillTable` inside `RillEngine` that this connector feeds into.
            max_buffer_records: Optional maximum number of buffered rows before applying backpressure/overflow strategy.
            on_overflow: Strategy when buffer exceeds `max_buffer_records` ('drop_oldest', 'drop_newest', or 'error').
        """
        self.target_table = target_table
        self.max_buffer_records = max_buffer_records
        self.on_overflow = on_overflow

    def start(self) -> None:
        """
        Starts any background listening threads, socket connections, or file monitoring.
        Default implementation is a no-op for pull-based connectors.
        """
        pass

    def stop(self) -> None:
        """
        Stops background listening, closes sockets/connections, and cleans up resources.
        Default implementation is a no-op.
        """
        pass

    @abstractmethod
    def read_batch(self) -> Optional[Union[pa.Table, pa.RecordBatch, List[pa.RecordBatch]]]:
        """
        Extracts all buffered events accumulated since the last micro-batch tick and converts
        them directly into PyArrow memory structures.

        Returns:
            A `pa.Table`, `pa.RecordBatch`, list of `pa.RecordBatch`es, or None if buffer is empty.
        """
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(target_table='{self.target_table}')>"
