"""
Memory/Queue Connector for Rill Streaming Engine.
Ideal for testing, local simulations, and programmatic event injection.
"""

import threading
from typing import Union, List, Optional, Any, Dict
import pyarrow as pa
from .base import BaseConnector


class MemoryConnector(BaseConnector):
    """
    In-memory queue connector. Programmatic events, PyArrow batches, or dictionaries can be pushed
    at any frequency using `.push()`. During each micro-batch tick, all buffered events are extracted
    and yielded as a single contiguous PyArrow Table.
    """

    def __init__(
        self,
        target_table: str,
        schema: Optional[pa.Schema] = None,
        max_buffer_records: Optional[int] = None,
        on_overflow: str = "drop_oldest"
    ):
        super().__init__(target_table, max_buffer_records=max_buffer_records, on_overflow=on_overflow)
        self.schema = schema
        self._buffer: List[pa.RecordBatch] = []
        self._lock = threading.RLock()

    def _enforce_backpressure(self) -> None:
        """
        Checks current buffered row count against `max_buffer_records` and enforces `on_overflow` policy.
        """
        if self.max_buffer_records is None or self.max_buffer_records <= 0:
            return

        total_rows = sum(b.num_rows for b in self._buffer)
        if total_rows <= self.max_buffer_records:
            return

        if self.on_overflow == "error":
            raise BufferError(f"Connector buffer for '{self.target_table}' exceeded max_buffer_records ({self.max_buffer_records})")
        
        excess = total_rows - self.max_buffer_records
        if self.on_overflow == "drop_newest":
            while self._buffer and excess > 0:
                last_batch = self._buffer[-1]
                if last_batch.num_rows <= excess:
                    excess -= last_batch.num_rows
                    self._buffer.pop()
                else:
                    self._buffer[-1] = last_batch.slice(0, last_batch.num_rows - excess)
                    excess = 0
        else:  # drop_oldest (default)
            while self._buffer and excess > 0:
                first_batch = self._buffer[0]
                if first_batch.num_rows <= excess:
                    excess -= first_batch.num_rows
                    self._buffer.pop(0)
                else:
                    self._buffer[0] = first_batch.slice(excess, first_batch.num_rows - excess)
                    excess = 0

    def push(
        self,
        data: Union[pa.Table, pa.RecordBatch, List[pa.RecordBatch], Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:
        """
        Pushes events or PyArrow batches into the connector's thread-safe internal buffer.

        Args:
            data: PyArrow Table, RecordBatch, list of RecordBatches, dict of arrays/lists, or list of row dicts.
        """
        with self._lock:
            if isinstance(data, pa.Table):
                for batch in data.to_batches():
                    self._buffer.append(batch)
            elif isinstance(data, pa.RecordBatch):
                self._buffer.append(data)
            elif isinstance(data, list):
                if not data:
                    return
                if isinstance(data[0], pa.RecordBatch):
                    self._buffer.extend(data)
                elif isinstance(data[0], dict):
                    # List of row dicts -> convert to column dict then pa.RecordBatch
                    keys = data[0].keys()
                    col_dict = {k: [row[k] for row in data] for k in keys}
                    batch = pa.RecordBatch.from_pydict(col_dict, schema=self.schema)
                    self._buffer.append(batch)
                else:
                    raise TypeError(f"Unsupported list item type: {type(data[0])}")
            elif isinstance(data, dict):
                # Dict of columns -> pa.RecordBatch
                batch = pa.RecordBatch.from_pydict(data, schema=self.schema)
                self._buffer.append(batch)
            else:
                raise TypeError(f"Unsupported push data type: {type(data)}")

            self._enforce_backpressure()

    def read_batch(self) -> Optional[pa.Table]:
        """
        Extracts all buffered batches and combines them into a single `pa.Table`.
        """
        with self._lock:
            if not self._buffer:
                return None
            batches = list(self._buffer)
            self._buffer.clear()

        return pa.Table.from_batches(batches, schema=self.schema)
