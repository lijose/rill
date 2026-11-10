"""
Memory/Queue Connector for Rill Streaming Engine.
Ideal for testing, local simulations, and programmatic event injection.
"""

import threading
from typing import Union, List, Optional, Any, Dict
import pyarrow as pa
from .base import BaseConnector
from collections import deque
import logging

logger = logging.getLogger(__name__)


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
        self._buffer: deque = deque()
        self._total_rows: int = 0
        self._lock = threading.RLock()

    def _enforce_backpressure(self) -> None:
        """
        Checks current buffered row count against `max_buffer_records` and enforces `on_overflow` policy.
        """
        if self.max_buffer_records is None or self.max_buffer_records <= 0:
            return

        total_rows = self._total_rows
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
                    self._total_rows -= last_batch.num_rows
                    self._buffer.pop()
                else:
                    self._buffer[-1] = last_batch.slice(0, last_batch.num_rows - excess)
                    self._total_rows -= excess
                    excess = 0
        else:  # drop_oldest (default)
            while self._buffer and excess > 0:
                first_batch = self._buffer[0]
                if first_batch.num_rows <= excess:
                    excess -= first_batch.num_rows
                    self._total_rows -= first_batch.num_rows
                    self._buffer.popleft()
                else:
                    self._buffer[0] = first_batch.slice(excess, first_batch.num_rows - excess)
                    self._total_rows -= excess
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
            # Stage the incoming batches without copying the existing queue.
            if isinstance(data, pa.Table):
                batches = data.to_batches()
            elif isinstance(data, pa.RecordBatch):
                batches = [data]
            elif isinstance(data, list):
                if not data:
                    return
                if all(isinstance(item, pa.RecordBatch) for item in data):
                    batches = data
                elif all(isinstance(item, dict) for item in data):
                    keys = self.schema.names if self.schema is not None else dict.fromkeys(k for row in data for k in row)
                    columns = {key: [row.get(key) for row in data] for key in keys}
                    batches = [pa.RecordBatch.from_pydict(columns, schema=self.schema)]
                else:
                    raise TypeError("Expected a list of RecordBatches or row dictionaries")
            elif isinstance(data, dict):
                try:
                    batch = pa.RecordBatch.from_pydict(data, schema=self.schema)
                except (pa.ArrowInvalid, KeyError, ValueError):
                    batch = pa.RecordBatch.from_pydict(data)
                batches = [batch]
            else:
                raise TypeError(f"Unsupported push data type: {type(data)}")
            incoming_rows = sum(batch.num_rows for batch in batches)
            if (self.on_overflow == "error" and self.max_buffer_records is not None
                    and self._total_rows + incoming_rows > self.max_buffer_records):
                raise BufferError(f"Connector buffer for {self.target_table!r} exceeded max_buffer_records ({self.max_buffer_records})")
            self._buffer.extend(batches)
            self._total_rows += incoming_rows
            self._enforce_backpressure()

    def read_batch(self) -> Optional[pa.Table]:
        """
        Extracts all buffered batches and combines them into a single `pa.Table`.
        """
        with self._lock:
            if not self._buffer:
                return None
            batches = list(self._buffer)
            tables = [pa.Table.from_batches([batch]) for batch in batches]
            result = pa.concat_tables(tables, promote_options="default")
            if self.schema is not None and set(result.column_names) == set(self.schema.names):
                result = result.select(self.schema.names).cast(self.schema)
            self._buffer.clear()
            self._total_rows = 0
            return result
