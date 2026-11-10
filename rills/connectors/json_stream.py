"""Bounded NDJSON ingestion with record-preserving overflow handling."""
import io
import threading
from collections import deque
from typing import Optional, Union
import pyarrow as pa
import pyarrow.json as pajson
from .base import BaseConnector


class JSONStreamConnector(BaseConnector):
    """Accept chunks containing complete NDJSON records (not partial records).

    Limits include the newline delimiter. Drop policies discard whole records;
    an individually oversized record is dropped, or raises in error mode.
    Error-mode pushes are atomic. Parsing remains in PyArrow's C++ reader.
    """
    def __init__(self, target_table: str,
                 parse_options: Optional[pajson.ParseOptions] = None,
                 read_options: Optional[pajson.ReadOptions] = None,
                 max_buffer_records: Optional[int] = None,
                 max_buffer_bytes: Optional[int] = None,
                 on_overflow: str = "drop_oldest"):
        super().__init__(target_table, max_buffer_records, on_overflow)
        if max_buffer_bytes is not None and (not isinstance(max_buffer_bytes, int) or isinstance(max_buffer_bytes, bool) or max_buffer_bytes <= 0):
            raise ValueError("max_buffer_bytes must be a positive integer")
        self.parse_options = parse_options or pajson.ParseOptions()
        self.read_options = read_options or pajson.ReadOptions()
        self.max_buffer_bytes = max_buffer_bytes
        self._records = deque()
        self._total_bytes = 0
        self._lock = threading.RLock()
        self._has_data = False
        self._last_byte_was_newline = False

    def push_bytes(self, raw_bytes: Union[bytes, bytearray, str]) -> None:
        if isinstance(raw_bytes, str):
            raw_bytes = raw_bytes.encode('utf-8')
        if not raw_bytes:
            return
        records = [bytes(line) + b'\n' for line in raw_bytes.splitlines() if line.strip()]
        with self._lock:
            if self.on_overflow == 'error':
                if ((self.max_buffer_records is not None and len(self._records) + len(records) > self.max_buffer_records)
                    or (self.max_buffer_bytes is not None and self._total_bytes + sum(map(len, records)) > self.max_buffer_bytes)):
                    raise BufferError(f"JSONStreamConnector buffer for {self.target_table!r} exceeded its limit")
            for record in records:
                size = len(record)
                if self.max_buffer_bytes is not None and size > self.max_buffer_bytes:
                    continue
                def exceeds():
                    return ((self.max_buffer_records is not None and len(self._records) + 1 > self.max_buffer_records)
                            or (self.max_buffer_bytes is not None and self._total_bytes + size > self.max_buffer_bytes))
                if self.on_overflow == 'drop_newest' and exceeds():
                    continue
                while self._records and exceeds():
                    self._total_bytes -= len(self._records.popleft())
                self._records.append(record)
                self._total_bytes += size
            self._has_data = bool(self._records)
            self._last_byte_was_newline = raw_bytes.endswith(b'\n')

    def read_batch(self) -> Optional[pa.Table]:
        with self._lock:
            if not self._records:
                return None
            try:
                table = pajson.read_json(io.BytesIO(b''.join(self._records)),
                                         parse_options=self.parse_options,
                                         read_options=self.read_options)
            except pa.ArrowException as exc:
                raise RuntimeError(f"JSONStreamConnector failed to parse NDJSON: {exc}") from exc
            self._records.clear()
            self._total_bytes = 0
            self._has_data = False
            self._last_byte_was_newline = False
            return table
