"""
JSON Byte Stream Connector for Rill Streaming Engine.
Captures high-frequency raw bytes and parses them directly into PyArrow tables using C++
vectorized readers (`pyarrow.json.read_json`), entirely bypassing Python dictionary overhead.
"""

import io
import threading
from typing import Optional, Union, List
import pyarrow as pa
import pyarrow.json as pajson
from .base import BaseConnector


class JSONStreamConnector(BaseConnector):
    """
    Ingests raw byte chunks containing newline-delimited JSON (NDJSON) or JSON arrays.
    Parses them directly into PyArrow Tables using C++ memory (`pyarrow.json.read_json`).
    """

    def __init__(
        self,
        target_table: str,
        parse_options: Optional[pajson.ParseOptions] = None,
        read_options: Optional[pajson.ReadOptions] = None,
        max_buffer_records: Optional[int] = None,
        max_buffer_bytes: Optional[int] = None,
        on_overflow: str = "drop_oldest"
    ):
        super().__init__(target_table, max_buffer_records=max_buffer_records, on_overflow=on_overflow)
        self.parse_options = parse_options or pajson.ParseOptions()
        self.read_options = read_options or pajson.ReadOptions()
        self.max_buffer_bytes = max_buffer_bytes
        self._buffer = io.BytesIO()
        self._lock = threading.RLock()
        self._has_data = False

    def push_bytes(self, raw_bytes: Union[bytes, bytearray, str]) -> None:
        """
        Appends raw high-frequency byte chunks to the internal byte buffer.

        Args:
            raw_bytes: Raw byte string (or string encoded as UTF-8) representing JSON lines or arrays.
        """
        if isinstance(raw_bytes, str):
            raw_bytes = raw_bytes.encode('utf-8')

        if not raw_bytes:
            return

        with self._lock:
            # Enforce max_buffer_bytes backpressure before appending
            if self.max_buffer_bytes is not None and self.max_buffer_bytes > 0:
                current_size = self._buffer.tell()
                if current_size + len(raw_bytes) > self.max_buffer_bytes:
                    if self.on_overflow == "error":
                        raise BufferError(f"JSONStreamConnector buffer for '{self.target_table}' exceeded max_buffer_bytes ({self.max_buffer_bytes})")
                    elif self.on_overflow == "drop_newest":
                        return  # Shed new chunk
                    else:  # drop_oldest
                        # Clear old buffer bytes and keep new chunk if fitting
                        self._buffer = io.BytesIO()
                        self._has_data = False

            # Ensure newline between appended chunks if NDJSON
            if self._has_data and not self._buffer.getvalue().endswith(b'\n') and not raw_bytes.startswith(b'\n'):
                self._buffer.write(b'\n')
            self._buffer.write(raw_bytes)
            self._has_data = True

    def read_batch(self) -> Optional[pa.Table]:
        """
        Reads the buffered bytes and invokes `pyarrow.json.read_json` in C++ memory.
        Returns `pa.Table` or None if buffer is empty.
        """
        with self._lock:
            if not self._has_data:
                return None

            raw_data = self._buffer.getvalue()
            # Reset buffer
            self._buffer = io.BytesIO()
            self._has_data = False

        if not raw_data.strip():
            return None

        # Parse directly in C++ via pyarrow.json.read_json
        stream = io.BytesIO(raw_data)
        try:
            table = pajson.read_json(
                stream,
                parse_options=self.parse_options,
                read_options=self.read_options
            )
            return table
        except (pa.ArrowInvalid, pa.ArrowException) as e:
            # If parsing fails on malformed bytes, raise or log
            raise RuntimeError(f"JSONStreamConnector failed to parse bytes directly to PyArrow Table: {e}") from e
