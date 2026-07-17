"""
Tests for connector backpressure and bounded memory buffers.
"""

import pytest
import pyarrow as pa
from rill import MemoryConnector, JSONStreamConnector


def test_memory_connector_backpressure_drop_oldest():
    connector = MemoryConnector("target", max_buffer_records=3, on_overflow="drop_oldest")
    
    # Push 5 rows in total
    connector.push(pa.Table.from_pydict({"v": [1, 2, 3]}))
    connector.push(pa.Table.from_pydict({"v": [4, 5]}))

    batch = connector.read_batch()
    assert batch is not None
    # Buffer capacity is 3, drop_oldest should keep the last 3 records (3, 4, 5)
    assert batch.num_rows == 3
    assert batch.column("v").to_pylist() == [3, 4, 5]


def test_memory_connector_backpressure_error():
    connector = MemoryConnector("target", max_buffer_records=2, on_overflow="error")
    
    connector.push(pa.Table.from_pydict({"v": [1, 2]}))
    with pytest.raises(BufferError):
        connector.push(pa.Table.from_pydict({"v": [3]}))


def test_json_stream_connector_max_buffer_bytes():
    connector = JSONStreamConnector("target", max_buffer_bytes=25, on_overflow="drop_oldest")
    
    # Push two JSON lines exceeding 25 bytes
    connector.push_bytes('{"id": 1, "name": "first"}\n')
    connector.push_bytes('{"id": 2, "name": "second"}\n')

    batch = connector.read_batch()
    assert batch is not None
    # First chunk should be cleared due to size limit, leaving second chunk
    assert batch.num_rows == 1
    assert batch.column("id").to_pylist() == [2]
