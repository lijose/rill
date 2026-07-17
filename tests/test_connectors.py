"""
Tests for Rill Input API connectors.
"""

import io
from pathlib import Path
import pyarrow as pa
from rill.connectors.memory import MemoryConnector
from rill.connectors.json_stream import JSONStreamConnector
from rill.connectors.file import FileStreamConnector


def test_memory_connector():
    connector = MemoryConnector(target_table="test_tbl")
    assert connector.read_batch() is None

    connector.push({"col1": [1, 2], "col2": ["x", "y"]})
    connector.push({"col1": [3], "col2": ["z"]})

    batch_table = connector.read_batch()
    assert batch_table is not None
    assert batch_table.num_rows == 3
    assert batch_table.column("col1").to_pylist() == [1, 2, 3]

    # After reading, buffer should be drained
    assert connector.read_batch() is None


def test_json_stream_connector():
    connector = JSONStreamConnector(target_table="json_tbl")
    assert connector.read_batch() is None

    # Push raw NDJSON bytes
    raw_bytes = (
        b'{"id": 1, "status": "active", "val": 9.9}\n'
        b'{"id": 2, "status": "pending", "val": 15.2}\n'
    )
    connector.push_bytes(raw_bytes)

    table = connector.read_batch()
    assert table is not None
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 2]
    assert table.column("status").to_pylist() == ["active", "pending"]


def test_file_stream_connector(tmp_path: Path):
    data_dir = tmp_path / "stream_data"
    data_dir.mkdir()

    connector = FileStreamConnector(
        target_table="file_tbl",
        watch_path=data_dir,
        file_pattern="*.json",
        delete_after_process=False
    )

    assert connector.read_batch() is None

    # Write a JSON file into data_dir
    f1 = data_dir / "event1.json"
    f1.write_text('{"event": "login", "user": "alice"}\n{"event": "logout", "user": "alice"}\n')

    table = connector.read_batch()
    assert table is not None
    assert table.num_rows == 2
    assert table.column("event").to_pylist() == ["login", "logout"]

    # Second read without new files should yield None because f1 is tracked as processed
    assert connector.read_batch() is None
