"""
Tests for RillTable processing modes (`mode="snapshot"` vs `mode="append"`) and mandatory TTL governance.
"""

import pytest
import pyarrow as pa
from rill import RillTable, RetentionPolicy, schema, extract_mode


def test_append_mode_mandatory_ttl_validation():
    # Attempting to create an append-only table without a RetentionPolicy must fail
    with pytest.raises(ValueError, match="Retention policy \\(TTL\\).*is mandatory for append-only tables"):
        RillTable("logs", mode="append", retention_policy=None)

    # Attempting to create via schema metadata with mode="append" without retention must fail
    s = schema([("msg", pa.string())], mode="append")
    assert extract_mode(s) == "append"
    with pytest.raises(ValueError, match="Retention policy \\(TTL\\).*is mandatory for append-only tables"):
        RillTable("logs_schema", schema=s, retention_policy=None)


def test_append_mode_does_not_overwrite_primary_key():
    # In append mode, even if a primary_key is present in schema, incoming rows are always appended
    s = schema([("id", pa.int64()), ("event", pa.string())], primary_key="id", mode="append")
    retention = RetentionPolicy(max_rows=10)
    tbl = RillTable("events", schema=s, retention_policy=retention)

    # Insert row id=1
    tbl.upsert(pa.Table.from_pydict({"id": [1], "event": ["login"]}))
    # Insert row id=1 again (new event)
    tbl.upsert(pa.Table.from_pydict({"id": [1], "event": ["logout"]}))

    assert tbl.num_rows == 2
    events = tbl.to_arrow().column("event").to_pylist()
    assert events == ["login", "logout"]


def test_snapshot_mode_overwrites_primary_key():
    # In default snapshot mode, incoming rows matching primary_key replace old rows
    s = schema([("id", pa.int64()), ("state", pa.string())], primary_key="id", mode="snapshot")
    tbl = RillTable("user_state", schema=s)

    tbl.upsert(pa.Table.from_pydict({"id": [1], "state": ["active"]}))
    tbl.upsert(pa.Table.from_pydict({"id": [1], "state": ["idle"]}))

    assert tbl.num_rows == 1
    assert tbl.to_arrow().column("state").to_pylist() == ["idle"]
