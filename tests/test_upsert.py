"""
Tests for Rill vectorized Delete-and-Insert (Upsert) compute kernels.
"""

import pytest
import pyarrow as pa
from rill.compute.upsert import upsert_table


def test_upsert_single_primary_key():
    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("score", pa.float64())
    ])

    old_batch = pa.RecordBatch.from_pydict({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [10.0, 20.0, 30.0]
    }, schema=schema)
    old_table = pa.Table.from_batches([old_batch])

    # Incoming batch updates id=2 and adds id=4
    new_batch = pa.RecordBatch.from_pydict({
        "id": [2, 4],
        "name": ["Bob Updated", "David"],
        "score": [25.5, 40.0]
    }, schema=schema)
    new_table = pa.Table.from_batches([new_batch])

    result = upsert_table(old_table, new_table, primary_key="id")
    assert result.num_rows == 4

    # Sort by id for predictable assertion check
    sorted_res = result.sort_by("id")
    ids = sorted_res.column("id").to_pylist()
    names = sorted_res.column("name").to_pylist()
    scores = sorted_res.column("score").to_pylist()

    assert ids == [1, 2, 3, 4]
    assert names == ["Alice", "Bob Updated", "Charlie", "David"]
    assert scores == [10.0, 25.5, 30.0, 40.0]


def test_upsert_composite_primary_key():
    schema = pa.schema([
        ("user_id", pa.int64()),
        ("session_id", pa.string()),
        ("clicks", pa.int64())
    ])

    old_table = pa.Table.from_pydict({
        "user_id": [100, 100, 200],
        "session_id": ["s1", "s2", "s1"],
        "clicks": [5, 10, 3]
    }, schema=schema)

    new_table = pa.Table.from_pydict({
        "user_id": [100, 300],
        "session_id": ["s2", "s1"],
        "clicks": [15, 1]
    }, schema=schema)

    result = upsert_table(old_table, new_table, primary_key=["user_id", "session_id"])
    assert result.num_rows == 4

    # Check updated row for (100, "s2") using pure PyArrow
    rows = result.to_pylist()
    assert len(rows) == 4
    clicks_for_100_s2 = next(
        r["clicks"] for r in rows if r["user_id"] == 100 and r["session_id"] == "s2"
    )
    assert clicks_for_100_s2 == 15


def test_upsert_no_primary_key_append():
    t1 = pa.Table.from_pydict({"val": [1, 2]})
    t2 = pa.Table.from_pydict({"val": [3, 4]})
    res = upsert_table(t1, t2, primary_key=None)
    assert res.num_rows == 4
    assert res.column("val").to_pylist() == [1, 2, 3, 4]


def test_upsert_empty_or_none_old_table():
    new_t = pa.Table.from_pydict({"a": [10, 20]})
    res = upsert_table(None, new_t, primary_key="a")
    assert res.num_rows == 2
    assert res.column("a").to_pylist() == [10, 20]
