"""
Tests for Table TTL and Retention Policies (max_rows and max_age_seconds).
"""

import time
import pyarrow as pa
from rill import RillEngine, RillTable, RetentionPolicy


def test_retention_max_rows():
    policy = RetentionPolicy(max_rows=3)
    table = RillTable("events", retention_policy=policy)

    # Insert 5 rows
    batch = pa.Table.from_pydict({
        "id": [1, 2, 3, 4, 5],
        "val": ["a", "b", "c", "d", "e"]
    })
    table.upsert(batch)

    # Should retain only the most recent 3 rows (3, 4, 5)
    assert table.num_rows == 3
    assert table.to_arrow().column("id").to_pylist() == [3, 4, 5]


def test_retention_max_age_seconds():
    policy = RetentionPolicy(max_age_seconds=10.0, time_column="ts")
    table = RillTable("metrics", retention_policy=policy)

    current_ts = 1000.0
    # Insert rows with timestamps ranging from 985.0 to 998.0 evaluated against current_ts
    batch = pa.Table.from_pydict({
        "ts": [985.0, 989.0, 991.0, 995.0, 998.0],
        "metric": [10, 20, 30, 40, 50]
    })
    table.upsert(batch, current_time=current_ts)

    # Apply retention evaluated at current_ts = 1000.0 (cutoff is 990.0)
    table.apply_retention(current_time=current_ts)

    # Only rows with ts >= 990.0 should remain (991.0, 995.0, 998.0)
    assert table.num_rows == 3
    assert table.to_arrow().column("ts").to_pylist() == [991.0, 995.0, 998.0]
