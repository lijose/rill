"""
Tests for zero-copy DuckDB SQL querying and scheduled SQL transformations over PyArrow tables.
"""

import pyarrow as pa
from rill.engine import RillEngine
from rill.compute.sql import ScheduledSQLTask


def test_duckdb_zero_copy_query():
    engine = RillEngine()
    tbl = engine.register_table("users", primary_key="user_id")
    tbl.upsert(pa.Table.from_pydict({
        "user_id": [1, 2, 3],
        "tier": ["free", "pro", "pro"],
        "spend": [0.0, 99.0, 150.0]
    }))

    # Run ad-hoc SQL query via DuckDB directly over PyArrow table
    result = engine.query_sql("SELECT tier, SUM(spend) AS total_spend, COUNT(*) AS cnt FROM users GROUP BY tier")
    assert result is not None
    assert result.num_rows == 2

    # Check spend totals per tier via pyarrow
    pro_spend = sum(
        row["total_spend"] for row in result.to_pylist() if row["tier"] == "pro"
    )
    free_cnt = sum(
        row["cnt"] for row in result.to_pylist() if row["tier"] == "free"
    )
    assert pro_spend == 249.0
    assert free_cnt == 1


def test_scheduled_sql_task_trigger():
    engine = RillEngine(trigger_interval_ms=50)
    tbl = engine.register_table("orders", primary_key="order_id")
    tbl.upsert(pa.Table.from_pydict({
        "order_id": [101, 102],
        "category": ["electronics", "books"],
        "amount": [500.0, 30.0]
    }))

    # Add scheduled SQL task that creates 'category_summary' every 0.1 seconds
    task = ScheduledSQLTask(
        name="summary_job",
        query="SELECT category, SUM(amount) AS total FROM orders GROUP BY category",
        output_table="category_summary",
        interval_seconds=0.1
    )
    engine.add_sql_task(task)

    # Execute a step right away - since last_run_time is 0, task should run immediately
    engine.step()

    summary_tbl = engine.get_table("category_summary")
    assert summary_tbl is not None
    assert summary_tbl.num_rows == 2

    # Verify output table rows via pure PyArrow
    rows = summary_tbl.to_arrow().to_pylist()
    elec_total = next(row["total"] for row in rows if row["category"] == "electronics")
    assert elec_total == 500.0
