"""
Tests for Rill Engine performance and architecture optimizations:
- PyArrow chunk compaction (`compact`, `compact_all`, auto-compaction)
- DuckDB prepared statement caching (`use_prepared_statements`)
- PyArrow dictionary encoding (`pa.dictionary` categorical support)
- Incremental micro-batch joins (`TableJoinTask(incremental=True)`)
"""

import time
import pyarrow as pa
import pytest
from rill import (
    RillEngine,
    MemoryConnector,
    ScheduledSQLTask,
    TableJoinTask,
    RetentionPolicy,
    schema as rill_schema,
)


def test_table_compaction():
    engine = RillEngine(trigger_interval_ms=10.0, auto_compact_chunks=5)
    
    # Register an append table without schema
    table = engine.register_table("events", mode="append", retention_policy=RetentionPolicy(max_rows=10000))
    connector = MemoryConnector("events")
    engine.add_connector(connector)

    # Push 10 separate batches of 5 rows each -> 10 chunks created
    for i in range(10):
        connector.push({"id": [i * 5 + j for j in range(5)], "val": [float(i)] * 5})
        engine.step()

    arrow_tbl = table.to_arrow()
    assert arrow_tbl is not None
    # Because auto_compact_chunks=5 and we pushed 10 batches across steps, compaction should have triggered
    assert arrow_tbl.column("id").num_chunks <= 5

    # Test explicit compact_all forcing to 1 chunk
    engine.compact_all(max_chunks=1)
    arrow_tbl_compacted = table.to_arrow()
    assert arrow_tbl_compacted.column("id").num_chunks == 1
    assert arrow_tbl_compacted.num_rows == 50


def test_dictionary_encoding_upsert_and_sql():
    engine = RillEngine(trigger_interval_ms=10.0)

    # Schema with dictionary encoding for status and tier
    s = rill_schema([
        ("user_id", pa.int64()),
        ("tier", pa.dictionary(pa.int8(), pa.string())),
        ("score", pa.float64())
    ], primary_key="user_id", mode="snapshot")

    engine.register_table("users", schema=s)
    connector = MemoryConnector("users", schema=s)
    engine.add_connector(connector)

    # Batch 1: tier dictionary ["bronze", "silver"]
    tier_arr1 = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0], type=pa.int8()),
        pa.array(["bronze", "silver"])
    )
    batch1 = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3], type=pa.int64()), tier_arr1, pa.array([10.0, 20.0, 30.0])],
        names=["user_id", "tier", "score"]
    )
    connector.push(batch1)
    engine.step()

    # Batch 2: tier dictionary ["silver", "gold"] (upsert user 2, add user 4)
    tier_arr2 = pa.DictionaryArray.from_arrays(
        pa.array([0, 1], type=pa.int8()),
        pa.array(["silver", "gold"])
    )
    batch2 = pa.RecordBatch.from_arrays(
        [pa.array([2, 4], type=pa.int64()), tier_arr2, pa.array([25.0, 40.0])],
        names=["user_id", "tier", "score"]
    )
    connector.push(batch2)
    engine.step()

    arrow_tbl = engine.get_table("users").to_arrow()
    assert arrow_tbl.num_rows == 4
    assert pa.types.is_dictionary(arrow_tbl.schema.field("tier").type)

    # Query via DuckDB zero-copy SQL over dictionary column
    res = engine.query_sql("SELECT tier, COUNT(*) as cnt FROM users GROUP BY tier ORDER BY cnt DESC")
    pylist = res.to_pylist()
    # 2 silver (user 2 updated, user 3 bronze, user 1 bronze, user 4 gold)
    # user 1: bronze, user 2: silver, user 3: bronze, user 4: gold -> bronze: 2, silver: 1, gold: 1
    counts = {row["tier"]: row["cnt"] for row in pylist}
    assert counts.get("bronze") == 2
    assert counts.get("silver") == 1
    assert counts.get("gold") == 1


def test_prepared_statement_sql_task():
    engine = RillEngine(trigger_interval_ms=10.0)
    engine.register_table("raw_metrics", mode="append", retention_policy=RetentionPolicy(max_rows=1000))
    connector = MemoryConnector("raw_metrics")
    engine.add_connector(connector)

    task = ScheduledSQLTask(
        name="agg_task",
        query="SELECT metric_name, SUM(value) as total FROM raw_metrics GROUP BY metric_name",
        output_table="metric_totals",
        interval_seconds=0.01,
        use_prepared_statements=True
    )
    engine.sql_tasks.append(task)

    connector.push({"metric_name": ["cpu", "mem", "cpu"], "value": [10.0, 50.0, 15.0]})
    engine.step()
    time.sleep(0.02)
    engine.step()

    # Check prepared statement cache populated in DuckDBBridge
    assert task.query in engine.duckdb._prepared_cache
    assert engine.duckdb._prepared_cache[task.query] is not None

    totals = engine.get_table("metric_totals").to_arrow()
    assert totals is not None
    res_dict = {row["metric_name"]: row["total"] for row in totals.to_pylist()}
    assert res_dict["cpu"] == 25.0
    assert res_dict["mem"] == 50.0


def test_incremental_join_task():
    engine = RillEngine(trigger_interval_ms=10.0)
    engine.register_table("orders", mode="append", retention_policy=RetentionPolicy(max_rows=1000))
    engine.register_table("profiles", mode="snapshot", primary_key="user_id")

    orders_conn = MemoryConnector("orders")
    profiles_conn = MemoryConnector("profiles")
    engine.add_connector(orders_conn)
    engine.add_connector(profiles_conn)

    jtask = TableJoinTask(
        name="join_profiles",
        left_table="orders",
        right_table="profiles",
        keys="user_id",
        output_table="enriched_orders",
        join_type="inner",
        incremental=True
    )
    engine.join_tasks.append(jtask)

    # Initial profiles and orders
    profiles_conn.push({"user_id": [101, 102], "country": ["USA", "UK"]})
    orders_conn.push({"order_id": [1, 2], "user_id": [101, 102], "amount": [100.0, 200.0]})
    engine.step()

    enriched_1 = engine.get_table("enriched_orders").to_arrow()
    assert enriched_1.num_rows == 2
    assert jtask._last_processed_rows == 2

    # Push new orders only (no changes to profiles)
    orders_conn.push({"order_id": [3], "user_id": [101], "amount": [300.0]})
    engine.step()

    enriched_2 = engine.get_table("enriched_orders").to_arrow()
    assert enriched_2.num_rows == 3
    assert jtask._last_processed_rows == 3
    countries = [r["country"] for r in enriched_2.to_pylist()]
    assert countries == ["USA", "UK", "USA"]
