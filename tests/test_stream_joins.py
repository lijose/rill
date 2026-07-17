"""
Tests for continuous multi-stream TableJoinTask (joining two streams and aggregating).
"""

import pyarrow as pa
from rill import RillEngine, TableJoinTask


def test_table_join_task_without_aggregation():
    engine = RillEngine()
    
    # Left stream table: orders
    orders_tbl = engine.register_table("orders", primary_key="order_id")
    orders_tbl.upsert(pa.Table.from_pydict({
        "order_id": [101, 102],
        "user_id": [1, 2],
        "amount": [50.0, 120.0]
    }))

    # Right stream table: users
    users_tbl = engine.register_table("users", primary_key="user_id")
    users_tbl.upsert(pa.Table.from_pydict({
        "user_id": [1, 2],
        "tier": ["Gold", "Silver"]
    }))

    # Join task joining orders and users on user_id
    task = TableJoinTask(
        name="enrich_orders",
        left_table="orders",
        right_table="users",
        keys="user_id",
        output_table="orders_enriched",
        join_type="inner"
    )
    engine.add_join_task(task)

    # Execute step
    engine.step()

    enriched = engine.get_table("orders_enriched")
    assert enriched is not None
    assert enriched.num_rows == 2
    
    rows = enriched.to_arrow().to_pylist()
    gold_order = next(r for r in rows if r["user_id"] == 1)
    assert gold_order["tier"] == "Gold"
    assert gold_order["amount"] == 50.0


def test_table_join_task_with_aggregation():
    engine = RillEngine()
    
    orders_tbl = engine.register_table("orders", primary_key="order_id")
    orders_tbl.upsert(pa.Table.from_pydict({
        "order_id": [1, 2, 3],
        "user_id": [10, 20, 10],
        "amount": [100.0, 250.0, 50.0]
    }))

    users_tbl = engine.register_table("users", primary_key="user_id")
    users_tbl.upsert(pa.Table.from_pydict({
        "user_id": [10, 20],
        "region": ["US", "EU"]
    }))

    # Join and aggregate total amount by region
    task = TableJoinTask(
        name="region_revenue",
        left_table="orders",
        right_table="users",
        keys="user_id",
        output_table="region_summary",
        group_by="region",
        aggregations=[("amount", "sum")],
        primary_key="region"
    )
    engine.add_join_task(task)

    engine.step()

    summary = engine.get_table("region_summary")
    assert summary is not None
    assert summary.num_rows == 2

    rows = summary.to_arrow().to_pylist()
    us_total = next(r["amount_sum"] for r in rows if r["region"] == "US")
    eu_total = next(r["amount_sum"] for r in rows if r["region"] == "EU")
    assert us_total == 150.0
    assert eu_total == 250.0
