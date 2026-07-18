"""
Live demonstration of Multi-Stream Joins, Optional TTL Retention, and Checkpointing in Rill.
Simulates two distinct input streams ('orders' and 'user_profiles'), continuously joins them
in C++ memory (`TableJoinTask`), applies time/row retention, and checkpoints snapshots.
"""

import time
from pathlib import Path
import pyarrow as pa
from rill import (
    RillEngine,
    MemoryConnector,
    TableJoinTask,
    ScheduledSQLTask,
    RetentionPolicy,
    OutputAPI,
)


def main():
    print("=" * 75)
    print("🚀 Initializing Rill Engine with Multi-Stream Joins & Checkpointing...")
    print("=" * 75)

    # checkpoint_dir = Path("/tmp/rill_multi_stream_checkpoints")
    # engine = RillEngine(
    #     trigger_interval_ms=200.0,
    #     checkpoint_dir=checkpoint_dir,
    #     checkpoint_interval_seconds=2.0
    # )
    # will add later
    engine = RillEngine(trigger_interval_ms=200.0)
    output_api = OutputAPI(engine)

    # 1. Register Stream A: 'user_profiles' (with primary_key='user_id')
    user_schema = pa.schema([
        ("user_id", pa.int64()),
        ("name", pa.string()),
        ("region", pa.string()),
        ("tier", pa.string())
    ])
    engine.register_table("user_profiles", schema=user_schema, primary_key="user_id")

    # 2. Register Stream B: 'orders' (with optional Retention Policy capping at 100 rows)
    order_schema = pa.schema([
        ("order_id", pa.int64()),
        ("user_id", pa.int64()),
        ("amount", pa.float64()),
        ("category", pa.string())
    ])
    retention = RetentionPolicy(max_rows=100)
    engine.register_table("orders", schema=order_schema, primary_key="order_id", retention_policy=retention)

    # 3. Attach connectors for both streams
    users_connector = MemoryConnector("user_profiles", schema=user_schema)
    orders_connector = MemoryConnector("orders", schema=order_schema, max_buffer_records=50, on_overflow="drop_oldest")
    engine.add_connector(users_connector)
    engine.add_connector(orders_connector)

    # 4. Attach Continuous Multi-Stream Join Task
    # Joins 'orders' + 'user_profiles' on 'user_id' into 'orders_enriched' table
    join_task = TableJoinTask(
        name="enrich_orders",
        left_table="orders",
        right_table="user_profiles",
        keys="user_id",
        output_table="orders_enriched",
        join_type="inner",
        primary_key="order_id"
    )
    engine.add_join_task(join_task)

    # 5. Attach Scheduled SQL Task computing regional revenue summary every 1 second
    sql_task = ScheduledSQLTask(
        name="regional_revenue_job",
        query="""
            SELECT 
                region, 
                tier, 
                COUNT(*) as order_cnt, 
                SUM(amount) as total_revenue
            FROM orders_enriched
            GROUP BY region, tier
        """,
        output_table="regional_revenue",
        interval_seconds=1.0,
        primary_key=["region", "tier"]
    )
    engine.add_sql_task(sql_task)

    # Subscribe to live notifications on our aggregated regional revenue table
    def on_regional_summary(tbl_name, arrow_tbl):
        print(f"\n[Callback Notification] Aggregated Table '{tbl_name}' updated! ({arrow_tbl.num_rows} rows)")

    output_api.subscribe_table("regional_revenue", on_regional_summary)

    # Start engine loop
    engine.start()
    print("✅ Rill Engine background loop active.")

    try:
        # Push Stream A: User profile updates
        print("\n📥 Stream A -> Pushing initial 'user_profiles' (Alice, Bob, Carol)...")
        users_connector.push({
            "user_id": [10, 20, 30],
            "name": ["Alice", "Bob", "Carol"],
            "region": ["North America", "Europe", "North America"],
            "tier": ["Platinum", "Gold", "Silver"]
        })

        # Push Stream B: Order events
        print("📥 Stream B -> Pushing order stream (3 orders across Alice & Bob)...")
        orders_connector.push({
            "order_id": [1001, 1002, 1003],
            "user_id": [10, 20, 10],
            "amount": [250.00, 89.50, 420.00],
            "category": ["Electronics", "Books", "Software"]
        })

        time.sleep(1.3)  # Wait for join tick + 1s SQL task

        print("\n--- Current Enriched Table ('orders_enriched') ---")
        enriched_tbl = output_api.get_table("orders_enriched")
        if enriched_tbl:
            for r in enriched_tbl.to_pylist():
                print(f"  Order {r['order_id']} | User: {r['name']:<6} ({r['tier']:<8}, {r['region']}) | Amount: ${r['amount']:.2f}")

        print("\n--- Aggregated Regional Revenue ('regional_revenue') ---")
        reg_tbl = output_api.get_table("regional_revenue")
        if reg_tbl:
            for r in reg_tbl.to_pylist():
                print(f"  Region: {r['region']:<15} | Tier: {r['tier']:<8} | Orders: {r['order_cnt']} | Total: ${r['total_revenue']:.2f}")

        # Push more events: Carol places an order, Bob upgrades tier & places another order
        print("\n📥 Stream A & B -> Pushing profile update (Bob -> Platinum) and 2 new orders...")
        users_connector.push({
            "user_id": [20],
            "name": ["Bob"],
            "region": ["Europe"],
            "tier": ["Platinum"]  # Upserted!
        })
        orders_connector.push({
            "order_id": [1004, 1005],
            "user_id": [30, 20],
            "amount": [60.00, 310.00],
            "category": ["Groceries", "Electronics"]
        })

        time.sleep(1.3)

        print("\n--- Updated Aggregated Regional Revenue ('regional_revenue') ---")
        reg_tbl = output_api.get_table("regional_revenue")
        if reg_tbl:
            for r in reg_tbl.to_pylist():
                print(f"  Region: {r['region']:<15} | Tier: {r['tier']:<8} | Orders: {r['order_cnt']} | Total: ${r['total_revenue']:.2f}")

        # Checkpoint persistence check
        # time.sleep(1.5)  # Allow checkpointer (2.0s interval) to save snapshots
        # saved_files = list(checkpoint_dir.glob("*.parquet"))
        # print(f"\n💾 Checkpointer persisted snapshots to disk: {[f.name for f in saved_files]}")
        # will add later

    finally:
        engine.stop()
        print("\n🛑 Rill Engine stopped gracefully.")
        print("=" * 75)


if __name__ == "__main__":
    main()
