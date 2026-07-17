"""
Live demonstration of The Rill Streaming Engine (`rill`).
Simulates continuous high-frequency order events, performs vectorized upserts in PyArrow,
runs scheduled DuckDB SQL transformations, tracks real-time system performance, and sinks outputs.
"""

import time
from pathlib import Path
import pyarrow as pa
from rill import (
    RillEngine,
    MemoryConnector,
    ScheduledSQLTask,
    OutputAPI,
)


def main():
    print("=" * 70)
    print("🚀 Initializing The Rill Streaming Engine...")
    print("=" * 70)

    # 1. Initialize Rill Engine with a 200ms micro-batch trigger
    engine = RillEngine(trigger_interval_ms=200.0)
    output_api = OutputAPI(engine)

    # 2. Register live orders table with primary key 'order_id'
    schema = pa.schema([
        ("order_id", pa.int64()),
        ("user_tier", pa.string()),
        ("amount", pa.float64()),
        ("status", pa.string()),
    ])
    engine.register_table("orders", schema=schema, primary_key="order_id")

    # 3. Attach memory connector feeding events into 'orders'
    connector = MemoryConnector(target_table="orders", schema=schema)
    engine.add_connector(connector)

    # 4. Attach a Scheduled DuckDB SQL Query running every 1.0 second
    # Summarizes revenue and order count by tier
    sql_task = ScheduledSQLTask(
        name="tier_revenue_summary",
        query="""
            SELECT 
                user_tier, 
                SUM(amount) AS total_revenue, 
                COUNT(*) AS order_count,
                AVG(amount) AS avg_ticket
            FROM orders 
            WHERE status != 'cancelled'
            GROUP BY user_tier
        """,
        output_table="tier_revenue",
        interval_seconds=1.0,
        primary_key="user_tier"
    )
    engine.add_sql_task(sql_task)

    # 5. Register custom live business metric formula evaluated every tick
    def max_active_order(eng):
        tbl = eng.get_table("orders")
        if tbl is None or tbl.num_rows == 0:
            return 0.0
        arrow_tbl = tbl.to_arrow()
        import pyarrow.compute as pc
        return pc.max(arrow_tbl.column("amount")).as_py()

    engine.register_business_metric("max_order_amount", max_active_order)

    # 6. Subscribe callback to notifications when 'tier_revenue' table updates
    def on_revenue_updated(tbl_name, arrow_table):
        print(f"\n[Callback Notification] Table '{tbl_name}' updated! Rows: {arrow_table.num_rows}")

    output_api.subscribe_table("tier_revenue", on_revenue_updated)

    # 7. Start Engine Background Loop
    engine.start()
    print("✅ Rill Engine loop started in background (200ms micro-batch interval).")

    # Simulate streaming events
    try:
        print("\n📥 Pushing initial order stream (Gold & Silver tiers)...")
        connector.push({
            "order_id": [1, 2, 3],
            "user_tier": ["Gold", "Silver", "Gold"],
            "amount": [120.50, 45.00, 310.00],
            "status": ["completed", "completed", "completed"],
        })

        time.sleep(1.2)  # Allow micro-batches and 1s SQL task to execute

        print("\n--- Current Live Table: 'tier_revenue' ---")
        revenue_tbl = output_api.get_table("tier_revenue")
        if revenue_tbl is not None:
            for row in revenue_tbl.to_pylist():
                print(f"  Tier: {row['user_tier']:<8} | Revenue: ${row['total_revenue']:<8.2f} | Orders: {row['order_count']}")

        print("\n📥 Pushing upsert & new events (Updating order_id 2 to $99.00, adding order 4)...")
        connector.push({
            "order_id": [2, 4],
            "user_tier": ["Silver", "Platinum"],
            "amount": [99.00, 1500.00],
            "status": ["completed", "completed"],
        })

        time.sleep(1.2)

        print("\n--- Updated Live Table: 'tier_revenue' (via zero-copy DuckDB query) ---")
        revenue_tbl = output_api.get_table("tier_revenue")
        if revenue_tbl is not None:
            for row in revenue_tbl.to_pylist():
                print(f"  Tier: {row['user_tier']:<8} | Revenue: ${row['total_revenue']:<8.2f} | Orders: {row['order_count']}")

        print("\n--- Real-time Engine Metrics & Business Formulas ---")
        all_metrics = output_api.get_all_scalars()
        print(f"  Total Records Processed : {all_metrics['total_records_processed']}")
        print(f"  Avg Micro-Batch Latency : {all_metrics['avg_batch_latency_ms']} ms")
        print(f"  Throughput              : {all_metrics['records_per_second']} records/sec")
        print(f"  Table Row Counts        : {all_metrics['table_row_counts']}")
        print(f"  Max Order Amount (Live) : ${all_metrics['business_metrics']['max_order_amount']}")

        # 8. Test ad-hoc zero-copy DuckDB SQL query
        print("\n🔍 Running ad-hoc DuckDB zero-copy query over 'orders'...")
        high_value = engine.query_sql("SELECT * FROM orders WHERE amount > 200.0 ORDER BY amount DESC")
        for row in high_value.to_pylist():
            print(f"  [High Value Order] ID: {row['order_id']} | Tier: {row['user_tier']} | Amount: ${row['amount']:.2f}")

        # 9. Sink finalized table to disk
        sink_path = Path("/tmp/rill_demo_output/tier_revenue.parquet")
        success = output_api.sink_to_file("tier_revenue", sink_path, format="parquet")
        if success:
            print(f"\n💾 Successfully persisted live table 'tier_revenue' to {sink_path}")

    finally:
        engine.stop()
        print("\n🛑 Rill Engine stopped gracefully.")
        print("=" * 70)


if __name__ == "__main__":
    main()
