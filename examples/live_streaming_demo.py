"""
Live demonstration of The Rill Streaming Engine (`rills`).
Simulates continuous high-frequency order events, performs vectorized upserts in PyArrow,
runs scheduled DuckDB SQL transformations, tracks real-time system performance, and sinks outputs.
"""

import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa
from rills import (
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

    # 6. (Subscription removed per D2 API cleanup)

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

        print("\n--- Current Alerts in Metrics DB ---")
        alerts_df = engine.query_metrics("SELECT ts, severity, message FROM rill_alerts LIMIT 5").to_pylist()
        print(alerts_df)

        print("\n📥 Pushing upsert & new events (Updating order_id 2 to $99.00, adding order 4)...")
        connector.push({
            "order_id": [2, 4],
            "user_tier": ["Silver", "Platinum"],
            "amount": [99.00, 1500.00],
            "status": ["completed", "completed"],
        })

        time.sleep(1.2)

        print("\n--- Updated Alerts in Metrics DB ---")
        alerts_df = engine.query_metrics("SELECT ts, severity, message FROM rill_alerts LIMIT 5").to_pylist()
        print(alerts_df)

        print("\n--- Real-time Engine Metrics & Business Formulas ---")
        all_metrics = output_api.get_all_scalars()
        print(f"  Total Records Processed : {all_metrics['total_records_processed']}")
        print(f"  Avg Micro-Batch Latency : {all_metrics['avg_batch_latency_ms']} ms")
        print(f"  Throughput              : {all_metrics['records_per_second']} records/sec")
        print(f"  Table Row Counts        : {all_metrics['table_row_counts']}")

        # 8. Test ad-hoc zero-copy metrics query
        print("\n🔍 Running ad-hoc DuckDB zero-copy query over 'rill_alerts'...")
        high_value = engine.query_metrics("SELECT * FROM rill_alerts ORDER BY ts DESC LIMIT 2").to_pylist()
        print(high_value)

    finally:
        engine.stop()
        print("\n🛑 Rill Engine stopped gracefully.")
        print("=" * 70)


if __name__ == "__main__":
    main()
