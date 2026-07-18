"""
Live Quack Dashboard Demo for The Rill Streaming Engine.
Starts Rill with a background Quack server enabled (`quack:0.0.0.0:9494`),
registers streaming tables and scheduled queries, and continuously pushes
data and allocates PyArrow memory so you can watch live metrics on the web dashboard.
"""

import time
import random
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow as pa
from rill import (
    RillEngine,
    MemoryConnector,
    ScheduledSQLTask,
    OutputAPI,
)


def main():
    print("=" * 70)
    print("🚀 Starting Rill Streaming Engine with Quack Server...")
    print("=" * 70)

    # 1. Initialize Rill Engine with Quack server enabled on port 9494
    engine = RillEngine(
        trigger_interval_ms=500.0,
        quack_address="quack:0.0.0.0:9494",
        quack_token="demo_token"
    )
    output_api = OutputAPI(engine)

    # 2. Register live orders table
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
    sql_task = ScheduledSQLTask(
        name="tier_revenue_summary",
        query="""
            SELECT 
                user_tier, 
                ROUND(SUM(amount), 2) AS total_revenue, 
                COUNT(*) AS order_count,
                ROUND(AVG(amount), 2) AS avg_ticket
            FROM orders 
            WHERE status != 'cancelled'
            GROUP BY user_tier
        """,
        output_table="tier_revenue",
        interval_seconds=1.0,
        primary_key="user_tier"
    )
    engine.add_sql_task(sql_task)

    # 5. Register custom business formulas evaluated every micro-batch tick
    import pyarrow.compute as pc

    def calc_max_order(eng):
        tbl = eng.get_table("orders")
        if not tbl or tbl.num_rows == 0:
            return 0.0
        return round(pc.max(tbl.to_arrow().column("amount")).as_py(), 2)

    def calc_total_revenue(eng):
        tbl = eng.get_table("orders")
        if not tbl or tbl.num_rows == 0:
            return 0.0
        return round(pc.sum(tbl.to_arrow().column("amount")).as_py(), 2)

    def calc_cancelled_ratio(eng):
        tbl = eng.get_table("orders")
        if not tbl or tbl.num_rows == 0:
            return "0.0%"
        arrow_tbl = tbl.to_arrow()
        statuses = arrow_tbl.column("status")
        cancelled_count = pc.sum(pc.cast(pc.equal(statuses, "cancelled"), pa.int64())).as_py()
        pct = (cancelled_count / tbl.num_rows) * 100
        return f"{pct:.1f}%"

    engine.register_business_metric("max_order_amount ($)", calc_max_order)
    engine.register_business_metric("total_revenue ($)", calc_total_revenue)
    engine.register_business_metric("cancelled_ratio", calc_cancelled_ratio)

    # 6. Start Engine Background Loop & Quack Thread
    engine.start()
    print("\n✅ Rill Engine is running!")
    print("📡 Quack Server exposed at: quack:127.0.0.1:9494")
    print("🔑 Quack Auth Token:      demo_token")
    print("\n👉 Next Steps:")
    print("   1. Open the Node.js Dashboard: http://localhost:3000")
    print("   2. In Quack Server Address, enter: quack:127.0.0.1:9494")
    print("   3. In Quack Auth Token, enter:     demo_token")
    print("   4. Watch live charts, metrics, and table histories update every second!\n")
    print("Press Ctrl+C to stop the simulation.\n")

    order_id_counter = 1
    tiers = ["Gold", "Silver", "Platinum", "Bronze"]
    statuses = ["completed", "completed", "completed", "processing", "cancelled"]

    # Keep references to dummy PyArrow arrays to simulate dynamic memory allocation variations
    memory_pool_buffers = []

    try:
        while True:
            # Generate a batch of 5 to 20 random streaming orders
            batch_size = random.randint(5, 20)
            batch_ids = list(range(order_id_counter, order_id_counter + batch_size))
            order_id_counter += batch_size

            connector.push({
                "order_id": batch_ids,
                "user_tier": [random.choice(tiers) for _ in range(batch_size)],
                "amount": [round(random.uniform(10.0, 500.0), 2) for _ in range(batch_size)],
                "status": [random.choice(statuses) for _ in range(batch_size)],
            })

            # Simulate PyArrow memory pool activity (allocating & releasing buffers)
            if len(memory_pool_buffers) > 5:
                memory_pool_buffers.pop(0)  # Release old buffers
            # Allocate a small random PyArrow buffer (0.5MB to 3MB) to show live fluctuations in PyArrow metrics
            dummy_alloc = pa.allocate_buffer(random.randint(500_000, 3_000_000))
            memory_pool_buffers.append(dummy_alloc)

            # Print concise progress to console
            all_metrics = output_api.get_all_scalars()
            records_proc = all_metrics.get("total_records_processed", 0)
            latency = all_metrics.get("avg_batch_latency_ms", 0.0)
            pa_bytes = pa.total_allocated_bytes() / (1024 * 1024)
            print(f"[Streaming Tick] Processed: {records_proc:<6} | Latency: {latency:<5.2f} ms | PyArrow Allocated: {pa_bytes:.2f} MB")

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        engine.stop()
        print("🛑 Rill Engine stopped gracefully.")


if __name__ == "__main__":
    main()
