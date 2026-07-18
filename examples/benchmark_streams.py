"""
Rill Engine Benchmark: Multi-Stream Impressions & Clicks (1K, 10K, 1M Products)

Evaluates performance and identifies bottlenecks in high-frequency streaming analytics across:
  Stream 1 (impressions): product_id, account_id, impression_id, system_ts
  Stream 2 (clicks):      product_id, account_id, click_impression_id, system_ts
  (Where product_id, account_id, and impression_id / click_impression_id match between streams)

Metrics Evaluated:
  - Top 10 products based on impressions in the last 10 minutes
  - Top 10 products based on clicks in the last 10 minutes

Benchmark Configurations:
  - 1,000 distinct products
  - 10,000 distinct products
  - 1,000,000 (1M) distinct products
"""

import argparse
import time
import gc
import sys
from pathlib import Path
from typing import Dict, Any, List
import pyarrow as pa
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from rill import (
    RillEngine,
    MemoryConnector,
    RetentionPolicy,
    schema as rill_schema,
)

def generate_stream_batches(
    num_products: int,
    total_impressions: int,
    click_ratio: float = 0.15,
    num_batches: int = 5
) -> List[Dict[str, Dict[str, np.ndarray]]]:
    """
    Generates realistic synthetic batches for both 'impressions' and 'clicks' streams using NumPy.
    Ensures that for clicked events, product_id, account_id, and impression_id (click_impression_id)
    match exactly between Stream 1 and Stream 2.
    """
    batches = []
    batch_imp_size = total_impressions // num_batches
    batch_click_size = int(batch_imp_size * click_ratio)

    # Base timestamp set to current unix time
    base_ts = time.time() - 300.0  # Distributed across the last 5 minutes (within 10min window)

    # Pre-generate product weights (Zipf-like distribution so some products get more impressions/clicks)
    weights = 1.0 / (np.arange(1, num_products + 1) ** 0.5)
    weights /= weights.sum()

    start_imp_id = 100_000_000
    for b in range(num_batches):
        # Generate Stream 1: Impressions
        prod_ids = np.random.choice(np.arange(1, num_products + 1), size=batch_imp_size, p=weights).astype(np.int64)
        # Account IDs range across 10,000 accounts
        acct_ids = np.random.randint(1, 10_001, size=batch_imp_size, dtype=np.int64)
        imp_ids = np.arange(start_imp_id, start_imp_id + batch_imp_size, dtype=np.int64)
        start_imp_id += batch_imp_size
        
        # Random timestamps within the window
        ts_offsets = np.random.uniform(0, 300.0, size=batch_imp_size)
        imp_ts = (base_ts + ts_offsets).astype(np.float64)

        # Generate Stream 2: Clicks (subset of impressions from this batch)
        click_indices = np.random.choice(batch_imp_size, size=batch_click_size, replace=False)
        click_prod_ids = prod_ids[click_indices]
        click_acct_ids = acct_ids[click_indices]
        click_imp_ids = imp_ids[click_indices]
        # Click occurs slightly after impression (0.1 to 5.0 seconds later)
        click_ts = imp_ts[click_indices] + np.random.uniform(0.1, 5.0, size=batch_click_size)

        batches.append({
            "impressions": {
                "product_id": prod_ids.tolist(),
                "account_id": acct_ids.tolist(),
                "impression_id": imp_ids.tolist(),
                "system_ts": imp_ts.tolist()
            },
            "clicks": {
                "product_id": click_prod_ids.tolist(),
                "account_id": click_acct_ids.tolist(),
                "click_impression_id": click_imp_ids.tolist(),
                "system_ts": click_ts.tolist()
            }
        })

    return batches


def run_benchmark(
    num_products: int,
    total_impressions: int,
    num_batches: int = 5,
    threads: int = 4,
    auto_compact_chunks: int = 32,
    use_dictionary: bool = False,
    use_prepared_statements: bool = True,
    use_incremental_join: bool = False
) -> Dict[str, Any]:
    """
    Runs a complete benchmark iteration for a specified product pool size, capturing
    ingestion throughput, DuckDB zero-copy SQL latency, memory allocation, and TTL pruning costs.
    """
    print("\n" + "=" * 80)
    opt_str = " | OPTIMIZED" if (use_dictionary or auto_compact_chunks > 0 or use_prepared_statements or use_incremental_join) else " | BASELINE"
    print(f"🚀 RUNNING BENCHMARK: {num_products:,} Products | {total_impressions:,} Impressions{opt_str}")
    print(f"   [Config: threads={threads}, compact_chunks={auto_compact_chunks}, dictionary={use_dictionary}, prepared_sql={use_prepared_statements}, incremental_join={use_incremental_join}]")
    print("=" * 80)

    # Clean memory before run
    gc.collect()
    pa.total_allocated_bytes()

    # 1. Initialize Rill Engine with optimization parameters
    engine = RillEngine(trigger_interval_ms=100.0, auto_compact_chunks=auto_compact_chunks)
    if threads and engine.duckdb.con:
        try:
            engine.duckdb.con.execute(f"PRAGMA threads={threads}")
        except Exception:
            pass
    engine.duckdb.use_prepared_statements = use_prepared_statements

    # 2. Define schemas (with optional dictionary encoding for categorical columns like product_id)
    prod_type = pa.dictionary(pa.int32(), pa.int64()) if use_dictionary else pa.int64()
    imp_schema = rill_schema([
        ("product_id", prod_type),
        ("account_id", pa.int64()),
        ("impression_id", pa.int64()),
        ("system_ts", pa.float64())
    ], mode="append")

    click_schema = rill_schema([
        ("product_id", prod_type),
        ("account_id", pa.int64()),
        ("click_impression_id", pa.int64()),
        ("system_ts", pa.float64())
    ], mode="append")

    # 3. Define 10-minute (600s) Retention Policy governed by system_ts
    retention = RetentionPolicy(max_age_seconds=600.0, time_column="system_ts")

    # Register tables
    engine.register_table("impressions", schema=imp_schema, retention_policy=retention, mode="append")
    engine.register_table("clicks", schema=click_schema, retention_policy=retention, mode="append")

    # Attach connectors
    imp_connector = MemoryConnector("impressions", schema=imp_schema)
    click_connector = MemoryConnector("clicks", schema=click_schema)
    engine.add_connector(imp_connector)
    engine.add_connector(click_connector)

    # If incremental join requested, register stream join task
    if use_incremental_join:
        from rill.compute.join import TableJoinTask
        jtask = TableJoinTask(
            name="imp_click_join",
            left_table="clicks",
            right_table="impressions",
            keys=["product_id", "account_id"],
            output_table="joined_events",
            join_type="inner",
            incremental=True
        )
        engine.join_tasks.append(jtask)

    # 4. Generate data batches
    print(f"📦 Generating synthetic streams ({num_batches} batches across {num_products:,} distinct products)...")
    t_gen_start = time.perf_counter()
    batches = generate_stream_batches(num_products, total_impressions, click_ratio=0.15, num_batches=num_batches)
    t_gen_end = time.perf_counter()
    print(f"   Done generating batches in {(t_gen_end - t_gen_start)*1000:.1f} ms.")

    total_imp_rows = sum(len(b["impressions"]["product_id"]) for b in batches)
    total_click_rows = sum(len(b["clicks"]["product_id"]) for b in batches)
    total_records = total_imp_rows + total_click_rows

    # 5. Measure Ingestion & Vectorized Append Bottleneck
    print(f"📥 Ingesting {total_records:,} total events across {num_batches} micro-batches into PyArrow C++ tables...")
    t_ingest_start = time.perf_counter()
    for batch in batches:
        imp_data = batch["impressions"]
        click_data = batch["clicks"]
        if use_dictionary:
            # Encode product_ids as dictionary arrays
            imp_prod = pa.DictionaryArray.from_arrays(
                pa.array(imp_data["product_id"]).dictionary_encode().indices.cast(pa.int32()),
                pa.array(imp_data["product_id"]).dictionary_encode().dictionary.cast(pa.int64())
            )
            imp_batch = pa.RecordBatch.from_arrays(
                [imp_prod, pa.array(imp_data["account_id"]), pa.array(imp_data["impression_id"]), pa.array(imp_data["system_ts"])],
                names=["product_id", "account_id", "impression_id", "system_ts"]
            )
            click_prod = pa.DictionaryArray.from_arrays(
                pa.array(click_data["product_id"]).dictionary_encode().indices.cast(pa.int32()),
                pa.array(click_data["product_id"]).dictionary_encode().dictionary.cast(pa.int64())
            )
            click_batch = pa.RecordBatch.from_arrays(
                [click_prod, pa.array(click_data["account_id"]), pa.array(click_data["click_impression_id"]), pa.array(click_data["system_ts"])],
                names=["product_id", "account_id", "click_impression_id", "system_ts"]
            )
            imp_connector.push(imp_batch)
            click_connector.push(click_batch)
        else:
            imp_connector.push(imp_data)
            click_connector.push(click_data)
        engine.step()  # Executes micro-batch ingestion and upserts
    t_ingest_end = time.perf_counter()

    ingest_time_ms = (t_ingest_end - t_ingest_start) * 1000.0
    throughput_rps = (total_records / ingest_time_ms) * 1000.0 if ingest_time_ms > 0 else 0.0

    print(f"   ✅ Ingestion Complete: {ingest_time_ms:.2f} ms | Throughput: {throughput_rps:,.0f} records/sec")

    # 6. Measure DuckDB Zero-Copy SQL Aggregation Bottleneck (Top 10 in Last 10 Minutes)
    now_ts = time.time()
    cutoff_ts = now_ts - 600.0  # 10 minutes window

    sql_impressions = f"""
        SELECT 
            product_id, 
            COUNT(*) AS impression_count 
        FROM impressions 
        WHERE system_ts >= {cutoff_ts} 
        GROUP BY product_id 
        ORDER BY impression_count DESC 
        LIMIT 10
    """

    sql_clicks = f"""
        SELECT 
            product_id, 
            COUNT(*) AS click_count 
        FROM clicks 
        WHERE system_ts >= {cutoff_ts} 
        GROUP BY product_id 
        ORDER BY click_count DESC 
        LIMIT 10
    """

    print("\n🔍 Executing DuckDB Zero-Copy SQL Window Queries (`WHERE system_ts >= now - 10m`)...")
    
    # Query Top 10 Impressions
    t_sql_imp_start = time.perf_counter()
    top_impressions = engine.query_sql(sql_impressions)
    t_sql_imp_end = time.perf_counter()
    sql_imp_ms = (t_sql_imp_end - t_sql_imp_start) * 1000.0

    # Query Top 10 Clicks
    t_sql_click_start = time.perf_counter()
    top_clicks = engine.query_sql(sql_clicks)
    t_sql_click_end = time.perf_counter()
    sql_click_ms = (t_sql_click_end - t_sql_click_start) * 1000.0

    print(f"   ⚡ Top 10 Impressions Query Latency : {sql_imp_ms:.2f} ms")
    print(f"   ⚡ Top 10 Clicks Query Latency      : {sql_click_ms:.2f} ms")

    # Display Top 10 Results
    imp_rows = top_impressions.to_pylist() if top_impressions else []
    click_rows = top_clicks.to_pylist() if top_clicks else []

    print("\n   🏆 [Top 10 Products by Impressions in Last 10m]")
    print("      +------------+--------------------+")
    print("      | Product ID |   Impression Count |")
    print("      +------------+--------------------+")
    for r in imp_rows[:10]:
        print(f"      | {r['product_id']:<10} | {r['impression_count']:>18,} |")
    print("      +------------+--------------------+")

    print("\n   🏆 [Top 10 Products by Clicks in Last 10m]")
    print("      +------------+--------------------+")
    print("      | Product ID |        Click Count |")
    print("      +------------+--------------------+")
    for r in click_rows[:10]:
        print(f"      | {r['product_id']:<10} | {r['click_count']:>18,} |")
    print("      +------------+--------------------+")

    # 7. Measure Vectorized TTL Retention Pruning Bottleneck
    print("\n🧹 Measuring Vectorized TTL Pruning Overhead (C++ pyarrow.compute.filter over all rows)...")
    t_ttl_start = time.perf_counter()
    engine.get_table("impressions").apply_retention(current_time=now_ts)
    engine.get_table("clicks").apply_retention(current_time=now_ts)
    t_ttl_end = time.perf_counter()
    ttl_prune_ms = (t_ttl_end - t_ttl_start) * 1000.0
    print(f"   🧹 TTL Retention Pruning Latency: {ttl_prune_ms:.2f} ms across {total_records:,} rows")

    # 8. Measure PyArrow C++ Memory Footprint
    allocated_bytes = pa.total_allocated_bytes()
    allocated_mb = allocated_bytes / (1024 * 1024)
    bytes_per_row = allocated_bytes / total_records if total_records > 0 else 0

    print(f"\n💾 PyArrow C++ Memory Allocation : {allocated_mb:.2f} MB ({bytes_per_row:.1f} bytes/record)")

    return {
        "num_products": num_products,
        "total_records": total_records,
        "ingest_time_ms": ingest_time_ms,
        "throughput_rps": throughput_rps,
        "sql_imp_ms": sql_imp_ms,
        "sql_click_ms": sql_click_ms,
        "ttl_prune_ms": ttl_prune_ms,
        "allocated_mb": allocated_mb
    }


def print_summary_report(results: List[Dict[str, Any]]) -> None:
    """
    Prints a formatted comparative bottleneck analysis summary across all evaluated scales.
    """
    print("\n" + "=" * 100)
    print("📊 RILL ENGINE BOTTLENECK ANALYSIS SUMMARY REPORT")
    print("=" * 100)
    print(f"{'Products Pool':<16} | {'Total Records':<14} | {'Ingest (ms)':<12} | {'Throughput (RPS)':<18} | {'Top 10 Imp SQL':<15} | {'Top 10 Clk SQL':<15} | {'TTL Prune':<10}")
    print("-" * 100)
    for r in results:
        print(
            f"{r['num_products']:<16,}"
            f" | {r['total_records']:<14,}"
            f" | {r['ingest_time_ms']:<12.1f}"
            f" | {r['throughput_rps']:<18,.0f}"
            f" | {r['sql_imp_ms']:<12.2f} ms"
            f" | {r['sql_click_ms']:<12.2f} ms"
            f" | {r['ttl_prune_ms']:<8.2f} ms"
        )
    print("=" * 100)
    print("\n💡 Key Bottleneck & Scalability Insights:")
    print("  1. DuckDB Zero-Copy Aggregation (Cardinality Scaling):")
    print("     Notice how query latency scales as distinct `product_id` keys increase from 1K to 1M.")
    print("     Because DuckDB queries live PyArrow memory directly via zero-copy C++ pointers, hash table")
    print("     aggregation scales logarithmically/linearly with cardinality without Python object deserialization.")
    print("\n  2. Ingestion & Vectorized Append Throughput:")
    print("     In append-only mode (`mode='append'`), Rill appends chunks directly to the PyArrow Table.")
    print("     Throughput remains consistently high across millions of events because no primary key index checks are required.")
    print("\n  3. Vectorized TTL Retention Pruning (`system_ts`):")
    print("     Time-to-live eviction uses `pyarrow.compute.filter` on C++ contiguous memory.")
    print("     Evaluating retention over millions of rows completes in just a few milliseconds.")
    print("=" * 100 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Rill Multi-Stream Benchmark: Impressions & Clicks across 1K, 10K, and 1M products")
    parser.add_argument("--products", type=int, choices=[1000, 10000, 1000000], help="Run benchmark for a specific product count only")
    parser.add_argument("--impressions", type=int, default=None, help="Override total impressions generated per run")
    parser.add_argument("--optimize", action="store_true", help="Enable all recommended optimizations (compaction, threads=4, dictionary, prepared statements)")
    parser.add_argument("--threads", type=int, default=4, help="DuckDB execution threads (default: 4)")
    parser.add_argument("--compact-chunks", type=int, default=32, help="Auto-compaction threshold (default: 32)")
    parser.add_argument("--dictionary", action="store_true", help="Use PyArrow dictionary encoding for product_id")
    parser.add_argument("--prepared-statements", action="store_true", default=True, help="Use prepared statement caching (default: True)")
    parser.add_argument("--incremental-join", action="store_true", help="Enable incremental micro-batch join between impressions and clicks")
    args = parser.parse_args()

    # If --optimize passed, enable all optimizations
    if args.optimize:
        args.dictionary = True
        args.compact_chunks = 16
        args.threads = 4
        args.prepared_statements = True

    if args.products:
        # Run specific requested product count
        imp_count = args.impressions if args.impressions else max(100_000, args.products)
        run_benchmark(
            num_products=args.products,
            total_impressions=imp_count,
            threads=args.threads,
            auto_compact_chunks=args.compact_chunks,
            use_dictionary=args.dictionary,
            use_prepared_statements=args.prepared_statements,
            use_incremental_join=args.incremental_join
        )
    else:
        # Run full benchmark across 1K, 10K, and 1M products
        configs = [
            (1_000, args.impressions if args.impressions else 100_000),
            (10_000, args.impressions if args.impressions else 250_000),
            (1_000_000, args.impressions if args.impressions else 1_000_000)
        ]
        results = []
        for prod_cnt, imp_cnt in configs:
            res = run_benchmark(
                num_products=prod_cnt,
                total_impressions=imp_cnt,
                threads=args.threads,
                auto_compact_chunks=args.compact_chunks,
                use_dictionary=args.dictionary,
                use_prepared_statements=args.prepared_statements,
                use_incremental_join=args.incremental_join
            )
            results.append(res)

        print_summary_report(results)


if __name__ == "__main__":
    main()
