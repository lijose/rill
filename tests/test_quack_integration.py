import time
import pytest
import pyarrow as pa
import duckdb
from rill.engine import RillEngine


def test_quack_metrics_registration():
    # 1. Test that RillEngine correctly starts and registers the rill_metrics table
    engine = RillEngine(trigger_interval_ms=50)
    engine.step()
    
    # Query rill_metrics table directly from the local DuckDB bridge
    metrics_tbl = engine.query_sql("SELECT * FROM rill_metrics")
    assert metrics_tbl.num_rows == 1
    
    # Verify that the schema matches what we expect
    schema_names = metrics_tbl.schema.names
    assert "timestamp" in schema_names
    assert "cpu_usage" in schema_names
    assert "total_memory" in schema_names
    assert "pyarrow_allocated_bytes" in schema_names
    assert "pyarrow_max_memory" in schema_names
    assert "business_metrics" in schema_names


def test_quack_server_startup_and_stop():
    # 2. Test that RillEngine correctly initializes the Quack server background thread
    engine = RillEngine(quack_address="quack:127.0.0.1:9499")
    
    # Start the engine
    engine.start()
    time.sleep(0.5)
    
    # Check if the thread was created and started
    assert engine._quack_thread is not None
    import threading
    assert isinstance(engine._quack_thread, threading.Thread)
        
    # Stop the engine and verify it cleans up
    engine.stop()
    assert engine._quack_thread is None


def test_quack_client_connection():
    # 3. Attempt to load quack extension to see if it is available locally
    con = duckdb.connect()
    try:
        con.execute("INSTALL quack;")
        con.execute("LOAD quack;")
        quack_available = True
    except Exception:
        try:
            con.execute("INSTALL quack FROM core_nightly;")
            con.execute("LOAD quack;")
            quack_available = True
        except Exception:
            quack_available = False
            
    if not quack_available:
        pytest.skip("Quack extension not available on this platform/version of DuckDB")
        
    # If quack is available, run end-to-end server/client test
    engine = RillEngine(quack_address="quack:127.0.0.1:9498")
    engine.start()
    time.sleep(1.0)
    
    try:
        client_con = duckdb.connect()
        client_con.execute("LOAD quack;")
        client_con.execute("ATTACH 'quack:127.0.0.1:9498' AS remote;")
        res = client_con.execute("SELECT * FROM remote.rill_metrics").fetchall()
        assert len(res) > 0
    finally:
        engine.stop()
