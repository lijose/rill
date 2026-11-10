import time
import os
import pytest
import pyarrow as pa
import duckdb
from rills.engine import RillEngine


def test_quack_metrics_registration():
    # 1. Test that RillEngine correctly starts and registers the rill_metrics table
    engine = RillEngine(trigger_interval_ms=50)
    engine.step()
    
    # Query rill_metrics table directly from the local DuckDB bridge
    metrics_tbl = engine.duckdb.query("SELECT * FROM rill_metrics")
    assert metrics_tbl.num_rows == 1
    
    # Verify that the schema matches what we expect
    schema_names = metrics_tbl.schema.names
    assert "timestamp" in schema_names
    assert "cpu_usage" in schema_names
    assert "total_memory" in schema_names
    assert "pyarrow_allocated_bytes" in schema_names
    assert "pyarrow_max_memory" in schema_names


def test_quack_server_startup_and_stop(monkeypatch):
    # Exercise connection ownership and readiness without downloading extensions.
    def serve(self):
        assert self._quack_con is not self.metrics_con
        assert self._quack_con.execute("SELECT 1").fetchone() == (1,)
        self.quack_ready.set()
    monkeypatch.setattr(RillEngine, "_run_quack_server", serve)
    engine = RillEngine(quack_address="quack:127.0.0.1:9499")
    engine.start()
    assert engine.quack_ready.wait(2)
    assert engine.last_error is None
    # The fake server does not own an extension server to stop.
    engine.quack_ready.clear()
    engine.stop()
    assert engine._quack_thread is None


@pytest.mark.integration
@pytest.mark.skipif(os.environ.get("RILL_TEST_QUACK") != "1", reason="Set RILL_TEST_QUACK=1 for real Quack integration")

def test_quack_client_connection(tmp_path):
    import socket
    extension_dir = str(tmp_path / "extensions")
    with duckdb.connect(config={"extension_directory": extension_dir}) as con:
        loaded = False
        errors = []
        for repository in ("", " FROM community", " FROM core_nightly"):
            try:
                con.execute("INSTALL quack" + repository)
                con.execute("LOAD quack")
                loaded = True
                break
            except duckdb.Error as exc:
                errors.append(str(exc))
        if not loaded:
            pytest.fail("Quack extension unavailable: " + errors[-1])
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    address = f"quack:127.0.0.1:{port}"
    engine = RillEngine(quack_address=address, quack_token="integration-test-token")
    engine.metrics_con.execute("SET extension_directory = ?", [extension_dir])
    engine.start()
    try:
        assert engine.quack_ready.wait(15), str(engine.quack_error)
        with duckdb.connect(config={"extension_directory": extension_dir}) as client:
            client.execute("LOAD quack")
            client.execute(f"ATTACH '{address}' AS remote (TOKEN 'integration-test-token')")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                rows = client.execute("SELECT * FROM remote.rill_metrics").fetchall()
                if rows:
                    break
                time.sleep(.05)
            assert rows
            assert engine.last_error is None
    finally:
        engine.stop(timeout=15)
    assert engine._closed
