"""
Tests for the alerts pipeline — record_alert(), rill_alerts table, user push_alert() API,
and severity/source routing.
"""

import time
import duckdb
import pyarrow as pa
from rills.engine import RillEngine
from rills.metrics import MetricsRegistry, create_alerts_table
from rills.connectors.memory import MemoryConnector


def test_record_alert_buffers_entries():
    """record_alert() should buffer alerts internally."""
    registry = MetricsRegistry()
    registry.record_alert("error", "test.source", "Something went wrong", "detail text")
    registry.record_alert("warning", "test.other", "Watch out")

    assert len(registry._alert_buffer) == 2
    assert registry._alert_buffer[0].severity == "error"
    assert registry._alert_buffer[0].source == "test.source"
    assert registry._alert_buffer[0].message == "Something went wrong"
    assert registry._alert_buffer[0].detail == "detail text"
    assert registry._alert_buffer[1].severity == "warning"


def test_record_alert_normalizes_severity():
    """record_alert() should normalize invalid severity to 'info'."""
    registry = MetricsRegistry()
    registry.record_alert("CRITICAL", "test", "bad severity")
    registry.record_alert("  Error  ", "test", "padded severity")
    registry.record_alert("info", "test", "normal")

    assert registry._alert_buffer[0].severity == "info"  # "CRITICAL" -> "info"
    assert registry._alert_buffer[1].severity == "error"  # "  Error  " -> "error"
    assert registry._alert_buffer[2].severity == "info"


def test_record_alert_truncates_detail():
    """record_alert() should truncate detail to 4096 chars."""
    registry = MetricsRegistry()
    long_detail = "x" * 10000
    registry.record_alert("info", "test", "long detail", long_detail)

    assert len(registry._alert_buffer[0].detail) == 4096


def test_create_alerts_table():
    """create_alerts_table() should create the rill_alerts table."""
    con = duckdb.connect(":memory:")
    create_alerts_table(con)

    # Should not raise
    result = con.execute("SELECT * FROM rill_alerts").fetchall()
    assert result == []

    # Verify columns
    desc = con.execute("DESCRIBE rill_alerts").fetchall()
    col_names = [row[0] for row in desc]
    assert "ts" in col_names
    assert "severity" in col_names
    assert "source" in col_names
    assert "message" in col_names
    assert "detail" in col_names
    con.close()


def test_alerts_flushed_to_duckdb():
    """Alerts should be flushed to rill_alerts table during write_to_duckdb()."""
    engine = RillEngine(trigger_interval_ms=1000)

    try:
        # Clear any leftover alerts from previous tests
        engine.metrics_con.execute("DELETE FROM rill_alerts")

        # Record some alerts
        engine.metrics.record_alert("error", "test.component", "Test error", "Error detail")
        engine.metrics.record_alert("warning", "test.other", "Test warning")

        # Flush
        engine.metrics.write_to_duckdb(engine.metrics_con, engine)

        # Query the alerts table
        results = engine.metrics_con.execute(
            "SELECT severity, source, message, detail FROM rill_alerts ORDER BY ts"
        ).fetchall()

        assert len(results) == 2
        assert results[0][0] == "error"
        assert results[0][1] == "test.component"
        assert results[0][2] == "Test error"
        assert results[0][3] == "Error detail"
        assert results[1][0] == "warning"
        assert results[1][1] == "test.other"
    finally:
        engine.stop()


def test_alert_buffer_cleared_after_flush():
    """Alert buffer should be empty after write_to_duckdb()."""
    engine = RillEngine(trigger_interval_ms=1000)

    try:
        engine.metrics.record_alert("info", "test", "Msg 1")
        engine.metrics.record_alert("info", "test", "Msg 2")
        assert len(engine.metrics._alert_buffer) == 2

        engine.metrics.write_to_duckdb(engine.metrics_con, engine)
        assert len(engine.metrics._alert_buffer) == 0
    finally:
        engine.stop()


def test_push_alert_user_api():
    """engine.push_alert() should record alerts with source='user'."""
    engine = RillEngine(trigger_interval_ms=1000)

    try:
        engine.push_alert("Revenue dropped", severity="warning", detail="Below $1000")

        assert len(engine.metrics._alert_buffer) == 1
        entry = engine.metrics._alert_buffer[0]
        assert entry.source == "user"
        assert entry.severity == "warning"
        assert entry.message == "Revenue dropped"
        assert entry.detail == "Below $1000"
    finally:
        engine.stop()


def test_convenience_methods():
    """engine.alert(), engine.warn(), engine.error() should map to correct severities."""
    engine = RillEngine(trigger_interval_ms=1000)

    try:
        engine.alert("Info message")
        engine.warn("Warning message")
        engine.error("Error message")

        assert len(engine.metrics._alert_buffer) == 3
        assert engine.metrics._alert_buffer[0].severity == "info"
        assert engine.metrics._alert_buffer[1].severity == "warning"
        assert engine.metrics._alert_buffer[2].severity == "error"

        # All should have source='user'
        for entry in engine.metrics._alert_buffer:
            assert entry.source == "user"
    finally:
        engine.stop()


def test_connector_error_generates_alert():
    """A failing connector should generate an alert in the engine."""
    engine = RillEngine(trigger_interval_ms=1000)

    class FailingConnector(MemoryConnector):
        def read_batch(self):
            raise RuntimeError("Connector exploded!")

    connector = FailingConnector(target_table="test_tbl")
    engine.add_connector(connector)

    try:
        engine.step()

        # step() flushes alerts to DB via write_to_duckdb(), so check the table
        results = engine.metrics_con.execute(
            "SELECT severity, source, message FROM rill_alerts WHERE severity = 'error'"
        ).fetchall()
        error_alerts = [r for r in results if "connector" in r[1].lower()]
        assert len(error_alerts) >= 1
        assert "Connector exploded!" in error_alerts[0][2]
    finally:
        engine.stop()


def test_alerts_persisted_after_step():
    """Alerts generated during step() should be flushed to the rill_alerts table."""
    engine = RillEngine(trigger_interval_ms=1000)

    class FailingConnector(MemoryConnector):
        def read_batch(self):
            raise ValueError("Bad data!")

    connector = FailingConnector(target_table="broken_tbl")
    engine.add_connector(connector)

    try:
        engine.step()

        # Alerts should be flushed since step() calls write_to_duckdb()
        results = engine.metrics_con.execute(
            "SELECT severity, source, message FROM rill_alerts WHERE severity = 'error'"
        ).fetchall()

        assert len(results) >= 1
        assert any("Bad data!" in row[2] for row in results)
    finally:
        engine.stop()


def test_alert_retention():
    """Alerts older than 1 hour should be pruned during write_to_duckdb()."""
    engine = RillEngine(trigger_interval_ms=1000)

    try:
        # Insert an old alert directly into the table
        old_ts = time.time() - 7200  # 2 hours ago
        engine.metrics_con.execute(
            "INSERT INTO rill_alerts VALUES (?, ?, ?, ?, ?)",
            [old_ts, "info", "old.source", "Old alert", ""]
        )

        # Insert a fresh alert
        engine.metrics.record_alert("info", "new.source", "Fresh alert")
        engine.metrics.write_to_duckdb(engine.metrics_con, engine)

        results = engine.metrics_con.execute("SELECT message FROM rill_alerts").fetchall()
        messages = [r[0] for r in results]

        assert "Fresh alert" in messages
        assert "Old alert" not in messages
    finally:
        engine.stop()


def test_user_alerts_in_duckdb_query():
    """User-pushed alerts should be queryable from rill_alerts."""
    engine = RillEngine(trigger_interval_ms=1000)

    try:
        # Clear any leftover alerts from previous tests
        engine.metrics_con.execute("DELETE FROM rill_alerts")
        engine.push_alert("Payment failed", severity="error", detail="Timeout after 30s")
        engine.push_alert("Low stock", severity="warning")
        engine.alert("Batch processed")

        engine.metrics.write_to_duckdb(engine.metrics_con, engine)

        results = engine.metrics_con.execute(
            "SELECT severity, source, message FROM rill_alerts WHERE source = 'user' ORDER BY ts"
        ).fetchall()

        assert len(results) == 3
        assert results[0] == ("error", "user", "Payment failed")
        assert results[1] == ("warning", "user", "Low stock")
        assert results[2] == ("info", "user", "Batch processed")
    finally:
        engine.stop()
