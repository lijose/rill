"""
Tests for system performance metrics.
"""

import pyarrow as pa
from rills.engine import RillEngine
from rills.connectors.memory import MemoryConnector


def test_system_metrics():
    engine = RillEngine(trigger_interval_ms=50)
    connector = MemoryConnector(target_table="sales")
    engine.add_connector(connector)

    connector.push({"id": [1, 2], "amount": [100.0, 200.0]})
    engine.step()

    metrics = engine.get_metrics()
    assert metrics["total_records_processed"] == 2
    assert metrics["last_batch_records"] == 2
    assert metrics["table_row_counts"]["sales"] == 2

    # Push another record and step again
    connector.push({"id": [3], "amount": [300.0]})
    engine.step()

    metrics = engine.get_metrics()
    assert metrics["total_records_processed"] == 3
    assert metrics["last_batch_records"] == 1
