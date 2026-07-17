"""
Tests for system performance metrics and custom business logic evaluation.
"""

import pyarrow as pa
from rill.engine import RillEngine
from rill.connectors.memory import MemoryConnector


def test_system_and_business_metrics():
    engine = RillEngine(trigger_interval_ms=50)
    connector = MemoryConnector(target_table="sales")
    engine.add_connector(connector)

    # Register custom business metric evaluator
    def compute_avg_ticket(eng):
        tbl = eng.get_table("sales")
        if tbl is None or tbl.num_rows == 0:
            return 0.0
        arrow_tbl = tbl.to_arrow()
        import pyarrow.compute as pc
        return pc.mean(arrow_tbl.column("amount")).as_py()

    engine.register_business_metric("avg_ticket", compute_avg_ticket)

    connector.push({"id": [1, 2], "amount": [100.0, 200.0]})
    engine.step()

    metrics = engine.get_metrics()
    assert metrics["total_records_processed"] == 2
    assert metrics["last_batch_records"] == 2
    assert metrics["table_row_counts"]["sales"] == 2
    assert metrics["business_metrics"]["avg_ticket"] == 150.0

    # Push another record and step again
    connector.push({"id": [3], "amount": [300.0]})
    engine.step()

    metrics = engine.get_metrics()
    assert metrics["total_records_processed"] == 3
    assert metrics["last_batch_records"] == 1
    assert metrics["business_metrics"]["avg_ticket"] == 200.0
