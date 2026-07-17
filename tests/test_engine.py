"""
Tests for core RillEngine scheduling, step execution, and table update callbacks.
"""

import time
import pyarrow as pa
from rill.engine import RillEngine
from rill.connectors.memory import MemoryConnector


def test_engine_step_and_callbacks():
    engine = RillEngine(trigger_interval_ms=50)
    engine.register_table("events", primary_key="event_id")

    callback_events = []
    def on_events_updated(tbl_name, arrow_table):
        callback_events.append((tbl_name, arrow_table.num_rows))

    engine.subscribe("events", on_events_updated)

    connector = MemoryConnector(target_table="events")
    engine.add_connector(connector)

    # Push two events and run step
    connector.push({"event_id": [1, 2], "type": ["click", "view"]})
    latency_ms = engine.step()

    assert latency_ms >= 0.0
    assert len(callback_events) == 1
    assert callback_events[0] == ("events", 2)

    # Push another event with upsert (event_id=2 updated, event_id=3 added)
    connector.push({"event_id": [2, 3], "type": ["view_updated", "purchase"]})
    engine.step()

    assert len(callback_events) == 2
    assert callback_events[1] == ("events", 3)
    assert engine.get_table("events").num_rows == 3


def test_engine_start_stop_loop():
    engine = RillEngine(trigger_interval_ms=50)
    connector = MemoryConnector(target_table="live_tbl")
    engine.add_connector(connector)

    engine.start()
    try:
        connector.push({"id": [10, 20]})
        time.sleep(0.15)
        assert engine.get_table("live_tbl") is not None
        assert engine.get_table("live_tbl").num_rows == 2
    finally:
        engine.stop()
