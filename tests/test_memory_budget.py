"""
Tests for RillEngine memory budget monitoring and warning emission.
"""

import pytest
import pyarrow as pa
from rill import RillEngine


def test_memory_budget_warning_and_callback():
    warnings_emitted = []
    callbacks_triggered = []

    def on_warning(allocated, budget):
        callbacks_triggered.append((allocated, budget))

    # Initialize engine with a very small memory budget (10 bytes)
    engine = RillEngine(memory_budget_bytes=10, on_memory_warning=on_warning)
    tbl = engine.register_table("heavy_table")

    # Insert data exceeding 10 bytes
    tbl.upsert(pa.Table.from_pydict({
        "col": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    }))

    with pytest.warns(ResourceWarning) as record:
        allocated, budget, exceeded = engine.check_memory_budget()
        assert exceeded is True
        assert budget == 10
        assert allocated > 10

    assert len(record) > 0
    assert "RillEngine memory budget exceeded!" in str(record[0].message)
    assert len(callbacks_triggered) == 1
    assert callbacks_triggered[0][1] == 10
