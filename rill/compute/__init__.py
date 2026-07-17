"""
Compute core and vectorized operations for Rill.
"""

from .upsert import upsert_table
from .operations import join_tables, filter_table, aggregate_table

__all__ = [
    "upsert_table",
    "join_tables",
    "filter_table",
    "aggregate_table",
]
