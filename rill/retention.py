"""
Optional Table TTL and Retention Policies for Rill.
Evicts expired or excess records from PyArrow tables using C++ vectorized compute kernels.
"""

import time
from typing import Optional
import pyarrow as pa
import pyarrow.compute as pc


class RetentionPolicy:
    """
    Optional retention policy for a `RillTable`.
    Enforces time-to-live (TTL) row evictions or maximum row count limits during each micro-batch tick.
    By default, time-based TTL (`max_age_seconds`) is governed by the metadata column `z_insert_ts`.
    """

    def __init__(
        self,
        max_rows: Optional[int] = None,
        max_age_seconds: Optional[float] = None,
        time_column: Optional[str] = "z_insert_ts"
    ):
        """
        Args:
            max_rows: Maximum number of rows to retain in memory. If exceeded, oldest rows are sliced off.
            max_age_seconds: Maximum age of rows in seconds. Rows older than this threshold are evicted.
            time_column: Name of the timestamp column used for evaluating `max_age_seconds` (default: 'z_insert_ts').
        """
        self.max_rows = max_rows
        self.max_age_seconds = max_age_seconds
        self.time_column = time_column

    def apply(self, table: Optional[pa.Table], current_time: Optional[float] = None) -> Optional[pa.Table]:
        """
        Applies retention limits onto a PyArrow table in C++ memory using vectorized filters/slices.

        Args:
            table: Input `pa.Table`.
            current_time: Reference timestamp (in seconds since epoch or matching `time_column` units).

        Returns:
            Pruned `pa.Table`.
        """
        if table is None or table.num_rows == 0:
            return table

        if current_time is None:
            current_time = time.time()

        # 1. Apply TTL / time-based eviction
        if self.max_age_seconds is not None and self.time_column in table.column_names:
            col = table.column(self.time_column)
            col_type = col.type

            try:
                if pa.types.is_timestamp(col_type):
                    # Convert current_time cutoff to matching pyarrow timestamp scalar
                    unit = col_type.unit
                    # Convert current_time seconds to unit ticks
                    multipliers = {'s': 1, 'ms': 1_000, 'us': 1_000_000, 'ns': 1_000_000_000}
                    cutoff_ticks = int((current_time - self.max_age_seconds) * multipliers[unit])
                    cutoff_scalar = pa.scalar(cutoff_ticks, type=pa.timestamp(unit, tz=col_type.tz))
                    mask = pc.greater_equal(col, cutoff_scalar)
                else:
                    # Floating or integer unix timestamp column
                    cutoff_val = current_time - self.max_age_seconds
                    if pa.types.is_integer(col_type):
                        cutoff_scalar = pa.scalar(int(cutoff_val), type=col_type)
                    else:
                        cutoff_scalar = pa.scalar(float(cutoff_val), type=col_type)
                    mask = pc.greater_equal(col, cutoff_scalar)

                # Filter table in C++ memory
                table = pc.filter(table, mask)
            except Exception:
                # If comparison fails due to schema/type mismatch, skip time filter safely
                pass

        # 2. Apply max_rows (FIFO slicing)
        if self.max_rows is not None and self.max_rows > 0:
            if table.num_rows > self.max_rows:
                offset = table.num_rows - self.max_rows
                table = table.slice(offset, self.max_rows)

        return table

    def __repr__(self) -> str:
        return f"<RetentionPolicy(max_rows={self.max_rows}, max_age_seconds={self.max_age_seconds}, time_column='{self.time_column}')>"
