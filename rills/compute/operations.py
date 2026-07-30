"""
Vectorized relational and analytical operations over PyArrow tables.
"""

from typing import Union, List, Tuple, Optional
import pyarrow as pa
import pyarrow.compute as pc


def join_tables(
    left: pa.Table,
    right: pa.Table,
    keys: Union[str, List[str]],
    join_type: str = "inner",
    right_keys: Optional[Union[str, List[str]]] = None,
    left_suffix: str = "",
    right_suffix: str = "_right"
) -> pa.Table:
    """
    Performs a relational join (`inner`, `left outer`, `right outer`, `full outer`) between
    two PyArrow tables using C++ execution kernels.

    Args:
        left: Left `pa.Table`.
        right: Right `pa.Table`.
        keys: Join key column names on left table.
        join_type: One of 'inner', 'left outer', 'right outer', 'full outer', 'left semi', 'right semi', 'left anti', 'right anti'.
        right_keys: Join key column names on right table (if different from `keys`).
        left_suffix: Suffix appended to overlapping column names from left table.
        right_suffix: Suffix appended to overlapping column names from right table.

    Returns:
        Joined `pa.Table`.
    """
    joined = left.join(
        right,
        keys=keys,
        right_keys=right_keys,
        join_type=join_type,
        left_suffix=left_suffix,
        right_suffix=right_suffix,
        use_threads=True
    )
    drop_cols = [c for c in joined.column_names if c in ("z_insert_ts_right", "z_update_ts_right")]
    if drop_cols:
        joined = joined.drop_columns(drop_cols)
    return joined


def filter_table(
    table: pa.Table,
    mask_or_expression: Union[pa.Array, pa.ChunkedArray, pc.Expression]
) -> pa.Table:
    """
    Filters rows of a PyArrow table based on a boolean mask array or compute expression.

    Args:
        table: Input `pa.Table`.
        mask_or_expression: Boolean Array/ChunkedArray or `pc.Expression`.

    Returns:
        Filtered `pa.Table`.
    """
    return pc.filter(table, mask_or_expression)


def aggregate_table(
    table: pa.Table,
    group_by: Optional[Union[str, List[str]]],
    aggregations: List[Tuple[str, str]]
) -> pa.Table:
    """
    Performs group-by or global aggregations using PyArrow compute kernels.

    Args:
        table: Input `pa.Table`.
        group_by: Column name or list of column names to group by. If None/empty, computes global aggregation.
        aggregations: List of tuples `(column_name, agg_function_name)` (e.g., `[("amount", "sum"), ("id", "count")]`).

    Returns:
        Aggregated `pa.Table`.
    """
    if group_by:
        if isinstance(group_by, str):
            group_by = [group_by]
        return table.group_by(group_by).aggregate(aggregations)
    else:
        # Global aggregation without grouping
        # table.group_by([]) supported in pyarrow >= 13.0
        return table.group_by([]).aggregate(aggregations)
