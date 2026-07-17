"""
Continuous Multi-Stream Table Join and Aggregation tasks for Rill Engine.
Enables joining two or more live tables produced by separate input streams and deriving
aggregated analytical tables in C++ memory.
"""

from typing import Union, List, Tuple, Optional, TYPE_CHECKING
import pyarrow as pa
from .operations import join_tables, aggregate_table

if TYPE_CHECKING:
    from ..engine import RillEngine


class TableJoinTask:
    """
    Defines a continuous relational join (and optional group-by aggregation) between two live
    stream tables inside `RillEngine`. Executed during each micro-batch tick.
    """

    def __init__(
        self,
        name: str,
        left_table: str,
        right_table: str,
        keys: Union[str, List[str]],
        output_table: str,
        join_type: str = "inner",
        right_keys: Optional[Union[str, List[str]]] = None,
        group_by: Optional[Union[str, List[str]]] = None,
        aggregations: Optional[List[Tuple[str, str]]] = None,
        primary_key: Optional[Union[str, List[str]]] = None
    ):
        """
        Args:
            name: Task identifier.
            left_table: Name of the left `RillTable` (e.g., 'orders').
            right_table: Name of the right `RillTable` (e.g., 'user_profiles').
            keys: Join key column(s) on the left table.
            output_table: Destination `RillTable` where joined/aggregated output is stored.
            join_type: Join strategy ('inner', 'left outer', 'right outer', 'full outer').
            right_keys: Optional join key column(s) on the right table if named differently.
            group_by: Optional column(s) to group by after joining.
            aggregations: Optional list of `(column_name, agg_func)` tuples (e.g. `[("amount", "sum")]`).
            primary_key: Optional primary key for the destination `output_table`.
        """
        self.name = name
        self.left_table = left_table
        self.right_table = right_table
        self.keys = keys
        self.output_table = output_table
        self.join_type = join_type
        self.right_keys = right_keys
        self.group_by = group_by
        self.aggregations = aggregations
        self.primary_key = primary_key

    def execute(self, engine: 'RillEngine') -> Optional[pa.Table]:
        """
        Retrieves the left and right PyArrow tables, executes the C++ relational join kernel,
        applies optional group-by aggregations, and updates `output_table`.
        """
        left_tbl = engine.get_table(self.left_table)
        right_tbl = engine.get_table(self.right_table)

        if left_tbl is None or right_tbl is None:
            return None

        left_arrow = left_tbl.to_arrow()
        right_arrow = right_tbl.to_arrow()

        if left_arrow is None or left_arrow.num_rows == 0 or right_arrow is None or right_arrow.num_rows == 0:
            return None

        try:
            joined = join_tables(
                left=left_arrow,
                right=right_arrow,
                keys=self.keys,
                join_type=self.join_type,
                right_keys=self.right_keys
            )

            if self.aggregations:
                final_table = aggregate_table(
                    table=joined,
                    group_by=self.group_by,
                    aggregations=self.aggregations
                )
            else:
                final_table = joined

            target = engine.get_or_create_table(
                self.output_table,
                schema=final_table.schema,
                primary_key=self.primary_key
            )
            target.replace_state(final_table)
            return final_table
        except Exception as e:
            # Handle schema or join mismatches cleanly during ongoing stream initialization
            return None

    def __repr__(self) -> str:
        return f"<TableJoinTask(name='{self.name}', left='{self.left_table}', right='{self.right_table}', output='{self.output_table}')>"
