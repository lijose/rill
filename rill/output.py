"""
Output API for Rill Streaming Engine.
Exposes finalized PyArrow tables, scalar values, callback subscriptions, and data sinks
for integration with frontend dashboards (Streamlit, Tkinter) and downstream consumers.
"""

from pathlib import Path
from typing import Optional, Dict, Any, Callable, Union, TYPE_CHECKING
import pyarrow as pa
import pyarrow.parquet as papq
import pyarrow.csv as pacsv
import pyarrow.json as pajson

if TYPE_CHECKING:
    from .engine import RillEngine


class OutputAPI:
    """
    Exposes clean accessors and callback mechanisms to extract live tables and scalar
    metrics from a running `RillEngine`.
    """

    def __init__(self, engine: 'RillEngine'):
        self.engine = engine

    def get_table(self, table_name: str) -> Optional[pa.Table]:
        """
        Returns a zero-copy reference to the finalized `pa.Table` in C++ memory.
        """
        table = self.engine.get_table(table_name)
        if table is not None:
            return table.to_arrow()
        return None

    def get_pandas(self, table_name: str) -> Any:
        """
        Returns the table as a pandas DataFrame for instant rendering in Streamlit or Tkinter.
        """
        arrow_table = self.get_table(table_name)
        if arrow_table is not None:
            return arrow_table.to_pandas()
        import pandas as pd
        return pd.DataFrame()

    def subscribe_table(self, table_name: str, callback: Callable[[str, pa.Table], None]) -> None:
        """
        Registers a callback invoked immediately whenever `table_name` is updated.
        """
        self.engine.subscribe(table_name, callback)

    def get_scalar(self, metric_name: str, default: Any = None) -> Any:
        """
        Retrieves a specific scalar metric (system KPI or calculated business metric).
        """
        all_metrics = self.engine.get_metrics()
        if metric_name in all_metrics:
            return all_metrics[metric_name]
        business = all_metrics.get("business_metrics", {})
        return business.get(metric_name, default)

    def get_all_scalars(self) -> Dict[str, Any]:
        """
        Retrieves all system metrics and business logic scalar metrics.
        """
        return self.engine.get_metrics()

    def sink_to_file(self, table_name: str, file_path: Union[str, Path], format: str = "parquet") -> bool:
        """
        Writes the current state of `table_name` to disk (`.parquet`, `.csv`, `.json`).
        Returns True if written successfully, False otherwise.
        """
        arrow_table = self.get_table(table_name)
        if arrow_table is None or arrow_table.num_rows == 0:
            return False

        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        fmt = format.lower()
        if fmt == "parquet":
            papq.write_table(arrow_table, path)
        elif fmt == "csv":
            pacsv.write_csv(arrow_table, path)
        elif fmt in ("json", "ndjson"):
            # Write as newline-delimited json
            pydict_list = arrow_table.to_pylist()
            import json
            with open(path, "w", encoding="utf-8") as f:
                for row in pydict_list:
                    f.write(json.dumps(row) + "\n")
        else:
            raise ValueError(f"Unsupported sink format: {format}")

        return True
