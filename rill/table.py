"""
RillTable: Encapsulates PyArrow Table state, schema, primary keys, and atomic updates.
"""

import threading
from typing import Union, List, Optional, Callable, TYPE_CHECKING
import pyarrow as pa
from .compute.upsert import upsert_table, _ensure_metadata_columns
from .schema import ensure_schema_metadata, extract_primary_key, extract_mode
if TYPE_CHECKING:
    from .retention import RetentionPolicy


class RillTable:
    """
    Thread-safe wrapper around a PyArrow Table representing live state in C++ memory.
    Supports atomic Delete-and-Insert upserts (`upsert()`), subscriber notifications (`subscribe()`),
    time-to-live row retention policies (`apply_retention()`), and automated system metadata
    columns (`z_insert_ts`, `z_update_ts`).
    Can operate in `"snapshot"` mode (upsert with primary key) or `"append"` mode (mandatory TTL).
    """

    def __init__(
        self,
        name: str,
        schema: Optional[pa.Schema] = None,
        primary_key: Optional[Union[str, List[str]]] = None,
        retention_policy: Optional['RetentionPolicy'] = None,
        mode: Optional[str] = None
    ):
        self.name = name
        if schema is not None:
            self.schema = ensure_schema_metadata(schema)
            if primary_key is None:
                self.primary_key = extract_primary_key(self.schema)
            else:
                self.primary_key = primary_key
            if mode is None:
                self.mode = extract_mode(self.schema)
            else:
                self.mode = str(mode).strip().lower()
        else:
            self.schema = None
            self.primary_key = primary_key
            self.mode = str(mode).strip().lower() if mode is not None else "snapshot"

        self.retention_policy = retention_policy
        self._validate_mode_and_ttl()
        self._arrow_table: Optional[pa.Table] = None
        self._lock = threading.RLock()
        self._callbacks: List[Callable[[str, pa.Table], None]] = []

    def _validate_mode_and_ttl(self) -> None:
        if self.mode == "append":
            if self.retention_policy is None or (
                self.retention_policy.max_rows is None and self.retention_policy.max_age_seconds is None
            ):
                raise ValueError(
                    f"Table '{self.name}': Retention policy (TTL) with max_rows or max_age_seconds "
                    f"is mandatory for append-only tables (mode='append') to prevent unbounded memory growth."
                )

    def subscribe(self, callback: Callable[[str, pa.Table], None]) -> None:
        """
        Registers a callback function triggered whenever this table state changes.
        """
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def unsubscribe(self, callback: Callable[[str, pa.Table], None]) -> None:
        """
        Removes a registered callback function.
        """
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

    def _notify_subscribers(self) -> None:
        """
        Notifies all subscribers with zero-copy reference to the current Table state.
        """
        if not self._callbacks or self._arrow_table is None:
            return
        # Copy callback list to prevent issues if subscriber modifies list during notification
        callbacks = list(self._callbacks)
        table_ref = self._arrow_table
        for cb in callbacks:
            try:
                cb(self.name, table_ref)
            except Exception as e:
                # Log or ignore callback exceptions so engine loop isn't blocked
                pass

    def upsert(
        self,
        batch_or_table: Union[pa.Table, pa.RecordBatch, List[pa.RecordBatch]],
        promote_options: str = "default",
        current_time: Optional[float] = None
    ) -> pa.Table:
        """
        Performs atomic Delete-and-Insert (Upsert) or append of new batch into this table.

        Args:
            batch_or_table: Incoming PyArrow Table, RecordBatch, or list of RecordBatches.
            promote_options: Schema promotion strategy passed to `pa.concat_tables`.
            current_time: Optional reference timestamp for evaluating TTL retention policies during upsert.

        Returns:
            The updated PyArrow Table in C++ memory.
        """
        with self._lock:
            if isinstance(batch_or_table, list):
                if not batch_or_table:
                    return self._arrow_table if self._arrow_table is not None else pa.Table.from_batches([], schema=self.schema)
                try:
                    batch_or_table = pa.Table.from_batches(batch_or_table, schema=self.schema)
                except (pa.ArrowInvalid, KeyError, ValueError):
                    batch_or_table = pa.Table.from_batches(batch_or_table)
            elif isinstance(batch_or_table, pa.RecordBatch):
                try:
                    batch_or_table = pa.Table.from_batches([batch_or_table], schema=self.schema)
                except (pa.ArrowInvalid, KeyError, ValueError):
                    batch_or_table = pa.Table.from_batches([batch_or_table])

            # If our table currently has no schema set, infer from incoming batch
            if self.schema is None:
                self.schema = ensure_schema_metadata(batch_or_table.schema)
                if self.primary_key is None:
                    self.primary_key = extract_primary_key(self.schema)
                if self.mode == "snapshot" and extract_mode(self.schema) == "append":
                    self.mode = "append"
                self._validate_mode_and_ttl()

            effective_pk = None if self.mode == "append" else self.primary_key

            # Perform C++ vectorized upsert via rill.compute.upsert
            updated_table = upsert_table(
                old_table=self._arrow_table,
                new_table=batch_or_table,
                primary_key=effective_pk,
                promote_options=promote_options,
                current_time=current_time
            )

            # Apply optional retention policy if configured
            if self.retention_policy is not None:
                updated_table = self.retention_policy.apply(updated_table, current_time=current_time)

            self._arrow_table = updated_table
            if self.schema is None and updated_table is not None:
                self.schema = ensure_schema_metadata(updated_table.schema)

        self._notify_subscribers()
        return self._arrow_table

    def apply_retention(self, current_time: Optional[float] = None) -> Optional[pa.Table]:
        """
        Explicitly triggers the retention policy to prune old/excess rows.
        Also automatically compacts chunked arrays if fragmentation exceeds threshold.
        """
        with self._lock:
            if self.retention_policy is not None:
                pruned_table = self.retention_policy.apply(self._arrow_table, current_time=current_time)
                changed = (pruned_table is not self._arrow_table) and (
                    pruned_table is None or self._arrow_table is None or pruned_table.num_rows != self._arrow_table.num_rows
                )
                self._arrow_table = pruned_table
                if changed:
                    self._notify_subscribers()

            if self._arrow_table is not None and self._arrow_table.num_rows > 0 and self._arrow_table.num_columns > 0:
                if any(self._arrow_table.column(i).num_chunks > 32 for i in range(self._arrow_table.num_columns)):
                    self._arrow_table = self._arrow_table.combine_chunks(pa.default_memory_pool())
        return self._arrow_table

    def compact(self, max_chunks: int = 32) -> Optional[pa.Table]:
        """
        Compacts the underlying PyArrow Table chunks if any column exceeds `max_chunks`.
        Returns the compacted table or None if the table is empty/None.
        """
        with self._lock:
            if self._arrow_table is None or self._arrow_table.num_rows == 0 or self._arrow_table.num_columns == 0:
                return self._arrow_table
            needs_compact = any(
                self._arrow_table.column(i).num_chunks > max_chunks
                for i in range(self._arrow_table.num_columns)
            )
            if needs_compact:
                self._arrow_table = self._arrow_table.combine_chunks(pa.default_memory_pool())
        return self._arrow_table

    def replace_state(self, new_table: pa.Table, current_time: Optional[float] = None) -> pa.Table:
        """
        Directly replaces the entire underlying table state (e.g., from a scheduled SQL query output).
        Automatically ensures system metadata columns exist on the replacement table.
        """
        with self._lock:
            if new_table is not None:
                _, new_table = _ensure_metadata_columns(new_table, None, self.primary_key, current_time)
                self.schema = ensure_schema_metadata(new_table.schema)
            self._arrow_table = new_table
        self._notify_subscribers()
        return self._arrow_table

    def to_arrow(self) -> Optional[pa.Table]:
        """
        Returns the current underlying PyArrow Table (zero-copy reference to C++ memory).
        """
        with self._lock:
            return self._arrow_table

    @property
    def num_rows(self) -> int:
        with self._lock:
            return self._arrow_table.num_rows if self._arrow_table is not None else 0

    @property
    def num_columns(self) -> int:
        with self._lock:
            return self._arrow_table.num_columns if self._arrow_table is not None else 0

    def estimate_row_size(self, avg_string_length: int = 32, avg_list_length: int = 5) -> Optional[float]:
        """
        Estimates the average memory size of a single row in this table (in bytes) based on its schema.
        Returns None if schema is not set.
        """
        with self._lock:
            if self.schema is None:
                return None
            from .schema import estimate_schema_row_size
            return estimate_schema_row_size(self.schema, avg_string_length=avg_string_length, avg_list_length=avg_list_length)

    def estimate_records_capacity(
        self,
        memory_budget_bytes: int,
        safety_factor: float = 0.75,
        avg_string_length: int = 32,
        avg_list_length: int = 5
    ) -> Optional[int]:
        """
        Estimates how many records will fit within a given memory budget (in bytes) for this table,
        applying a safety factor to account for alignment and overhead.
        Returns None if schema is not set.
        """
        row_size = self.estimate_row_size(avg_string_length=avg_string_length, avg_list_length=avg_list_length)
        if row_size is None or row_size <= 0:
            return None
        return int((memory_budget_bytes * safety_factor) / row_size)

    def __repr__(self) -> str:
        return f"<RillTable(name='{self.name}', mode='{self.mode}', num_rows={self.num_rows}, num_columns={self.num_columns}, primary_key={self.primary_key})>"
