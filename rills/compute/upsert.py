"""
Vectorized Delete-and-Insert (Upsert) implementation using pure PyArrow compute kernels.
Bypasses Python row loops to keep operations entirely in C++ contiguous memory.
"""

from typing import Union, List, Optional, Tuple
import pyarrow as pa
import pyarrow.compute as pc


def _extract_key_array(table: pa.Table, keys: Union[str, List[str]]) -> Union[pa.Array, pa.ChunkedArray]:
    """
    Extracts key column(s) from a PyArrow Table as an Array or StructArray for comparison.
    Decodes dictionary keys if present to ensure uniform comparison across batches.
    """
    if isinstance(keys, str):
        col = table.column(keys)
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        return col
    elif isinstance(keys, (list, tuple)):
        if len(keys) == 1:
            col = table.column(keys[0])
            if pa.types.is_dictionary(col.type):
                col = pc.dictionary_decode(col)
            return col
        # For composite keys, combine into a StructArray, decoding dictionary fields first
        arrays = []
        for k in keys:
            col = table.column(k).combine_chunks()
            if pa.types.is_dictionary(col.type):
                col = pc.dictionary_decode(col)
            arrays.append(col)
        return pa.StructArray.from_arrays(arrays, names=list(keys))
    else:
        raise TypeError(f"primary_key must be str or list of str, got {type(keys)}")


import time


def _ensure_metadata_columns(
    new_table: pa.Table,
    old_table: Optional[pa.Table],
    primary_key: Optional[Union[str, List[str]]] = None,
    current_time: Optional[float] = None
) -> Tuple[Optional[pa.Table], pa.Table]:
    """
    Ensures `z_insert_ts` and `z_update_ts` columns exist on both old and new tables.
    Always appends `z_insert_ts` first and `z_update_ts` second so schemas remain aligned.
    For incoming records matching existing keys, preserves original `z_insert_ts` from `old_table`.
    """
    ts = current_time if current_time is not None else time.time()

    # Ensure old_table has metadata columns if present (in exact order: z_insert_ts, z_update_ts)
    if old_table is not None and old_table.num_rows > 0:
        if "z_insert_ts" not in old_table.column_names:
            old_table = old_table.append_column("z_insert_ts", pa.array([ts] * old_table.num_rows, type=pa.float64()))
        if "z_update_ts" not in old_table.column_names:
            old_table = old_table.append_column("z_update_ts", pa.array([ts] * old_table.num_rows, type=pa.float64()))

    # Populate z_insert_ts on new_table first
    if "z_insert_ts" in new_table.column_names:
        pass
    elif old_table is not None and old_table.num_rows > 0 and primary_key is not None and primary_key != "" and primary_key != []:
        keys_list = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        # Hash joins may reorder rows. Carry an ordinal through the lookup and
        # restore it before attaching timestamps to the original incoming table.
        ordinal = "__rill_row_position"
        while ordinal in new_table.column_names or ordinal in old_table.column_names:
            ordinal += "_"
        lookup_input = _decode_dictionary_columns(new_table.select(keys_list)).append_column(
            ordinal, pa.array(range(new_table.num_rows), type=pa.int64())
        )
        lookup = lookup_input.join(
            _decode_dictionary_columns(old_table.select(keys_list + ["z_insert_ts"])),
            keys=keys_list, join_type="left outer"
        ).sort_by(ordinal)
        if lookup.num_rows != new_table.num_rows:
            raise ValueError("Existing state contains duplicate primary keys")
        filled = pc.fill_null(lookup.column("z_insert_ts"), pa.scalar(ts, type=pa.float64()))
        new_table = new_table.append_column("z_insert_ts", filled)
    else:
        new_table = new_table.append_column("z_insert_ts", pa.array([ts] * new_table.num_rows, type=pa.float64()))

    # Populate or update z_update_ts on new_table second
    if "z_update_ts" in new_table.column_names:
        pass  # Keep explicit update_ts if supplied
    else:
        new_table = new_table.append_column("z_update_ts", pa.array([ts] * new_table.num_rows, type=pa.float64()))

    return old_table, new_table


def upsert_table(
    old_table: Optional[pa.Table],
    new_table: pa.Table,
    primary_key: Optional[Union[str, List[str]]] = None,
    promote_options: str = "default",
    current_time: Optional[float] = None
) -> pa.Table:
    """
    Performs a high-performance vectorized upsert (Delete-and-Insert) or append of `new_table`
    into `old_table` using PyArrow compute kernels (`pc.is_in`, `pc.invert`, `pa.concat_tables`).
    Also automatically maintains system metadata columns (`z_insert_ts`, `z_update_ts`).

    Args:
        old_table: Existing state table (`pa.Table` or None).
        new_table: Incoming micro-batch table (`pa.Table` or `pa.RecordBatch`).
        primary_key: Single column name (`str`) or list of column names (`list[str]`) defining unique keys.
                     If None, `new_table` is simply appended to `old_table`.
        promote_options: Schema promotion options passed to `pa.concat_tables` (default: 'default').
        current_time: Optional reference timestamp for populating metadata columns.

    Returns:
        Updated `pa.Table` in C++ memory.
    """
    # Ensure new_table is pa.Table
    if isinstance(new_table, pa.RecordBatch):
        new_table = pa.Table.from_batches([new_table])
    elif not isinstance(new_table, pa.Table):
        raise TypeError(f"new_table must be pa.Table or pa.RecordBatch, got {type(new_table)}")

    # Deduplicate intra-batch primary keys in new_table
    if primary_key:
        import duckdb
        keys_list = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        partition_by = ", ".join('"' + k.replace('"', '""') + '"' for k in keys_list)
        row_id = "__rill_dedup_position"
        while row_id in new_table.column_names:
            row_id += "_"
        # A private connection avoids sharing DuckDB's module-global connection
        # between independent table writers.
        with duckdb.connect() as con:
            con.register("incoming", new_table)
            query = f'SELECT * EXCLUDE ("{row_id}") FROM (SELECT *, row_number() OVER () as "{row_id}" FROM incoming) QUALIFY row_number() OVER (PARTITION BY {partition_by} ORDER BY "{row_id}" DESC) = 1 ORDER BY "{row_id}"'
            new_table = con.sql(query).to_arrow_table()

    # Ensure metadata columns exist and preserve z_insert_ts for updated rows
    old_table, new_table = _ensure_metadata_columns(new_table, old_table, primary_key, current_time)

    # Edge cases when one table is empty or None
    if old_table is None or old_table.num_rows == 0:
        return new_table
    if new_table.num_rows == 0:
        return old_table

    # Ensure schema unification if field nullabilities or types slightly differ
    # Try casting or unifying schema when appropriate
    if old_table.schema != new_table.schema:
        try:
            # First try selecting columns in exact order of old_table if names match
            if set(old_table.column_names) == set(new_table.column_names):
                new_table = new_table.select(old_table.column_names)
            new_table = new_table.cast(old_table.schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError):
            # If explicit cast to old_table schema fails, pa.concat_tables with promote_options
            # will handle schema promotion or unification
            pass

    # If no primary_key is provided, standard append
    if not primary_key:
        old_table = _decode_dictionary_columns(old_table)
        new_table = _decode_dictionary_columns(new_table)
        return pa.concat_tables([old_table, new_table], promote_options=promote_options)

    # For composite keys (or if pc.is_in is not supported on struct type),
    # leverage C++ relational left anti join kernel (`old_table.join(new_table, keys=..., join_type="left anti")`)
    if isinstance(primary_key, (list, tuple)) and len(primary_key) > 1:
        surviving_table = old_table.join(new_table, keys=list(primary_key), join_type="left anti")
        surviving_table = _decode_dictionary_columns(surviving_table)
        new_table = _decode_dictionary_columns(new_table)
        return pa.concat_tables([surviving_table, new_table], promote_options=promote_options)

    # For single keys, use fast primitive compute kernel `pc.is_in`
    old_keys = _extract_key_array(old_table, primary_key)
    new_keys = _extract_key_array(new_table, primary_key)

    if isinstance(new_keys, pa.ChunkedArray):
        new_keys_val_set = new_keys.combine_chunks()
    else:
        new_keys_val_set = new_keys

    try:
        mask = pc.is_in(old_keys, value_set=new_keys_val_set)
        surviving_mask = pc.invert(mask)
        surviving_table = pc.filter(old_table, surviving_mask)
    except pa.ArrowNotImplementedError:
        # Fallback to C++ relational left anti join if kernel unsupported
        keys_list = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        surviving_table = old_table.join(new_table, keys=keys_list, join_type="left anti")

    surviving_table = _decode_dictionary_columns(surviving_table)
    new_table = _decode_dictionary_columns(new_table)
    return pa.concat_tables([surviving_table, new_table], promote_options=promote_options)


def _decode_dictionary_columns(table: pa.Table) -> pa.Table:
    """
    Decodes all dictionary-encoded columns in a table to their plain value types.
    This prevents dictionary index alignment issues when concatenating tables
    that were built with different dictionary value sets.
    """
    for i, field in enumerate(table.schema):
        if pa.types.is_dictionary(field.type):
            table = table.set_column(i, field.name, pc.dictionary_decode(table.column(i)))
    return table
