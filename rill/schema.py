"""
Schema definition and metadata helper utilities for Rill Streaming Engine.
Ensures all tables include system metadata columns (`z_insert_ts`, `z_update_ts`) and supports
specifying primary keys directly inside PyArrow schema metadata (`primary_key`).
"""

import time
from typing import Union, List, Optional, Tuple, Any, Dict
import pyarrow as pa


METADATA_COLUMNS = [
    ("z_insert_ts", pa.float64()),
    ("z_update_ts", pa.float64())
]


def schema(
    fields: Union[List[Union[Tuple[str, pa.DataType], pa.Field]], pa.Schema],
    primary_key: Optional[Union[str, List[str]]] = None,
    mode: Optional[str] = None,
    metadata: Optional[Dict[str, str]] = None
) -> pa.Schema:
    """
    Creates or enriches a PyArrow Schema for the Rill Streaming Engine.
    Automatically appends required system metadata columns (`z_insert_ts`, `z_update_ts`) and
    embeds `primary_key` and `mode` (`snapshot`, `append`) definitions inside the schema metadata.

    Args:
        fields: List of `(name, pa.DataType)` tuples, `pa.Field` instances, or an existing `pa.Schema`.
        primary_key: Column name (`str`) or list of column names (`list[str]`) defining the unique key for upserts.
        mode: Table processing mode (`"snapshot"` or `"append"`).
        metadata: Optional additional dictionary metadata to embed.

    Returns:
        Enriched `pa.Schema`.
    """
    if isinstance(fields, pa.Schema):
        field_list = list(fields)
        meta = {k.decode('utf-8'): v.decode('utf-8') for k, v in (fields.metadata or {}).items()}
    else:
        field_list = []
        for f in fields:
            if isinstance(f, tuple):
                field_list.append(pa.field(f[0], f[1]))
            elif isinstance(f, pa.Field):
                field_list.append(f)
            else:
                raise TypeError(f"Unsupported field item type: {type(f)}")
        meta = {}

    if metadata:
        meta.update(metadata)

    # Check existing field names
    existing_names = {f.name for f in field_list}
    for col_name, col_type in METADATA_COLUMNS:
        if col_name not in existing_names:
            field_list.append(pa.field(col_name, col_type))

    # Embed primary key in schema metadata if specified
    if primary_key is not None:
        if isinstance(primary_key, (list, tuple)):
            meta["primary_key"] = ",".join(str(k).strip() for k in primary_key)
        else:
            meta["primary_key"] = str(primary_key).strip()

    # Embed table processing mode if specified
    if mode is not None:
        meta["mode"] = str(mode).strip().lower()

    return pa.schema(field_list, metadata=meta)


def extract_primary_key(schema_obj: Optional[pa.Schema]) -> Optional[Union[str, List[str]]]:
    """
    Extracts the primary key definition embedded in a PyArrow Schema's metadata (`primary_key`).
    Returns `str` if single column, `list[str]` if composite key, or `None` if not specified.
    """
    if schema_obj is None or schema_obj.metadata is None:
        return None

    for key_bytes in (b"primary_key", "primary_key"):
        if key_bytes in schema_obj.metadata:
            val = schema_obj.metadata[key_bytes]
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            val = val.strip()
            if not val:
                return None
            if "," in val:
                return [k.strip() for k in val.split(",") if k.strip()]
            return val
    return None


def extract_mode(schema_obj: Optional[pa.Schema]) -> str:
    """
    Extracts the table processing mode embedded in a PyArrow Schema's metadata (`mode`).
    Returns `"append"` if mode is set to append, otherwise returns `"snapshot"` (default).
    """
    if schema_obj is None or schema_obj.metadata is None:
        return "snapshot"

    for key_bytes in (b"mode", "mode"):
        if key_bytes in schema_obj.metadata:
            val = schema_obj.metadata[key_bytes]
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            val = val.strip().lower()
            if val in ("append", "snapshot", "upsert"):
                return val
    return "snapshot"


def ensure_schema_metadata(schema_obj: Optional[pa.Schema]) -> Optional[pa.Schema]:
    """
    Ensures that any PyArrow Schema passed or inferred has `z_insert_ts` and `z_update_ts` fields.
    """
    if schema_obj is None:
        return None
    return schema(schema_obj)


def estimate_datatype_size(t: pa.DataType, avg_string_length: int = 32, avg_list_length: int = 5) -> float:
    """
    Estimates the average memory size (in bytes per row) for a given PyArrow DataType.
    """
    # 1. Check if type has a fixed byte_width (e.g. int, float, timestamp, decimal, interval)
    try:
        return t.byte_width
    except (ValueError, TypeError, AttributeError):
        pass

    # 2. Check for boolean (stored as 1 bit per row = 0.125 bytes)
    if pa.types.is_boolean(t):
        return 0.125

    # 3. Check for string / binary types
    if pa.types.is_string(t) or pa.types.is_binary(t):
        # 32-bit offsets (4 bytes) + average value data size
        return 4.0 + avg_string_length

    if pa.types.is_large_string(t) or pa.types.is_large_binary(t):
        # 64-bit offsets (8 bytes) + average value data size
        return 8.0 + avg_string_length

    # 4. Check for nested types: Lists/Maps
    if pa.types.is_list(t):
        # 32-bit offsets (4 bytes) + average elements * size of child type
        item_size = estimate_datatype_size(t.value_type, avg_string_length, avg_list_length)
        return 4.0 + (avg_list_length * item_size)

    if pa.types.is_large_list(t):
        # 64-bit offsets (8 bytes) + average elements * size of child type
        item_size = estimate_datatype_size(t.value_type, avg_string_length, avg_list_length)
        return 8.0 + (avg_list_length * item_size)

    if pa.types.is_fixed_size_list(t):
        # No offset buffer, just list_size * child size
        item_size = estimate_datatype_size(t.value_type, avg_string_length, avg_list_length)
        return t.list_size * item_size

    if pa.types.is_map(t):
        # Map is stored as a list of structs containing keys and items
        # 32-bit offset (4 bytes) + avg elements * (key size + item size)
        key_size = estimate_datatype_size(t.key_type, avg_string_length, avg_list_length)
        item_size = estimate_datatype_size(t.item_type, avg_string_length, avg_list_length)
        return 4.0 + (avg_list_length * (key_size + item_size))

    # 5. Check for struct types
    if pa.types.is_struct(t):
        # Size of struct is the sum of sizes of its fields
        size = 0.0
        for i in range(t.num_fields):
            field = t.field(i)
            size += estimate_datatype_size(field.type, avg_string_length, avg_list_length)
        return size

    # 6. Check for dictionary type
    if pa.types.is_dictionary(t):
        # Per row size is just the index size.
        # (Dictionary values are stored once globally in the chunk and not per row)
        return estimate_datatype_size(t.index_type, avg_string_length, avg_list_length)

    # Fallback to a default if unknown
    return 8.0


def estimate_schema_row_size(
    schema_obj: pa.Schema,
    avg_string_length: int = 32,
    avg_list_length: int = 5,
    include_validity_bitmap: bool = True
) -> float:
    """
    Estimates the average row size (in bytes) of a PyArrow schema.
    """
    total_bytes = 0.0
    for field in schema_obj:
        # Get base type size
        field_size = estimate_datatype_size(field.type, avg_string_length, avg_list_length)
        # Add 1 bit (0.125 bytes) for the validity bitmap if nullable and requested
        if include_validity_bitmap and field.nullable:
            field_size += 0.125
        total_bytes += field_size
    return total_bytes

