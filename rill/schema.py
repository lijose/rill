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
