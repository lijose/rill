"""
Tests for schema definition, metadata enrichment (`z_insert_ts`, `z_update_ts`), and primary key embedding.
"""

import pyarrow as pa
from rill import schema, extract_primary_key, RillTable


def test_schema_enrichment_and_primary_key_extract():
    # Define schema with single primary key
    s = schema([
        ("user_id", pa.int64()),
        ("email", pa.string())
    ], primary_key="user_id")

    # Verify metadata columns automatically added
    assert "z_insert_ts" in s.names
    assert "z_update_ts" in s.names
    assert s.field("z_insert_ts").type == pa.float64()

    # Verify primary key extraction
    assert extract_primary_key(s) == "user_id"


def test_schema_composite_primary_key():
    s = schema([
        ("tenant_id", pa.string()),
        ("event_id", pa.int64())
    ], primary_key=["tenant_id", "event_id"])

    pk = extract_primary_key(s)
    assert pk == ["tenant_id", "event_id"]


def test_table_auto_extracts_primary_key_from_schema():
    # When RillTable is initialized with an enriched schema and primary_key=None,
    # it should automatically extract and use the primary key embedded in the schema metadata
    s = schema([("sku", pa.string()), ("qty", pa.int64())], primary_key="sku")
    tbl = RillTable("inventory", schema=s)
    
    assert tbl.primary_key == "sku"

    # Upsert two items and verify primary key upsert behavior
    tbl.upsert(pa.Table.from_pydict({"sku": ["A", "B"], "qty": [10, 20]}))
    tbl.upsert(pa.Table.from_pydict({"sku": ["A"], "qty": [15]}))

    assert tbl.num_rows == 2
    rows = tbl.to_arrow().to_pylist()
    a_row = next(r for r in rows if r["sku"] == "A")
    assert a_row["qty"] == 15
    # Verify metadata columns populated
    assert "z_insert_ts" in a_row and "z_update_ts" in a_row
