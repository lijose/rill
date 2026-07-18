"""
Tests for schema definition, metadata enrichment (`z_insert_ts`, `z_update_ts`), and primary key embedding.
"""

import pyarrow as pa
from rill import schema, extract_primary_key, RillTable, estimate_schema_row_size


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


def test_schema_row_size_estimation():
    # 1. Test basic types
    # z_insert_ts (8B), z_update_ts (8B) are automatically added, plus user_id (8B) and score (4B)
    # Total fixed bytes: 8 + 8 + 8 + 4 = 28 bytes.
    # Total fields = 4. If all nullable, 4 * 0.125 = 0.5 bytes.
    # Total = 28.5 bytes.
    s1 = schema([
        ("user_id", pa.int64()),
        ("score", pa.int32())
    ])

    assert estimate_schema_row_size(s1, include_validity_bitmap=True) == 28.5
    assert estimate_schema_row_size(s1, include_validity_bitmap=False) == 28.0

    # 2. Test string and nested list types
    # user_id: 8 bytes + 0.125
    # name: string -> 4 (offset) + avg_string_length (e.g. 10) = 14 + 0.125
    # tags: list of string -> 4 (offset) + 3 elements * (4 + avg_string_length (10)) = 4 + 3 * 14 = 46 + 0.125
    # metadata: z_insert_ts (8 + 0.125), z_update_ts (8 + 0.125)
    # Total: 8.125 + 14.125 + 46.125 + 8.125 + 8.125 = 84.625 bytes
    s2 = schema([
        ("user_id", pa.int64()),
        ("name", pa.string()),
        ("tags", pa.list_(pa.string()))
    ])

    est = estimate_schema_row_size(s2, avg_string_length=10, avg_list_length=3)
    assert est == 84.625

    # 3. Test RillTable integration
    tbl = RillTable("metrics", schema=s1)
    # Row size for s1 is 28.5 bytes
    assert tbl.estimate_row_size() == 28.5

    # 1000 bytes budget. Safety factor = 0.75.
    # Capacity = int(1000 * 0.75 / 28.5) = int(750 / 28.5) = 26 records.
    assert tbl.estimate_records_capacity(memory_budget_bytes=1000, safety_factor=0.75) == 26
