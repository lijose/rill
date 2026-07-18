# will add later
# """
# Tests for Persistent Checkpointing and Snapshot Recovery.
# """
# 
# import time
# from pathlib import Path
# import pyarrow as pa
# from rill import RillEngine, Checkpointer
# 
# 
# def test_checkpoint_save_and_restore(tmp_path):
#     checkpoint_dir = tmp_path / "checkpoints"
#     
#     # 1. Start engine with checkpointer and add data to table
#     engine1 = RillEngine(checkpoint_dir=checkpoint_dir, checkpoint_interval_seconds=0.1)
#     tbl1 = engine1.register_table("users", primary_key="id")
#     tbl1.upsert(pa.Table.from_pydict({
#         "id": [1, 2],
#         "name": ["Alice", "Bob"]
#     }))
# 
#     # Save checkpoints explicitly
#     saved = engine1.checkpointer.save_snapshots(engine1)
#     assert "users" in saved
#     assert (checkpoint_dir / "users.parquet").exists()
# 
#     # 2. Start new fresh engine instance pointing to the same checkpoint_dir
#     engine2 = RillEngine(checkpoint_dir=checkpoint_dir)
#     restored = engine2.restore_checkpoints()
#     assert "users" in restored
# 
#     # Verify table state was fully restored
#     tbl2 = engine2.get_table("users")
#     assert tbl2 is not None
#     assert tbl2.num_rows == 2
#     assert tbl2.to_arrow().column("name").to_pylist() == ["Alice", "Bob"]

