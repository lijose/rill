# will add later
# """
# Persistent Checkpointing and Snapshot Recovery for Rill Streaming Engine.
# Saves zero-copy snapshots of active PyArrow tables to disk and restores them on engine start.
# """
# 
# import time
# from pathlib import Path
# from typing import Union, List, Optional, TYPE_CHECKING
# import pyarrow as pa
# import pyarrow.parquet as papq
# 
# if TYPE_CHECKING:
#     from .engine import RillEngine
# 
# 
# class Checkpointer:
#     """
#     Manages persistent state snapshots for a `RillEngine`.
#     Periodically writes active `RillTable` PyArrow tables to Parquet files and automatically
#     reloads them on start to survive process restarts.
#     """
# 
#     def __init__(self, checkpoint_dir: Union[str, Path], interval_seconds: float = 60.0):
#         """
#         Args:
#             checkpoint_dir: Directory where Parquet table snapshots are stored.
#             interval_seconds: Frequency in seconds for taking automated snapshots during micro-batch ticks.
#         """
#         self.checkpoint_dir = Path(checkpoint_dir)
#         self.interval_seconds = interval_seconds
#         self.last_checkpoint_time: float = 0.0
# 
#     def is_due(self, current_time: float) -> bool:
#         """
#         Checks if the checkpoint interval has elapsed since `last_checkpoint_time`.
#         """
#         return (current_time - self.last_checkpoint_time) >= self.interval_seconds
# 
#     def save_snapshots(self, engine: 'RillEngine', current_time: Optional[float] = None) -> List[str]:
#         """
#         Sinks all active tables containing data into Parquet files inside `checkpoint_dir`.
#         Returns the list of table names successfully checkpointed.
#         """
#         self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
#         if current_time is None:
#             current_time = time.perf_counter()
# 
#         saved_tables = []
#         # Snapshot table references under engine lock
#         with engine._lock:
#             tables_snapshot = dict(engine.tables)
# 
#         for name, rill_table in tables_snapshot.items():
#             arrow_tbl = rill_table.to_arrow()
#             if arrow_tbl is not None and arrow_tbl.num_rows > 0:
#                 file_path = self.checkpoint_dir / f"{name}.parquet"
#                 try:
#                     papq.write_table(arrow_tbl, file_path)
#                     saved_tables.append(name)
#                 except Exception:
#                     # Skip or log if disk write fails
#                     pass
# 
#         self.last_checkpoint_time = current_time
#         return saved_tables
# 
#     def restore_snapshots(self, engine: 'RillEngine') -> List[str]:
#         """
#         Scans `checkpoint_dir` for existing Parquet snapshots and reloads them into `engine`.
#         Returns the list of table names successfully restored.
#         """
#         if not self.checkpoint_dir.exists():
#             return []
# 
#         restored_tables = []
#         for file_path in sorted(self.checkpoint_dir.glob("*.parquet")):
#             table_name = file_path.stem
#             try:
#                 loaded_table = papq.read_table(file_path)
#                 if loaded_table is not None and loaded_table.num_rows > 0:
#                     tbl = engine.get_or_create_table(table_name, schema=loaded_table.schema)
#                     tbl.replace_state(loaded_table)
#                     restored_tables.append(table_name)
#             except Exception:
#                 # Skip corrupted or unreadable checkpoint files safely
#                 pass
# 
#         return restored_tables
# 
#     def __repr__(self) -> str:
#         return f"<Checkpointer(checkpoint_dir='{self.checkpoint_dir}', interval_seconds={self.interval_seconds})>"

