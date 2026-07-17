"""
File & Directory Monitoring Connector for Rill Streaming Engine.
Monitors files/folders and ingests batches from JSON, CSV, Parquet, or Arrow IPC files directly into C++ memory.
"""

import os
import glob
import threading
from pathlib import Path
from typing import Optional, List, Set, Union
import pyarrow as pa
import pyarrow.json as pajson
import pyarrow.csv as pacsv
import pyarrow.parquet as papq
import pyarrow.ipc as paipc
from .base import BaseConnector


class FileStreamConnector(BaseConnector):
    """
    Monitors a directory or file path for incoming data files (`.json`, `.csv`, `.parquet`, `.arrow`)
    and loads them into PyArrow Tables during micro-batch ticks.
    Once processed, files can optionally be deleted or marked as read to prevent duplicate processing.
    """

    def __init__(
        self,
        target_table: str,
        watch_path: Union[str, Path],
        file_pattern: str = "*",
        delete_after_process: bool = False
    ):
        super().__init__(target_table)
        self.watch_path = Path(watch_path)
        self.file_pattern = file_pattern
        self.delete_after_process = delete_after_process
        self._processed_files: Set[str] = set()
        self._lock = threading.RLock()

    def read_batch(self) -> Optional[pa.Table]:
        """
        Scans `watch_path` for new matching files and converts them directly to PyArrow Tables.
        """
        if not self.watch_path.exists():
            return None

        new_tables: List[pa.Table] = []

        with self._lock:
            if self.watch_path.is_file():
                files_to_check = [self.watch_path]
            else:
                files_to_check = [
                    Path(f) for f in glob.glob(str(self.watch_path / self.file_pattern))
                    if Path(f).is_file()
                ]

            for file_path in sorted(files_to_check):
                abs_str = str(file_path.absolute())
                if not self.delete_after_process and abs_str in self._processed_files:
                    continue

                try:
                    table = self._parse_file(file_path)
                    if table is not None and table.num_rows > 0:
                        new_tables.append(table)

                    if self.delete_after_process:
                        file_path.unlink(missing_ok=True)
                    else:
                        self._processed_files.add(abs_str)
                except Exception as e:
                    # Skip or log error on unparseable/partial files being written
                    pass

        if not new_tables:
            return None
        return pa.concat_tables(new_tables, promote_options="default")

    def _parse_file(self, file_path: Path) -> Optional[pa.Table]:
        suffix = file_path.suffix.lower()
        if suffix in (".json", ".ndjson"):
            return pajson.read_json(file_path)
        elif suffix == ".csv":
            return pacsv.read_csv(file_path)
        elif suffix == ".parquet":
            return papq.read_table(file_path)
        elif suffix in (".arrow", ".ipc"):
            with pa.OSFile(str(file_path), 'rb') as source:
                try:
                    reader = paipc.open_stream(source)
                except pa.ArrowInvalid:
                    source.seek(0)
                    reader = paipc.open_file(source)
                return reader.read_all()
        else:
            # Try JSON fallback
            try:
                return pajson.read_json(file_path)
            except Exception:
                return None
