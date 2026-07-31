"""
Rill: The Lightweight PyArrow & DuckDB Streaming Engine.
"""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("rills")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0.dev0"

from .engine import RillEngine
from .table import RillTable
from .schema import schema, extract_primary_key, extract_mode, estimate_schema_row_size
from .connectors.base import BaseConnector
from .connectors.memory import MemoryConnector
from .connectors.json_stream import JSONStreamConnector
from .connectors.file import FileStreamConnector
from .connectors.websocket import WebSocketConnector
from .connectors.kafka import KafkaConnector
from .compute.sql import ScheduledSQLTask, DuckDBBridge
from .compute.join import TableJoinTask
from .compute.upsert import upsert_table
from .retention import RetentionPolicy
from .metrics import MetricsRegistry
from .output import OutputAPI

__all__ = [
    "RillEngine",
    "RillTable",
    "schema",
    "extract_primary_key",
    "extract_mode",
    "estimate_schema_row_size",
    "BaseConnector",
    "MemoryConnector",
    "JSONStreamConnector",
    "FileStreamConnector",
    "WebSocketConnector",
    "KafkaConnector",
    "ScheduledSQLTask",
    "TableJoinTask",
    "DuckDBBridge",
    "RetentionPolicy",
    "MetricsRegistry",
    "OutputAPI",
    "upsert_table",
]
