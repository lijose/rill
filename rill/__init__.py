"""
Rill: The Lightweight PyArrow & DuckDB Streaming Engine.
"""

from .engine import RillEngine
from .table import RillTable
from .schema import schema, extract_primary_key, extract_mode
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
from .checkpoint import Checkpointer
from .metrics import MetricsRegistry
from .output import OutputAPI

__all__ = [
    "RillEngine",
    "RillTable",
    "schema",
    "extract_primary_key",
    "extract_mode",
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
    "Checkpointer",
    "MetricsRegistry",
    "OutputAPI",
    "upsert_table",
]
