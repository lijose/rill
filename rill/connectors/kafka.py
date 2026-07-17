"""
Kafka Connector for Rill Streaming Engine.
Captures high-throughput Kafka event streams directly into C++ PyArrow memory.
"""

import threading
from typing import Optional, List, Dict, Any
import pyarrow as pa
from .base import BaseConnector
from .json_stream import JSONStreamConnector


class KafkaConnector(BaseConnector):
    """
    Subscribes to one or more Kafka topics, polls raw message payloads in a background thread,
    and converts them directly into PyArrow tables during scheduled micro-batch ticks.
    Requires `confluent-kafka` library (`pip install confluent-kafka`).
    """

    def __init__(self, target_table: str, topics: List[str], kafka_config: Dict[str, Any]):
        super().__init__(target_table)
        self.topics = topics
        self.kafka_config = kafka_config
        self._json_stream = JSONStreamConnector(target_table=target_table)
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        """
        Starts the background Kafka consumer polling thread.
        """
        if self._running:
            return
        try:
            import confluent_kafka
        except ImportError as e:
            raise ImportError(
                "The 'confluent-kafka' library is required for KafkaConnector. "
                "Install it using `pip install confluent-kafka`."
            ) from e

        self._running = True
        self._thread = threading.Thread(target=self._kafka_loop, name="KafkaConnectorThread", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """
        Stops the Kafka polling thread and closes the consumer.
        """
        self._running = False
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            self._thread = None

    def _kafka_loop(self) -> None:
        from confluent_kafka import Consumer, KafkaError

        consumer = Consumer(self.kafka_config)
        consumer.subscribe(self.topics)

        try:
            while self._running:
                msg = consumer.poll(timeout=0.2)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        # Log error if needed
                        pass
                    continue

                raw_val = msg.value()
                if raw_val:
                    self._json_stream.push_bytes(raw_val)
        finally:
            consumer.close()

    def read_batch(self) -> Optional[pa.Table]:
        """
        Extracts accumulated Kafka message payloads and parses them into PyArrow Tables.
        """
        return self._json_stream.read_batch()
