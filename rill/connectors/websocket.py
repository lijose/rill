"""
WebSocket Connector for Rill Streaming Engine.
Captures live WebSocket event streams directly into C++ PyArrow tables.
"""

import threading
from typing import Optional
import pyarrow as pa
from .base import BaseConnector
from .json_stream import JSONStreamConnector


class WebSocketConnector(BaseConnector):
    """
    Connects to a remote WebSocket endpoint in a background thread, buffers incoming raw byte frames,
    and converts them directly to PyArrow tables during each micro-batch tick.
    Requires `websockets` library (`pip install websockets`).
    """

    def __init__(self, target_table: str, uri: str):
        super().__init__(target_table)
        self.uri = uri
        self._json_stream = JSONStreamConnector(target_table=target_table)
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        """
        Starts the background WebSocket client listening loop.
        """
        if self._running:
            return
        try:
            import websockets
        except ImportError as e:
            raise ImportError(
                "The 'websockets' library is required for WebSocketConnector. "
                "Install it using `pip install websockets`."
            ) from e

        self._running = True
        self._thread = threading.Thread(target=self._ws_loop, name="WebSocketConnectorThread", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """
        Stops the background WebSocket client loop.
        """
        self._running = False
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
            self._thread = None

    def _ws_loop(self) -> None:
        import asyncio
        import websockets

        async def _async_listen():
            while self._running:
                try:
                    async with websockets.connect(self.uri) as ws:
                        while self._running:
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            if isinstance(message, str):
                                self._json_stream.push_bytes(message.encode('utf-8'))
                            elif isinstance(message, (bytes, bytearray)):
                                self._json_stream.push_bytes(message)
                except Exception:
                    # On disconnect or timeout, sleep briefly before reconnect
                    if self._running:
                        await asyncio.sleep(1.0)

        try:
            asyncio.run(_async_listen())
        except Exception:
            pass

    def read_batch(self) -> Optional[pa.Table]:
        """
        Reads the buffered WebSocket frames and parses them via C++ PyArrow readers.
        """
        return self._json_stream.read_batch()
