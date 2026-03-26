"""WebSocket handler for stdlib HTTP server.

Pure-Python WebSocket implementation on top of BaseHTTPRequestHandler.
Handles the WebSocket upgrade handshake (RFC 6455) and provides
frame-level send/receive for text messages.

No external dependencies -- uses only Python stdlib (hashlib, base64, struct).

Thread-safe: uses locks for shared WebSocket connection registry.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import struct
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# RFC 6455 magic GUID for WebSocket handshake
_WS_MAGIC = b"258EAFA5-E914-47DA-95CA-5AB5CD7AB653"

# Opcodes
WS_OPCODE_TEXT = 0x1
WS_OPCODE_BINARY = 0x2
WS_OPCODE_CLOSE = 0x8
WS_OPCODE_PING = 0x9
WS_OPCODE_PONG = 0xA

# Max frame payload (prevent memory exhaustion)
WS_MAX_PAYLOAD = 1_048_576  # 1 MB

# ── Active WebSocket connections registry (thread-safe) ──
_ws_connections: dict[str, "WebSocketConnection"] = {}
_ws_connections_lock = threading.Lock()
_WS_MAX_CONNECTIONS = 200  # Cap total concurrent WS connections


class WebSocketError(Exception):
    """Raised on WebSocket protocol errors."""

    pass


class WebSocketConnection:
    """Represents a single WebSocket connection.

    Wraps the raw socket from BaseHTTPRequestHandler.wfile/rfile
    and provides send/receive methods with proper framing.
    """

    def __init__(
        self,
        rfile,
        wfile,
        connection_id: str,
        conversation_id: str = "",
    ) -> None:
        """Initialize WebSocket connection.

        Args:
            rfile: Readable file-like object (request.rfile).
            wfile: Writable file-like object (request.wfile).
            connection_id: Unique ID for this connection.
            conversation_id: Associated conversation ID (for chat streams).
        """
        self.rfile = rfile
        self.wfile = wfile
        self.connection_id = connection_id
        self.conversation_id = conversation_id
        self.closed = False
        self._write_lock = threading.Lock()
        self._created_at = time.time()

    def send_text(self, data: str) -> bool:
        """Send a text frame to the client.

        Args:
            data: Text payload to send.

        Returns:
            True if sent successfully, False if connection is broken.
        """
        if self.closed:
            return False
        try:
            frame = _encode_frame(data.encode("utf-8"), WS_OPCODE_TEXT)
            with self._write_lock:
                self.wfile.write(frame)
                self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as exc:
            logger.debug("WebSocket send failed (conn=%s): %s", self.connection_id, exc)
            self.closed = True
            return False

    def send_json(self, obj: dict | list) -> bool:
        """Send a JSON-serialized text frame.

        Args:
            obj: JSON-serializable object.

        Returns:
            True if sent successfully, False if connection is broken.
        """
        return self.send_text(json.dumps(obj))

    def recv(self, timeout: float = 30.0) -> Optional[str]:
        """Receive a text frame from the client.

        Handles ping/pong automatically.  Returns None on close or timeout.

        Args:
            timeout: Seconds to wait for data before returning None.

        Returns:
            Decoded text payload, or None on close/timeout.
        """
        if self.closed:
            return None
        try:
            # Set socket timeout for blocking reads
            raw_sock = self.rfile.raw if hasattr(self.rfile, "raw") else self.rfile
            if hasattr(raw_sock, "settimeout"):
                raw_sock.settimeout(timeout)
            elif hasattr(self.rfile, "_sock"):
                self.rfile._sock.settimeout(timeout)

            opcode, payload = _decode_frame(self.rfile)

            if opcode == WS_OPCODE_TEXT:
                return payload.decode("utf-8")
            elif opcode == WS_OPCODE_PING:
                # Respond with pong
                pong_frame = _encode_frame(payload, WS_OPCODE_PONG)
                with self._write_lock:
                    self.wfile.write(pong_frame)
                    self.wfile.flush()
                # Recurse to get actual data
                return self.recv(timeout)
            elif opcode == WS_OPCODE_PONG:
                # Ignore pong frames, recurse
                return self.recv(timeout)
            elif opcode == WS_OPCODE_CLOSE:
                self._send_close()
                self.closed = True
                return None
            else:
                # Unknown opcode -- ignore
                return self.recv(timeout)
        except (TimeoutError, OSError, WebSocketError) as exc:
            logger.debug("WebSocket recv ended (conn=%s): %s", self.connection_id, exc)
            self.closed = True
            return None

    def send_ping(self) -> bool:
        """Send a ping frame to keep the connection alive.

        Returns:
            True if sent successfully, False otherwise.
        """
        if self.closed:
            return False
        try:
            frame = _encode_frame(b"", WS_OPCODE_PING)
            with self._write_lock:
                self.wfile.write(frame)
                self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.closed = True
            return False

    def close(self) -> None:
        """Send close frame and mark connection as closed."""
        if not self.closed:
            self._send_close()
            self.closed = True
        _unregister_ws(self.connection_id)

    def _send_close(self) -> None:
        """Send a WebSocket close frame."""
        try:
            frame = _encode_frame(b"", WS_OPCODE_CLOSE)
            with self._write_lock:
                self.wfile.write(frame)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


# ── Frame encoding/decoding (RFC 6455) ──


def _encode_frame(payload: bytes, opcode: int) -> bytes:
    """Encode a WebSocket frame (server -> client, unmasked).

    Args:
        payload: Raw payload bytes.
        opcode: WebSocket opcode (text=0x1, binary=0x2, close=0x8, etc.).

    Returns:
        Encoded frame bytes ready to write to socket.
    """
    frame = bytearray()
    # FIN bit + opcode
    frame.append(0x80 | opcode)

    length = len(payload)
    if length < 126:
        frame.append(length)
    elif length < 65536:
        frame.append(126)
        frame.extend(struct.pack("!H", length))
    else:
        frame.append(127)
        frame.extend(struct.pack("!Q", length))

    frame.extend(payload)
    return bytes(frame)


def _decode_frame(rfile) -> tuple[int, bytes]:
    """Decode a WebSocket frame from the client (client -> server, masked).

    Args:
        rfile: Readable file-like object.

    Returns:
        Tuple of (opcode, payload_bytes).

    Raises:
        WebSocketError: On protocol violation or connection close.
    """
    # Read first 2 bytes
    header = _read_exact(rfile, 2)
    if len(header) < 2:
        raise WebSocketError("Connection closed during frame header read")

    fin = (header[0] >> 7) & 1
    opcode = header[0] & 0x0F
    masked = (header[1] >> 7) & 1
    payload_len = header[1] & 0x7F

    # Extended payload length
    if payload_len == 126:
        ext = _read_exact(rfile, 2)
        payload_len = struct.unpack("!H", ext)[0]
    elif payload_len == 127:
        ext = _read_exact(rfile, 8)
        payload_len = struct.unpack("!Q", ext)[0]

    if payload_len > WS_MAX_PAYLOAD:
        raise WebSocketError(f"Payload too large: {payload_len} bytes")

    # Masking key (client frames MUST be masked per RFC 6455)
    mask_key = b""
    if masked:
        mask_key = _read_exact(rfile, 4)

    # Payload
    payload = _read_exact(rfile, payload_len) if payload_len > 0 else b""

    # Unmask if needed
    if masked and mask_key and payload:
        payload = _unmask(payload, mask_key)

    return opcode, payload


def _unmask(data: bytes, mask: bytes) -> bytes:
    """Unmask WebSocket payload using XOR with 4-byte mask key.

    Args:
        data: Masked payload bytes.
        mask: 4-byte masking key.

    Returns:
        Unmasked payload bytes.
    """
    return bytes(b ^ mask[i % 4] for i, b in enumerate(data))


def _read_exact(rfile, n: int) -> bytes:
    """Read exactly n bytes from a file-like object.

    Args:
        rfile: Readable file-like object.
        n: Number of bytes to read.

    Returns:
        Bytes read (may be shorter if EOF).
    """
    data = b""
    while len(data) < n:
        chunk = rfile.read(n - len(data))
        if not chunk:
            break
        data += chunk
    return data


# ── Handshake ──


def ws_handshake(handler) -> Optional[WebSocketConnection]:
    """Perform WebSocket upgrade handshake on a BaseHTTPRequestHandler.

    Validates the Upgrade header, computes Sec-WebSocket-Accept, and
    sends the 101 Switching Protocols response.

    Args:
        handler: A BaseHTTPRequestHandler instance (self in do_GET).

    Returns:
        WebSocketConnection if handshake succeeds, None on failure.
    """
    # Validate required headers
    upgrade = handler.headers.get("Upgrade", "").lower()
    connection = handler.headers.get("Connection", "").lower()
    ws_key = handler.headers.get("Sec-WebSocket-Key", "")
    ws_version = handler.headers.get("Sec-WebSocket-Version", "")

    if "websocket" not in upgrade:
        handler.send_error(400, "Missing or invalid Upgrade header")
        return None

    if "upgrade" not in connection:
        handler.send_error(400, "Missing or invalid Connection header")
        return None

    if not ws_key:
        handler.send_error(400, "Missing Sec-WebSocket-Key header")
        return None

    if ws_version != "13":
        handler.send_error(400, f"Unsupported WebSocket version: {ws_version}")
        return None

    # Check connection limit
    with _ws_connections_lock:
        if len(_ws_connections) >= _WS_MAX_CONNECTIONS:
            handler.send_error(503, "WebSocket connection limit reached")
            return None

    # Compute accept key per RFC 6455
    accept_key = base64.b64encode(
        hashlib.sha1(ws_key.encode("utf-8") + _WS_MAGIC).digest()
    ).decode("utf-8")

    # Send 101 Switching Protocols
    handler.send_response(101, "Switching Protocols")
    handler.send_header("Upgrade", "websocket")
    handler.send_header("Connection", "Upgrade")
    handler.send_header("Sec-WebSocket-Accept", accept_key)
    # CORS
    cors_origin = getattr(handler, "_get_cors_origin", lambda: None)()
    if cors_origin:
        handler.send_header("Access-Control-Allow-Origin", cors_origin)
    handler.end_headers()

    # Create connection object
    conn_id = f"ws-{int(time.time() * 1000)}-{id(handler)}"
    ws_conn = WebSocketConnection(
        rfile=handler.rfile,
        wfile=handler.wfile,
        connection_id=conn_id,
    )
    _register_ws(conn_id, ws_conn)

    logger.info("WebSocket connection established: %s", conn_id)
    return ws_conn


# ── Connection registry ──


def _register_ws(conn_id: str, conn: WebSocketConnection) -> None:
    """Register a WebSocket connection in the global registry.

    Args:
        conn_id: Unique connection identifier.
        conn: WebSocketConnection instance.
    """
    with _ws_connections_lock:
        _ws_connections[conn_id] = conn


def _unregister_ws(conn_id: str) -> None:
    """Remove a WebSocket connection from the global registry.

    Args:
        conn_id: Unique connection identifier.
    """
    with _ws_connections_lock:
        _ws_connections.pop(conn_id, None)


def get_ws_connection_count() -> int:
    """Return the number of active WebSocket connections.

    Returns:
        Current count of registered WebSocket connections.
    """
    with _ws_connections_lock:
        return len(_ws_connections)


def broadcast_ws(message: str, filter_fn: Optional[Callable] = None) -> int:
    """Broadcast a text message to all (or filtered) WebSocket connections.

    Args:
        message: Text message to broadcast.
        filter_fn: Optional callable(WebSocketConnection) -> bool to filter recipients.

    Returns:
        Number of connections the message was sent to.
    """
    sent = 0
    dead_ids: list[str] = []

    with _ws_connections_lock:
        conns = list(_ws_connections.items())

    for conn_id, conn in conns:
        if conn.closed:
            dead_ids.append(conn_id)
            continue
        if filter_fn and not filter_fn(conn):
            continue
        if conn.send_text(message):
            sent += 1
        else:
            dead_ids.append(conn_id)

    # Clean up dead connections
    if dead_ids:
        with _ws_connections_lock:
            for cid in dead_ids:
                _ws_connections.pop(cid, None)

    return sent
