"""
WebSocket client package for Snowflake Nextflow integration.

This package provides WebSocket client functionality for connecting to
Nextflow PTY servers in Snowpark Container Services.
"""

from .websocket_client import WebSocketClient
from .websocket_exceptions import (
    WebSocketError,
    WebSocketConnectionError,
    WebSocketAuthenticationError,
    WebSocketInvalidURIError,
    WebSocketServerError,
)

__all__ = [
    "WebSocketClient",
    "WebSocketError",
    "WebSocketConnectionError",
    "WebSocketAuthenticationError",
    "WebSocketInvalidURIError",
    "WebSocketServerError",
]
