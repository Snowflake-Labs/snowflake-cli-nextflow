"""
Custom exceptions for WebSocket operations.
"""


class WebSocketError(Exception):
    """Base exception for WebSocket operations"""

    pass


class WebSocketConnectionError(WebSocketError):
    """Raised when connection to WebSocket server fails"""

    pass


class WebSocketAuthenticationError(WebSocketError):
    """Raised when authentication with WebSocket server fails"""

    pass


class WebSocketInvalidURIError(WebSocketError):
    """Raised when WebSocket URI is invalid"""

    pass


class WebSocketServerError(WebSocketError):
    """Raised when server returns an error message"""

    def __init__(self, message: str, error_code: str = None, error_data: dict = None):
        super().__init__(message)
        self.error_code = error_code
        self.error_data = error_data or {}
