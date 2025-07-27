"""
WebSocket client for connecting to Nextflow PTY servers in Snowpark Container Services.

This module provides a WebSocketClient class that handles secure WebSocket connections
with Snowflake authentication for streaming real-time output from Nextflow processes.
"""

import websockets
from websockets.exceptions import ConnectionClosed, InvalidURI, InvalidHandshake
import json
import ssl
from typing import Callable, Optional, Dict, Any
from .websocket_exceptions import (
    WebSocketConnectionError,
    WebSocketAuthenticationError,
    WebSocketInvalidURIError,
    WebSocketServerError,
)


class WebSocketClient:
    """
    WebSocket client for connecting to Nextflow PTY servers with Snowflake authentication.

    This class is decoupled from CLI dependencies and uses callback patterns for
    output and error handling.
    """

    def __init__(
        self,
        conn,
        message_callback: Optional[Callable[[str], None]] = None,
        status_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        error_callback: Optional[Callable[[str, Exception], None]] = None,
    ):
        """
        Initialize WebSocket client.

        Args:
            conn: Snowflake connection object
            message_callback: Callback for output messages (type, data)
            status_callback: Callback for status updates (status, data)
            error_callback: Callback for error handling (message, exception)
        """
        self.conn = conn
        self.message_callback = message_callback or self._default_message_callback
        self.status_callback = status_callback or self._default_status_callback
        self.error_callback = error_callback or self._default_error_callback
        self.exit_code = None  # Track the exit code

    def _default_message_callback(self, message: str) -> None:
        """Default message callback - just print"""
        print(message, end="")

    def _default_status_callback(self, status: str, data: Dict[str, Any]) -> None:
        """Default status callback - just print"""
        print(f"Status: {status} - {data}")

    def _default_error_callback(self, message: str, exception: Exception) -> None:
        """Default error callback - just print"""
        print(f"Error: {message}")

    def _get_auth_token(self) -> str:
        """Get Snowflake session token for authentication"""
        try:
            self.conn.cursor().execute("alter session set python_connector_query_result_format = 'json'")
            token_data = self.conn._rest._token_request("ISSUE")
            return token_data["data"]["sessionToken"]
        except Exception as e:
            raise WebSocketAuthenticationError(f"Failed to get authentication token: {e}")

    async def connect_and_stream(self, server_url: str) -> Optional[int]:
        """
        Connect to WebSocket server and stream messages.

        Args:
            server_url: WebSocket server URL

        Returns:
            Exit code if process completed successfully, None otherwise

        Raises:
            WebSocketInvalidURIError: If URL is invalid
            WebSocketAuthenticationError: If authentication fails
            WebSocketConnectionError: If connection fails
            WebSocketServerError: If server returns error
        """
        try:
            # Get authentication token
            token = self._get_auth_token()

            # Prepare headers for authentication
            headers = {"Authorization": f'Snowflake Token="{token}"'}

            # Create SSL context for wss connection
            ssl_context = ssl.create_default_context()

            # Connect to the WebSocket server
            try:
                async with websockets.connect(server_url, additional_headers=headers, ssl=ssl_context) as websocket:
                    self.status_callback("connected", {"url": server_url})

                    try:
                        # Continuously read messages from the server
                        async for message in websocket:
                            await self._handle_message(message)
                            # If we received a completion status, we can break
                            if self.exit_code is not None:
                                break

                    except ConnectionClosed:
                        self.status_callback("disconnected", {"reason": "Connection closed by server"})
                    except KeyboardInterrupt:
                        self.status_callback("disconnected", {"reason": "Disconnected by user"})
                        raise

            except InvalidHandshake as e:
                if "401" in str(e):
                    raise WebSocketAuthenticationError("Authentication failed. Check your Snowflake connection.")
                else:
                    raise WebSocketConnectionError(f"Connection handshake failed: {e}")

        except InvalidURI:
            raise WebSocketInvalidURIError(f"Invalid WebSocket URL: {server_url}")
        except (WebSocketAuthenticationError, WebSocketConnectionError, WebSocketInvalidURIError):
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            raise WebSocketConnectionError(f"Unexpected error connecting to server: {e}")

        return self.exit_code

    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message"""
        try:
            # Try to parse as JSON
            data = json.loads(message)

            # Handle different message types
            msg_type = data.get("type", "unknown")

            if msg_type == "output":
                # Output data from Nextflow
                output_data = data.get("data", "")
                self.message_callback(output_data)

            elif msg_type == "status":
                # Status updates
                status = data.get("status", "")
                status_data = {k: v for k, v in data.items() if k != "type"}
                self.status_callback(status, status_data)

                # Capture exit code if process completed
                if status == "completed":
                    self.exit_code = data.get("exit_code", 0)

            elif msg_type == "error":
                # Server error message
                error_msg = data.get("message", "Unknown server error")
                error_code = data.get("code")
                error_data = data.get("data", {})
                raise WebSocketServerError(error_msg, error_code, error_data)

            else:
                # Unknown message type, pass raw message
                self.message_callback(f"Unknown message type '{msg_type}': {message}\n")

        except json.JSONDecodeError:
            # If not JSON, treat as raw output
            self.message_callback(message)
        except WebSocketServerError:
            # Re-raise server errors
            raise
        except Exception as e:
            # Handle other parsing errors
            self.error_callback(f"Error processing message: {message}", e)
