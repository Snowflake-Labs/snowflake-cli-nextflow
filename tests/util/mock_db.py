from unittest.mock import MagicMock
from snowflake.connector.cursor import SnowflakeCursor

class MockDB:
    def __init__(self):
        self.query_results = {}
        self.executed_queries = []
        self.mock_cursor = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_conn.execute_stream.side_effect = self._execute_stream

    def register_query(self, query_match, result):
        """Register a query and the result it should return from fetchone/fetchall."""
        self.query_results[query_match] = result

    def get_executed_queries(self):
        """Return list of all executed queries with their parameters."""
        return self.executed_queries.copy()

    def clear_executed_queries(self):
        """Clear the list of recorded executed queries."""
        self.executed_queries.clear()

    def _execute_stream(self, query, remove_comments=False, cursor_class=SnowflakeCursor, **kwargs):
        # Record the executed query
        self.executed_queries.append(query.getvalue())
        
        # Create a new cursor for streaming
        stream_cursor = MagicMock()
        
        # You can make this smarter (e.g. fuzzy matching, regex, params)
        result = self.query_results.get(query)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, list):
            stream_cursor.fetchall.return_value = result
            stream_cursor.fetchone.return_value = result[0] if result else None
        else:
            stream_cursor.fetchone.return_value = result
            stream_cursor.fetchall.return_value = [result] if result else []
        
        return [stream_cursor]

    def get_connection(self):
        return self.mock_conn
