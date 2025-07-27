"""
Mock CommandRunner for testing NextflowManager without executing actual commands.
"""

from typing import Callable, Dict, List, Optional


class MockCommandRunner:
    """
    Mock CommandRunner that simulates nextflow config command output for testing.

    This mock allows tests to:
    - Control the output of nextflow config commands
    - Simulate success/failure scenarios
    - Verify which commands were executed
    - Test error handling paths
    """

    def __init__(self, mock_config_output: Optional[Dict[str, str]] = None, return_code: int = 0):
        """
        Initialize the mock command runner.

        Args:
            mock_config_output: Dictionary of config key-value pairs to simulate
            return_code: Exit code to return from run() method (0 = success, non-zero = error)
        """
        self.mock_config_output = mock_config_output or {
            "snowflake.computePool": "test_pool",
            "snowflake.workDirStage": "test_stage",
            "snowflake.stageMounts": "",
            "snowflake.enableStageMountV2": "false",
        }
        self.return_code = return_code
        self.stdout_callback: Optional[Callable[[str], None]] = None
        self.stderr_callback: Optional[Callable[[str], None]] = None
        self.last_commands: Optional[List[str]] = None
        self.call_count = 0

    def set_stdout_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback function for stdout output."""
        self.stdout_callback = callback

    def set_stderr_callback(self, callback: Callable[[str], None]) -> None:
        """Set callback function for stderr output."""
        self.stderr_callback = callback

    def run(self, commands: List[str]) -> int:
        """
        Simulate running a command.

        Args:
            commands: List of command arguments (e.g., ["nextflow", "config", "project", "-flat"])

        Returns:
            The configured return code
        """
        self.last_commands = commands.copy()
        self.call_count += 1

        if self.return_code == 0:
            # Simulate successful config output
            for key, value in self.mock_config_output.items():
                if self.stdout_callback:
                    self.stdout_callback(f"{key} = '{value}'")
        else:
            # Simulate error
            if self.stderr_callback:
                self.stderr_callback("Error: Failed to parse nextflow config")

        return self.return_code

    def reset(self) -> None:
        """Reset the mock state for reuse in multiple tests."""
        self.last_commands = None
        self.call_count = 0
        self.stdout_callback = None
        self.stderr_callback = None
