import pytest
import subprocess
from unittest.mock import Mock, patch
from snowflake.cli.api.exceptions import CliError

from snowflakecli.nextflow.util.cmd_runner import CommandRunner


class TestCommandRunner:
    """Test suite for CommandRunner class."""

    def test_init(self):
        """Test CommandRunner initialization."""
        runner = CommandRunner()
        assert runner.stdout_callback is None
        assert runner.stderr_callback is None

    def test_set_stdout_callback(self):
        """Test setting stdout callback."""
        runner = CommandRunner()
        mock_callback = Mock()

        result = runner.set_stdout_callback(mock_callback)

        assert runner.stdout_callback == mock_callback
        assert result == runner  # Should return self for chaining

    def test_set_stderr_callback(self):
        """Test setting stderr callback."""
        runner = CommandRunner()
        mock_callback = Mock()

        result = runner.set_stderr_callback(mock_callback)

        assert runner.stderr_callback == mock_callback
        assert result == runner  # Should return self for chaining

    def test_method_chaining(self):
        """Test that callback setters can be chained."""
        runner = CommandRunner()
        stdout_callback = Mock()
        stderr_callback = Mock()

        result = runner.set_stdout_callback(stdout_callback).set_stderr_callback(stderr_callback)

        assert runner.stdout_callback == stdout_callback
        assert runner.stderr_callback == stderr_callback
        assert result == runner

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_successful_command(self, mock_environ, mock_popen):
        """Test successful command execution."""
        # Setup mocks
        mock_environ.copy.return_value = {"TEST": "value"}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = []
        mock_process.stderr = []
        mock_popen.return_value = mock_process

        runner = CommandRunner()
        result = runner.run(["echo", "hello"])

        assert result == 0
        mock_popen.assert_called_once_with(
            "echo hello",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            shell=True,
            env={"TEST": "value"},
        )

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_command_with_non_zero_exit_code(self, mock_environ, mock_popen):
        """Test command execution with non-zero exit code."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 1
        mock_process.stdout = []
        mock_process.stderr = []
        mock_popen.return_value = mock_process

        runner = CommandRunner()
        result = runner.run(["false"])

        assert result == 1

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_with_stdout_callback(self, mock_environ, mock_popen):
        """Test command execution with stdout callback."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = ["line1\n", "line2\n", "line3"]
        mock_process.stderr = []
        mock_popen.return_value = mock_process

        stdout_callback = Mock()
        runner = CommandRunner()
        runner.set_stdout_callback(stdout_callback)

        result = runner.run(["echo", "test"])

        assert result == 0
        # Verify callback was called for each line with stripped newlines
        expected_calls = [(("line1",),), (("line2",),), (("line3",),)]
        assert stdout_callback.call_args_list == expected_calls

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_with_stderr_callback(self, mock_environ, mock_popen):
        """Test command execution with stderr callback."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = []
        mock_process.stderr = ["error1\n", "error2\n"]
        mock_popen.return_value = mock_process

        stderr_callback = Mock()
        runner = CommandRunner()
        runner.set_stderr_callback(stderr_callback)

        result = runner.run(["some", "command"])

        assert result == 0
        # Verify callback was called for each line with stripped newlines
        expected_calls = [(("error1",),), (("error2",),)]
        assert stderr_callback.call_args_list == expected_calls

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_with_both_callbacks(self, mock_environ, mock_popen):
        """Test command execution with both stdout and stderr callbacks."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = ["stdout_line\n"]
        mock_process.stderr = ["stderr_line\n"]
        mock_popen.return_value = mock_process

        stdout_callback = Mock()
        stderr_callback = Mock()
        runner = CommandRunner()
        runner.set_stdout_callback(stdout_callback).set_stderr_callback(stderr_callback)

        result = runner.run(["test", "command"])

        assert result == 0
        stdout_callback.assert_called_once_with("stdout_line")
        stderr_callback.assert_called_once_with("stderr_line")

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_without_callbacks(self, mock_environ, mock_popen):
        """Test command execution without any callbacks."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = ["some output\n"]
        mock_process.stderr = ["some error\n"]
        mock_popen.return_value = mock_process

        runner = CommandRunner()
        result = runner.run(["test", "command"])

        assert result == 0
        # Since no callbacks are set, the stdout/stderr should be processed but no callbacks called

    @patch("subprocess.Popen")
    def test_run_file_not_found_error(self, mock_popen):
        """Test FileNotFoundError handling."""
        # Setup mock to raise FileNotFoundError
        mock_popen.side_effect = FileNotFoundError()

        runner = CommandRunner()

        with pytest.raises(CliError) as exc_info:
            runner.run(["nonexistent_command", "arg1"])

        assert "Command not found: nonexistent_command" in str(exc_info.value)

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_preserves_environment(self, mock_environ, mock_popen):
        """Test that the command runs with a copy of the current environment."""
        # Setup mocks
        test_env = {"PATH": "/usr/bin", "HOME": "/home/user", "CUSTOM_VAR": "test"}
        mock_environ.copy.return_value = test_env
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = []
        mock_process.stderr = []
        mock_popen.return_value = mock_process

        runner = CommandRunner()
        runner.run(["env"])

        # Verify that os.environ.copy() was called and the result was passed to Popen
        mock_environ.copy.assert_called_once()
        mock_popen.assert_called_once()
        call_kwargs = mock_popen.call_args[1]
        assert call_kwargs["env"] == test_env

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_command_joining(self, mock_environ, mock_popen):
        """Test that command list is properly joined into a string."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = []
        mock_process.stderr = []
        mock_popen.return_value = mock_process

        runner = CommandRunner()
        cmd = ["python", "-c", "print('hello world')", "--verbose"]
        runner.run(cmd)

        # Verify the command was joined with spaces
        expected_cmd = "python -c print('hello world') --verbose"
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0]
        assert call_args[0] == expected_cmd

    @patch("subprocess.Popen")
    @patch("os.environ")
    def test_run_subprocess_options(self, mock_environ, mock_popen):
        """Test that subprocess.Popen is called with correct options."""
        # Setup mocks
        mock_environ.copy.return_value = {}
        mock_process = Mock()
        mock_process.wait.return_value = 0
        mock_process.stdout = []
        mock_process.stderr = []
        mock_popen.return_value = mock_process

        runner = CommandRunner()
        runner.run(["echo", "test"])

        # Verify all the subprocess options
        mock_popen.assert_called_once()
        call_kwargs = mock_popen.call_args[1]
        assert call_kwargs["stdout"] == subprocess.PIPE
        assert call_kwargs["stderr"] == subprocess.PIPE
        assert call_kwargs["text"] is True
        assert call_kwargs["bufsize"] == 1
        assert call_kwargs["shell"] is True
