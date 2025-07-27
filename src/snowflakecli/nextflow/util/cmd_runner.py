from typing import Callable, List, Optional
import subprocess
import os
from snowflake.cli.api.exceptions import CliError


class CommandRunner:
    def __init__(self):
        self.stdout_callback: Optional[Callable[[str], None]] = None
        self.stderr_callback: Optional[Callable[[str], None]] = None

    def set_stdout_callback(self, callback: Callable[[str], None]):
        self.stdout_callback = callback
        return self

    def set_stderr_callback(self, callback: Callable[[str], None]):
        self.stderr_callback = callback
        return self

    def run(self, cmd: List[str]) -> int:
        try:
            env = os.environ.copy()
            process = subprocess.Popen(
                " ".join(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                shell=True,
                env=env,
            )

            # Process stdout
            if self.stdout_callback:
                for line in process.stdout:
                    self.stdout_callback(line.rstrip("\n"))

            # Process stderr
            if self.stderr_callback:
                for line in process.stderr:
                    self.stderr_callback(line.rstrip("\n"))

            return process.wait()

        except FileNotFoundError:
            raise CliError(f"Command not found: {cmd[0]}")
