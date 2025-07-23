from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor, DictCursor
from snowflakecli.nextflow.util.cmd_runner import CommandRunner
from snowflakecli.nextflow.service_spec import (
    Specification, Spec, Container, parse_stage_mounts, VolumeConfig, VolumeMount, Volume, Endpoint
)
from dataclasses import dataclass
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.console import cli_console as cc
import os
import tarfile
import tempfile
from pathlib import Path
import random
import string
from datetime import datetime
import json
import asyncio
from snowflakecli.nextflow.wss import (
    WebSocketClient,
    WebSocketError,
    WebSocketConnectionError,
    WebSocketAuthenticationError,
    WebSocketInvalidURIError,
    WebSocketServerError
)
from typing import Optional

@dataclass
class ProjectConfig:
    computePool: str = ""
    workDirStage: str = ""
    volumeConfig: VolumeConfig = None

class NextflowManager(SqlExecutionMixin):

    def __init__(self, project_dir: str, profile: str = None, nf_snowflake_image: str = None):
        super().__init__()
        self._project_dir = Path(project_dir)

        if not self._project_dir.exists() or not self._project_dir.is_dir():
            raise CliError(f"Invalid project directory '{project_dir}'")

        self._profile = profile
        self._nf_snowflake_image = nf_snowflake_image
        
        # Generate random alphanumeric runtime ID using UTC timestamp and random seed
        utc_timestamp = int(datetime.now().timestamp())
        random.seed(utc_timestamp)
        
        # Generate 8-character runtime ID that complies with Nextflow naming requirements
        # Must start with lowercase letter, followed by lowercase letters and digits
        first_char = random.choice(string.ascii_lowercase)
        remaining_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=7))
        self._run_id = first_char + remaining_chars
        self.service_name = f"NXF_MAIN_{self._run_id}"

    def _parse_config(self) -> ProjectConfig:
        """
        Parse the nextflow.config file and return a ProjectConfig object.
        """

        config = ProjectConfig()

        def parse_config_line(line: str) -> None:
            key, val = line.split(" = ")
            if key == "snowflake.computePool":
                config.computePool = val.strip().replace("'", "")
            elif key == "snowflake.stageMounts":
                config.volumeConfig = parse_stage_mounts(val.strip().replace("'", ""))
            elif key == "snowflake.workDirStage":
                config.workDirStage = val.strip().replace("'", "")

        stderr = []
        def collect_stderr(line: str) -> None:
            stderr.append(line)

        runner = CommandRunner()
        runner.set_stdout_callback(parse_config_line)
        runner.set_stderr_callback(collect_stderr)
        cmds = ["nextflow", "config", self._project_dir.name, "-flat"]
        if self._profile:
            cmds+=["-profile", self._profile]

        ret = runner.run(cmds)
        if ret != 0:
            err_msg = "Failed to parse nextflow.config\n"
            err_msg += "\n".join(stderr)
            raise CliError(err_msg)

        
        return config

    def _upload_project(self, config: ProjectConfig) -> str:
        """
        Create a tarball of the project directory and upload to Snowflake stage.
        """
        
        # Create temporary file for the tarball
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as temp_file:
            temp_tarball_path = temp_file.name
        
        try:
            cc.step("Creating tarball...")
            # Create tarball excluding .git directory
            self._create_tarball(self._project_dir, temp_tarball_path)
            
            cc.step(f"Uploading to stage {config.workDirStage}...")
            # Upload to Snowflake stage
            self.execute_query(f"PUT file://{temp_tarball_path} @{config.workDirStage}/{self._run_id}")

            return temp_tarball_path
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_tarball_path):
                os.unlink(temp_tarball_path)
    
    def _create_tarball(self, project_path: Path, tarball_path: str):
        """
        Create a tarball of the project directory, excluding .git and other unwanted files.
        
        Args:
            project_path: Path to the project directory
            tarball_path: Path where the tarball should be created
        """
        
        def tar_filter(tarinfo):
            """Filter function to exclude unwanted files/directories"""
            # Exclude other common unwanted files/directories
            excluded_patterns = [
                '.git',
                '.gitignore',
            ]
            
            for pattern in excluded_patterns:
                if pattern in tarinfo.name:
                    return None
            
            return tarinfo
        
        try:
            with tarfile.open(tarball_path, 'w:gz') as tar:
                # Add all files from project directory with filtering
                tar.add(
                    project_path, 
                    arcname=project_path.name,  # Use project name as root in archive
                    filter=tar_filter
                )
                
        except Exception as e:
            raise CliError(f"Failed to create tarball: {str(e)}")
        
    def _stream_service_logs(self, service_name: str) -> Optional[int]:
        """
        Connect to service WebSocket endpoint and stream logs.
        
        Args:
            service_name: Name of the service to connect to
            
        Returns:
            Exit code if execution completed successfully, None otherwise
        """
        # Get WebSocket endpoint
        cursor = self.execute_query(f"show endpoints in service {service_name}", cursor_class=DictCursor)
        wss_url = cursor.fetchone()["ingress_url"]
        
        # Callback functions for WebSocket events
        def on_message(message: str) -> None:
            print(message, end='')
        
        def on_status(status: str, data: dict) -> None:
            if status == 'starting':
                cc.step(f"Starting: {data.get('command', '')}")
            elif status == 'started':
                cc.step(f"Started with PID: {data.get('pid', '')}")
            elif status == 'connected':
                cc.step(f"Connected to WebSocket server: {wss_url}")
                cc.step("Streaming live output... (Press Ctrl+C to stop)")
                cc.step("=" * 50)
            elif status == 'disconnected':
                cc.step(f"Disconnected: {data.get('reason', '')}")
        
        def on_error(message: str, exception: Exception) -> None:
            cc.warning(f"Processing error: {message}")
        
        exit_code = None
        # Create WebSocket client and connect
        try:
            wss_client = WebSocketClient(
                conn=self._conn,
                message_callback=on_message,
                status_callback=on_status,
                error_callback=on_error
            )
            exit_code = asyncio.run(wss_client.connect_and_stream("wss://"+wss_url))
        except WebSocketInvalidURIError as e:
            raise CliError(f"Invalid WebSocket URL: {e}")
        except WebSocketAuthenticationError as e:
            raise CliError(f"Authentication failed: {e}")
        except WebSocketConnectionError as e:
            raise CliError(f"Connection failed: {e}")
        except WebSocketServerError as e:
            error_msg = f"Server error: {e}"
            if e.error_code:
                error_msg += f" (Code: {e.error_code})"
            raise CliError(error_msg)
        except WebSocketError as e:
            raise CliError(f"WebSocket error: {e}")
        except KeyboardInterrupt:
            cc.step("Disconnected by user")
        
        return exit_code

    def _submit_nextflow_job(self, config: ProjectConfig, tarball_path: str, is_async: bool) -> SnowflakeCursor:
        """
        Run the nextflow pipeline.
        
        Returns:
            Exit code if execution completed successfully, None otherwise
        """
        tags = json.dumps({
            "NEXTFLOW_JOB_TYPE": "main",
            "NEXTFLOW_RUN_ID": self._run_id,
        })

        self.execute_query(f"alter session set query_tag = '{tags}'")

        workDir = "/mnt/workdir"
        tarball_filename = os.path.basename(tarball_path)

        nf_run_cmds = [
            "nextflow",
            "run",
            ".",
            "-name",
            self._run_id,
            "-ansi-log",
            str(not is_async),
            "-profile",
            self._profile,
            "-work-dir",
            workDir,
            "-with-report",
            workDir+"/report.html",
            "-with-trace",
            workDir+"/trace.txt",
            "-with-timeline",
            workDir+"/timeline.html",
        ]
        
        run_script = f"""
        mkdir -p /mnt/project
        cd /mnt/project
        tar -zxf {workDir}/{tarball_filename}
        """
        if not is_async:
            run_script += "python3 /app/pty_server.py -- "
        run_script += (" ".join(nf_run_cmds) + "\n")

        config.volumeConfig.volumeMounts.append(
            VolumeMount(name="workdir", mountPath=workDir)
        )
        config.volumeConfig.volumes.append(
            Volume(name="workdir", source="@"+config.workDirStage+"/"+self._run_id+"/")
        )

        endpoints = None if is_async else [
            Endpoint(name="wss", port=8765, public=True)
        ]

        spec = Specification(
            spec = Spec(
                containers = [
                    Container(
                        name="nf-main",
                        image=self._nf_snowflake_image,
                        command=["/bin/bash", "-c", run_script],
                        volumeMounts=config.volumeConfig.volumeMounts
                    )
                ],
                volumes = config.volumeConfig.volumes,
                endpoints = endpoints
            )
        )
        
        # Get YAML string for inline spec
        yaml_spec = spec.to_yaml()

        if is_async:
            execute_sql = f"""
EXECUTE JOB SERVICE
IN COMPUTE POOL {config.computePool}
NAME = {self.service_name}
FROM SPECIFICATION $$
{yaml_spec}
$$
            """
        else:
            execute_sql = f"""
CREATE SERVICE {self.service_name}
IN COMPUTE POOL {config.computePool}
FROM SPECIFICATION $$
{yaml_spec}
$$
        """
        return self.execute_query(execute_sql, _exec_async=is_async)

    def run_async(self) -> str:
        """
        Run a Nextflow workflow asynchronously.
        
        Returns:
            Service name for monitoring the async execution
        """
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project to Snowflake..."):
            tarball_path = self._upload_project(config)

        cc.step("Submitting nextflow job to Snowflake...")
        cursor = self._submit_nextflow_job(config, tarball_path, True)
        return cursor.sfqid

    def run(self) -> Optional[int]:
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project to Snowflake..."):
            tarball_path = self._upload_project(config)

        cc.step("Submitting nextflow job to Snowflake...")
        cursor = self._submit_nextflow_job(config, tarball_path, False)
        cc.step(f"Nextflow job submitted successfully as service: {self.service_name}, query_id: {cursor.sfqid}")

        try: 
            self.execute_query(f"call system$wait_for_services(30, '{self.service_name}')")
            self.execute_query("alter session unset query_tag")

            # Stream logs and get exit code
            exit_code = self._stream_service_logs(self.service_name)
            return exit_code
        finally:
            self.execute_query("drop service if exists "+self.service_name)
