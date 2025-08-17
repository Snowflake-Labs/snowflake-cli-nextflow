from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor, DictCursor
from snowflakecli.nextflow.service_spec import (
    Specification,
    Spec,
    Container,
    parse_stage_mounts,
    VolumeConfig,
    VolumeMount,
    Volume,
    Endpoint,
    StageConfig,
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
import re
from snowflakecli.nextflow.wss import (
    WebSocketClient,
    WebSocketError,
    WebSocketConnectionError,
    WebSocketAuthenticationError,
    WebSocketInvalidURIError,
    WebSocketServerError,
)
from typing import Optional, Callable
from snowflakecli.nextflow.parser import NextflowConfigParser


@dataclass
class ProjectConfig:
    computePool: str = ""
    workDirStage: str = ""
    volumeConfig: VolumeConfig = None
    enableStageMountV2: bool = True


class NextflowManager(SqlExecutionMixin):
    def __init__(
        self,
        project_dir: str,
        profile: str = None,
        nf_snowflake_image: str = None,
        id_generator: Callable[[], str] = None,
        temp_file_generator: Callable[[str], str] = None,
    ):
        super().__init__()
        self._project_dir = Path(project_dir)

        if not self._project_dir.exists() or not self._project_dir.is_dir():
            raise CliError(f"Invalid project directory '{project_dir}'")

        self._profile = profile
        self._nf_snowflake_image = nf_snowflake_image

        # Use injected temp file generator or default one
        self._temp_file_generator = temp_file_generator or self._default_temp_file_generator

        # Use injected ID generator or default one
        self._run_id = id_generator() if id_generator else self._generate_run_id()
        self.service_name = f"NXF_MAIN_{self._run_id}"

    def _default_temp_file_generator(self, suffix: str) -> str:
        """
        Default temporary file generator using tempfile.NamedTemporaryFile.
        """
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
            return temp_file.name

    def _generate_run_id(self) -> str:
        """
        Generate a random 8-character runtime ID that complies with Nextflow naming requirements.
        Must start with lowercase letter, followed by lowercase letters and digits.
        """
        # Generate random alphanumeric runtime ID using UTC timestamp and random seed
        utc_timestamp = int(datetime.now().timestamp())
        random.seed(utc_timestamp)

        # Generate 8-character runtime ID that complies with Nextflow naming requirements
        # Must start with lowercase letter, followed by lowercase letters and digits
        first_char = random.choice(string.ascii_lowercase)
        remaining_chars = "".join(random.choices(string.ascii_lowercase + string.digits, k=7))
        return first_char + remaining_chars

    def _parse_config(self) -> ProjectConfig:
        """
        Parse the nextflow.config file and return a ProjectConfig object.
        Also validates plugin versions against nf_snowflake_image version.
        """
        parser = NextflowConfigParser()
        selected = parser.parse(self._project_dir, self._profile)

        self._validate_plugin_versions(selected)

        config = ProjectConfig()
        config.computePool = selected.get("computePool", "")
        config.workDirStage = selected.get("workDirStage", "")
        config.enableStageMountV2 = selected.get("enableStageMountV2", True)
        stage_mounts_expr = selected.get("stageMounts", "")
        config.volumeConfig = parse_stage_mounts(stage_mounts_expr, config.enableStageMountV2)

        return config

    def _validate_plugin_versions(self, parsed_config: dict) -> None:
        """
        Validate that the nf-snowflake plugin version matches the nf_snowflake_image version.

        Args:
            parsed_config: The parsed configuration containing plugins information

        Raises:
            CliError: If version mismatch is detected
        """
        # Find nf-snowflake plugin
        nf_snowflake_plugin = None
        for plugin in parsed_config.get("plugins", []):
            if plugin.get("name") == "nf-snowflake":
                nf_snowflake_plugin = plugin
                break

        if not nf_snowflake_plugin:
            # nf-snowflake plugin not found, skip validation
            raise CliError("nf-snowflake plugin not found in nextflow.config")

        plugin_version = nf_snowflake_plugin.get("version")
        if not plugin_version:
            # Plugin version not specified, skip validation
            raise CliError("nf-snowflake plugin version not specified in nextflow.config")

        # Extract version from nf_snowflake_image
        image_version = self._extract_version_from_image(self._nf_snowflake_image)
        if not image_version:
            # Cannot extract version from image, skip validation
            raise CliError("nf_snowflake_image version not specified")

        # Compare versions
        if plugin_version != image_version:
            raise CliError(
                f"Version mismatch detected: "
                f"nf-snowflake plugin version '{plugin_version}' does not match "
                f"nf_snowflake_image version '{image_version}'. "
                f"Please ensure both versions are aligned to avoid plugin download inside container."
            )

    def _extract_version_from_image(self, image_name: str) -> Optional[str]:
        """
        Extract version from nf_snowflake_image name.

        Supports patterns like:
        - repo/nf-snowflake:0.8.0
        - nf-snowflake:0.8.0
        - ghcr.io/snowflake-labs/nf-snowflake:0.7.1

        Args:
            image_name: The image name/tag

        Returns:
            Version string if found, None otherwise
        """
        if not image_name:
            return None

        # Pattern to match version tag after colon
        # Matches semantic versions (x.y.z) or tags like "latest", "stable", etc.
        version_pattern = r":([a-zA-Z0-9\.-]+?)(?:$|@)"
        match = re.search(version_pattern, image_name)

        if match:
            return match.group(1)

        return None

    def _upload_project(self, config: ProjectConfig) -> str:
        """
        Create a tarball of the project directory and upload to Snowflake stage.
        """

        # Create temporary file for the tarball using injected generator
        temp_tarball_path = self._temp_file_generator(".tar.gz")

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
                ".git",
                ".gitignore",
            ]

            for pattern in excluded_patterns:
                if pattern in tarinfo.name:
                    return None

            return tarinfo

        try:
            with tarfile.open(tarball_path, "w:gz") as tar:
                # Add all files from project directory with filtering
                tar.add(
                    project_path,
                    arcname=project_path.name,  # Use project name as root in archive
                    filter=tar_filter,
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
        cc.step(f"Log Streaming URL: {wss_url}")

        # Callback functions for WebSocket events
        def on_message(message: str) -> None:
            print(message, end="")

        def on_status(status: str, data: dict) -> None:
            if status == "starting":
                cc.step(f"Starting: {data.get('command', '')}")
            elif status == "started":
                cc.step(f"Started with PID: {data.get('pid', '')}")
            elif status == "connected":
                cc.step("Connected to WebSocket server...")
                cc.step("Streaming live output... (Press Ctrl+C to stop)")
                cc.step("=" * 50)
            elif status == "disconnected":
                cc.step(f"Disconnected: {data.get('reason', '')}")

        def on_error(message: str, exception: Exception) -> None:
            cc.warning(f"Processing error: {message}")

        exit_code = None
        # Create WebSocket client and connect
        try:
            wss_client = WebSocketClient(
                conn=self._conn, message_callback=on_message, status_callback=on_status, error_callback=on_error
            )
            exit_code = asyncio.run(wss_client.connect_and_stream("wss://" + wss_url))
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
        tags = json.dumps(
            {
                "NEXTFLOW_JOB_TYPE": "main",
                "NEXTFLOW_RUN_ID": self._run_id,
            }
        )

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
            "/tmp/report.html",
            "-with-trace",
            "/tmp/trace.txt",
            "-with-timeline",
            "/tmp/timeline.html",
        ]

        # if not async, we need to run the pty server to get the logs
        python_pty_server_cmd = "python3 /app/pty_server.py -- " if not is_async else ""
        run_script = f"""
        mkdir -p /mnt/project
        cd /mnt/project
        tar -zxf {workDir}/{tarball_filename}

        {python_pty_server_cmd}{" ".join(nf_run_cmds)}
        cp /tmp/report.html /mnt/workdir/report.html
        cp /tmp/trace.txt /mnt/workdir/trace.txt
        cp /tmp/timeline.html /mnt/workdir/timeline.html
        """

        config.volumeConfig.volumeMounts.append(VolumeMount(name="workdir", mountPath=workDir))

        volume = (
            Volume(
                name="workdir",
                source="stage",
                stageConfig=StageConfig(name="@" + config.workDirStage + "/" + self._run_id + "/", enableSymlink=True),
            )
            if config.enableStageMountV2
            else Volume(name="workdir", source="@" + config.workDirStage + "/" + self._run_id + "/")
        )

        config.volumeConfig.volumes.append(volume)

        endpoints = None if is_async else [Endpoint(name="wss", port=8765, public=True)]

        spec = Specification(
            spec=Spec(
                containers=[
                    Container(
                        name="nf-main",
                        image=self._nf_snowflake_image,
                        command=["/bin/bash", "-c", run_script],
                        volumeMounts=config.volumeConfig.volumeMounts,
                    )
                ],
                volumes=config.volumeConfig.volumes,
                endpoints=endpoints,
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
            self.execute_query("drop service if exists " + self.service_name)
