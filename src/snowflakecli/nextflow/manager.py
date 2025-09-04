from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor, DictCursor
from snowflakecli.nextflow.service_spec import (
    Specification,
    Spec,
    Container,
    parse_stage_mounts,
    VolumeMount,
    Volume,
    Endpoint,
    StageConfig,
    ReadinessProbe,
    LogExporters,
)

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
from snowflakecli.nextflow.config.parser import NextflowConfigParser
from snowflakecli.nextflow.config.project import ProjectConfig

class NextflowManager(SqlExecutionMixin):
    def __init__(
        self,
        project_dir: str,
        profile: str = None,
        id_generator: Callable[[], str] = None,
        temp_file_generator: Callable[[str], str] = None,
    ):
        super().__init__()
        self._project_dir = Path(project_dir)

        if not self._project_dir.exists() or not self._project_dir.is_dir():
            raise CliError(f"Invalid project directory '{project_dir}'")

        self._profile = profile

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

        config = ProjectConfig(
            computePool=selected.get("computePool", None),
            workDirStage=selected.get("workDirStage", None),
            driverImage=selected.get("driverImage", None),
            eai=selected.get("externalAccessIntegrations", ""),
            volumeConfig=parse_stage_mounts(selected.get("stageMounts", None)),
        )

        self._validate_plugin_versions(selected.get("plugins", []), config.driverImage)

        return config

    def _validate_plugin_versions(self, plugins: list[dict], driver_image: str) -> None:
        """
        Validate that the nf-snowflake plugin version matches the nf_snowflake_image version.

        Args:
            parsed_config: The parsed configuration containing plugins information

        Raises:
            CliError: If version mismatch is detected
        """
        # Find nf-snowflake plugin
        nf_snowflake_plugin = None
        for plugin in plugins:
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
        image_version = self._extract_version_from_image(driver_image)
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
            exit_code = asyncio.run(wss_client.connect_and_stream("wss://" + wss_url + "/ws"))
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

    def _submit_nextflow_job(
        self, config: ProjectConfig, tarball_path: str, is_async: bool, params: list[str], quiet: bool, resume: str
    ) -> SnowflakeCursor:
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
        cursor = self.execute_query("select current_warehouse() as WH", cursor_class=DictCursor)
        warehouse = cursor.fetchone()["WH"]

        cursor = self.execute_query("select current_user() as USER", cursor_class=DictCursor)
        user = cursor.fetchone()["USER"]

        workDir = f"/mnt/workdir/{self._run_id}/"
        tarball_filename = os.path.basename(tarball_path)

        nf_run_cmds = ["nextflow"]
        if quiet:
            nf_run_cmds.append("-q")

        # If sync run, python pty server will read the .nextflow.log file in bg
        # emit to stderr. If async run, emit .nextflow.log to stderr directly.
        if is_async:
            nf_run_cmds.extend(["-log", "/dev/stderr"])

        nf_run_cmds.extend(
            [
                "run",
                f"{workDir}/project/",
                "-name",
                self._run_id,
                "-ansi-log",
                str(not is_async),
                "-profile",
                self._profile,
                # to support resume, we need to use the same work-dir across runs
                "-work-dir",
                "/mnt/workdir",
                "-with-report",
                "/tmp/report.html",
                "-with-trace",
                "/tmp/trace.txt",
                "-with-timeline",
                "/tmp/timeline.html",
            ]
        )

        if resume:
            nf_run_cmds.extend(["-resume", resume])

        for param in params:
            param_key, param_value = param.split("=")
            nf_run_cmds.append(f"--{param_key}")
            nf_run_cmds.append(param_value)

        # if not async, we need to run the pty server to get the logs
        python_pty_server_cmd = "python3 /app/pty_server.py -- " if not is_async else ""
        run_script = f"""
mkdir -p /mnt/project && cd /mnt/project
tar -zxf {workDir}/{tarball_filename} 2>/dev/null
cp -r /mnt/project/ {workDir}/

{python_pty_server_cmd}{" ".join(nf_run_cmds)}
cp /tmp/report.html {workDir}/report.html
cp /tmp/trace.txt {workDir}/trace.txt
cp /tmp/timeline.html {workDir}/timeline.html
"""
        if is_async:
            run_script += "echo 'nextflow command finished successfully'"

        config.volumeConfig.volumeMounts.append(VolumeMount(name="workdir", mountPath="/mnt/workdir"))

        volume = Volume(
            name="workdir",
            source="stage",
            stageConfig=StageConfig(name="@" + config.workDirStage + "/", enableSymlink=True),
        )

        config.volumeConfig.volumes.append(volume)

        endpoints = None if is_async else [Endpoint(name="wss", port=8765, public=True)]

        env = {
            "CURRENT_USER": user if user else "UNKNOWN",
            # to support resume, we need to use the same cache path across runs
            "SNOWFLAKE_CACHE_PATH": "/mnt/workdir/cache",
            "NXF_IGNORE_RESUME_HISTORY": "true",
        }
        if warehouse:
            env["SNOWFLAKE_WAREHOUSE"] = warehouse

        spec = Specification(
            spec=Spec(
                containers=[
                    Container(
                        name="nf-main",
                        image=config.driverImage,
                        command=["/bin/bash", "-e", "-c", run_script],
                        volumeMounts=config.volumeConfig.volumeMounts,
                        readinessProbe=ReadinessProbe(port=8765, path="/healthz") if not is_async else None,
                        env=env,
                    )
                ],
                volumes=config.volumeConfig.volumes,
                endpoints=endpoints,
                logExporters=LogExporters(eventTableConfig={"logLevel": "INFO"}),
            )
        )

        # Get YAML string for inline spec
        yaml_spec = spec.to_yaml()

        if is_async:
            execute_sql = f"""
EXECUTE JOB SERVICE
IN COMPUTE POOL {config.computePool}
NAME = {self.service_name}
EXTERNAL_ACCESS_INTEGRATIONS = ({config.eai})
FROM SPECIFICATION $$
{yaml_spec}
$$
            """
        else:
            execute_sql = f"""
CREATE SERVICE {self.service_name}
IN COMPUTE POOL {config.computePool}
EXTERNAL_ACCESS_INTEGRATIONS = ({config.eai})
FROM SPECIFICATION $$
{yaml_spec}
$$
        """
        return self.execute_query(execute_sql, _exec_async=is_async)

    def run_async(self, params: list[str], quiet: bool, resume: str = None):
        """
        Run a Nextflow workflow asynchronously.

        Args:
            params: Parameters to pass to the workflow
            log_level: Log level for the workflow execution (NONE, ERROR, INFO)

        Returns:
            Service name for monitoring the async execution
        """
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project to Snowflake..."):
            tarball_path = self._upload_project(config)

        cc.step("Submitting nextflow job to Snowflake...")
        cursor = self._submit_nextflow_job(config, tarball_path, True, params, quiet, resume)
        cc.step(f"QueryId: {cursor.sfqid}")
        cc.step(f"Job service Name: {self.service_name}")

    def run(self, params: list[str], quiet: bool, resume: str = None) -> Optional[int]:
        """
        Run a Nextflow workflow synchronously.

        Args:
            params: Parameters to pass to the workflow
            log_level: Log level for the workflow execution (NONE, ERROR, INFO)

        Returns:
            Exit code if execution completed successfully, None otherwise
        """
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project to Snowflake..."):
            tarball_path = self._upload_project(config)

        cc.step("Submitting nextflow job to Snowflake...")
        cursor = self._submit_nextflow_job(config, tarball_path, False, params, quiet, resume)
        cc.step(f"Nextflow job submitted successfully as service: {self.service_name}, query_id: {cursor.sfqid}")

        try:
            self.execute_query(f"call system$wait_for_services(60, '{self.service_name}')")
            self.execute_query("alter session unset query_tag")

            # Stream logs and get exit code
            exit_code = self._stream_service_logs(self.service_name)
            return exit_code
        finally:
            self.execute_query("drop service if exists " + self.service_name)

    def _run_nextflow_inspect(self, config: ProjectConfig, tarball_path: str) -> None:
        """
        Run the nextflow inspect command.
        """
        workDir = "/mnt/workdir"
        tarball_filename = os.path.basename(tarball_path)

        nf_inspect_cmds = [
            "nextflow",
            "inspect",
            "."
        ]

        run_script = f"""
mkdir -p /mnt/project && cd /mnt/project
tar -zxf {workDir}/{tarball_filename} 2>/dev/null

{" ".join(nf_inspect_cmds)}
"""

        volumeMount = VolumeMount(name="workdir", mountPath=workDir)
        volume = Volume(
            name="workdir",
            source="stage",
            stageConfig=StageConfig(name="@" + config.workDirStage + "/" + self._run_id + "/", enableSymlink=True),
        )

        config.volumeConfig.volumes.append(volume)
        spec = Specification(
            spec=Spec(
                containers=[
                    Container(
                        name="nf-inspect",
                        image=config.driverImage,
                        command=["/bin/bash", "-c", run_script],
                        volumeMounts=[volumeMount],
                    )
                ],
                volumes=[volume],
                endpoints=None,
            ),
        )

        yaml_spec = spec.to_yaml()
        self.execute_query(f"""
EXECUTE JOB SERVICE
IN COMPUTE POOL {config.computePool}
NAME = {self.service_name}
EXTERNAL_ACCESS_INTEGRATIONS = ({config.eai})
FROM SPECIFICATION $$
{yaml_spec}
$$
"""
        )

        cursor = self.execute_query(f"select system$get_service_logs('{self.service_name}', 0, 'nf-inspect')")
        return cursor.fetchone()[0]

    def replicate_image(self):
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project to Snowflake..."):
            tarball_path = self._upload_project(config)

        cc.step("Submitting nextflow job to Snowflake...")
        data = self._run_nextflow_inspect(config, tarball_path)
        print(data)
