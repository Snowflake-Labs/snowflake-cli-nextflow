from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.exceptions import CliError
from snowflakecli.nextflow.config.project import ProjectConfig
from snowflakecli.nextflow.config.parser import NextflowConfigParser
from snowflakecli.nextflow.uploader import ProjectUploader

from snowflake.connector.cursor import DictCursor

import os
import tempfile
import random
import string
import time
from pathlib import Path
from datetime import datetime
from snowflake.cli.api.console import cli_console as cc
from snowflakecli.nextflow.service_spec import (
    Specification,
    Spec,
    Container,
    VolumeMount,
    Volume,
    StageConfig,
)
import json
import typer


class ImageReplicator(SqlExecutionMixin):
    def __init__(self, project_dir: str, profile: str = None):
        super().__init__()
        self._project_dir = Path(project_dir)
        self._profile = profile
        self._run_id = self._generate_run_id()
        self._project_uploader = ProjectUploader(
            self._project_dir, self._run_id, self._default_temp_file_generator, self
        )
        self.service_name = f"NXF_REPLICATE_{self._run_id}"

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

    def _default_temp_file_generator(self, suffix: str) -> str:
        """
        Default temporary file generator using tempfile.NamedTemporaryFile.
        """
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as temp_file:
            return temp_file.name

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
            craneImage=selected.get("craneImage", None),
            eai=selected.get("externalAccessIntegrations", ""),
            volumeConfig=None,
            registryMappings=selected.get("registryMappings", None),
        )

        return config

    def _run_nextflow_inspect(self, config: ProjectConfig, tarball_path: str) -> None:
        """
        Run the nextflow inspect command.
        """
        workDir = "/mnt/workdir"
        tarball_filename = os.path.basename(tarball_path)

        nf_inspect_cmds = ["nextflow", "inspect", "."]

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

        spec = Specification(
            spec=Spec(
                containers=[
                    Container(
                        name="nf-inspect",
                        image=config.driverImage,
                        command=["/bin/bash", "-c", run_script],
                        volumeMounts=[volumeMount],
                        env={
                            "SNOWFLAKE_CACHE_PATH": "/mnt/workdir/cache",
                        },
                    )
                ],
                volumes=[volume],
                endpoints=None,
                logExporters=None,
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
""")

        cursor = self.execute_query(f"select system$get_service_logs('{self.service_name}', 0, 'nf-inspect')")
        return json.loads(cursor.fetchone()[0])

    def _check_service_status(self, service_name: str) -> str:
        """
        Check the status of a service.
        Potential return values: DONE, FAILED, READY, UNKNOWN, PENDING
        """
        cursor = self.execute_query(f"SHOW SERVICE CONTAINERS IN SERVICE {service_name}", cursor_class=DictCursor)
        return cursor.fetchone()["status"]

    def _replicate_single_image(self, config: ProjectConfig, registryMappings: dict, process_name: str, container: str):
        nxf_registry_host = container.split("/")[0]
        nxf_container_rest = container[len(nxf_registry_host) + 1 :]

        _, mapped_repo_url = registryMappings[container.split("/")[0]]

        mapped_repo_host = mapped_repo_url.split("/")[0]
        mapped_repo_host_part = mapped_repo_host.split(".")
        mapped_repo_path = mapped_repo_url[len(mapped_repo_host) + 1 :]
        for idx, part in enumerate(mapped_repo_host_part):
            # replace registry or registry-dev with registry-local
            if "registry" in part:
                mapped_repo_host_part[idx] = "registry-local"
        mapped_repo_host_internal = ".".join(mapped_repo_host_part)
        target = f"{mapped_repo_host_internal}/{mapped_repo_path}/{nxf_container_rest}"

        run_script = f"""
crane auth login -v -u 0auth2accesstoken -p $(cat /snowflake/session/token) {mapped_repo_host_internal}
crane copy {container} {target} --insecure
"""

        spec = Specification(
            spec=Spec(
                containers=[
                    Container(
                        name="crane-copy",
                        image=config.craneImage,
                        command=["/busybox/sh", "-c", run_script],
                        volumeMounts=None,
                    )
                ],
                volumes=None,
                endpoints=None,
                logExporters=None,
            ),
        )

        replicate_task_name = f"NXF_REPLICATE_{self._run_id}_{process_name}"

        yaml_spec = spec.to_yaml()

        # Use lock to ensure thread-safe database access
        self.execute_query(f"""
EXECUTE JOB SERVICE
IN COMPUTE POOL {config.computePool}
NAME = {replicate_task_name}
ASYNC = TRUE
EXTERNAL_ACCESS_INTEGRATIONS = ({config.eai})
FROM SPECIFICATION $$
{yaml_spec}
$$
""")
        return replicate_task_name

    def _replicate_images_impl(self, config: ProjectConfig, result: dict):
        """
        Replicate the images from the result.
        """

        # parse registry mappings
        # e.g. {nxf_registry_host: (repo_url, repo_name)}
        registryMappings = {}
        for mapping in config.registryMappings.split(","):
            mapping_parts = mapping.split(":")
            cursor = self.execute_query(f"show image repositories like '{mapping_parts[1]}'", cursor_class=DictCursor)
            repo_url = cursor.fetchone()["repository_url"]

            registryMappings[mapping_parts[0]] = (mapping_parts[1], repo_url)

        for process in result["processes"]:
            container = process["container"]

            nxf_registry_host = container.split("/")[0]
            if nxf_registry_host not in registryMappings:
                raise CliError(f"Registry host {nxf_registry_host} not found in registry mappings")

        processes = result["processes"]
        total_processes = len(processes)

        # Deduplicate containers - multiple processes may use the same container
        unique_containers = list(set(process["container"] for process in processes))

        cc.step(f"Found {total_processes} processes using {len(unique_containers)} unique container image(s)")

        # Submit all replication jobs asynchronously (one per unique container)
        cc.step(f"Submitting {len(unique_containers)} image replication job(s)...")

        # Submit all jobs and track their service names
        job_to_container = {}
        for idx, container in enumerate(unique_containers):
            try:
                # Use index for unique service naming since we don't have process name
                service_name = self._replicate_single_image(config, registryMappings, f"img_{idx}", container)
                job_to_container[service_name] = container
            except Exception as exc:
                cc.warning(f"Failed to submit job for {container}: {exc}")
                raise CliError(f"Failed to submit replication job for {container}")

        cc.step(f"Submitted {len(job_to_container)} job(s). Monitoring progress...")

        # Poll for job completion with progress bar
        failed_jobs = []
        completed_jobs = set()
        poll_interval = 2  # seconds
        total_jobs = len(job_to_container)

        with typer.progressbar(
            length=total_jobs,
            label="Replicating images",
            show_eta=True,
            show_percent=True,
        ) as progress:
            while len(completed_jobs) < total_jobs:
                for service_name, container in job_to_container.items():
                    if service_name in completed_jobs:
                        continue

                    try:
                        status = self._check_service_status(service_name)

                        if status == "DONE":
                            completed_jobs.add(service_name)
                            progress.update(1)
                        elif status == "FAILED":
                            completed_jobs.add(service_name)
                            failed_jobs.append((container, f"Job failed with status: {status}"))
                            progress.update(1)
                    except Exception as exc:
                        # If we can't check status, mark as failed
                        completed_jobs.add(service_name)
                        failed_jobs.append((container, f"Status check failed: {exc}"))
                        progress.update(1)

                # Sleep before next poll cycle if jobs are still running
                if len(completed_jobs) < total_jobs:
                    time.sleep(poll_interval)

        # Report results
        if failed_jobs:
            cc.warning(f"Replication completed with {len(failed_jobs)} failed jobs:")
            for container, error in failed_jobs:
                cc.warning(f"  - {container}: {error}")
            raise CliError(f"{len(failed_jobs)} image replication jobs failed")
        else:
            cc.step(f"Successfully replicated {total_jobs} images")

    def replicate_image(self):
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project to Snowflake..."):
            tarball_path = self._project_uploader.upload_project(config)

        cc.step("Running nextflow inspect...")
        result = self._run_nextflow_inspect(config, tarball_path)

        cc.step("Replicating images...")
        self._replicate_images_impl(config, result)
