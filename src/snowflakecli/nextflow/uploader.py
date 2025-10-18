"""
Project uploading utilities for Nextflow workflows.
"""

import os
import tarfile
from pathlib import Path
from typing import Callable

from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflakecli.nextflow.config.project import ProjectConfig


class ProjectUploader:
    """
    Handles the creation of project tarballs and uploading to Snowflake stages.
    """

    def __init__(
        self,
        project_dir: Path,
        run_id: str,
        temp_file_generator: Callable[[str], str],
        sql_executor: SqlExecutionMixin,
    ):
        """
        Initialize the ProjectUploader.

        Args:
            project_dir: Path to the project directory
            run_id: Unique run identifier
            temp_file_generator: Function to generate temporary file paths
            sql_executor: SQL execution mixin for running queries
        """
        self._project_dir = project_dir
        self._run_id = run_id
        self._temp_file_generator = temp_file_generator
        self._sql_executor = sql_executor

    def upload_project(self, config: ProjectConfig) -> str:
        """
        Create a tarball of the project directory and upload to Snowflake stage.

        Args:
            config: Project configuration containing stage information

        Returns:
            Path to the temporary tarball file
        """
        # Create temporary file for the tarball using injected generator
        temp_tarball_path = self._temp_file_generator(".tar.gz")

        try:
            cc.step("Creating tarball...")
            # Create tarball excluding .git directory
            self._create_tarball(self._project_dir, temp_tarball_path)

            cc.step(f"Uploading to stage {config.workDirStage}...")
            # Upload to Snowflake stage
            self._sql_executor.execute_query(f"PUT file://{temp_tarball_path} @{config.workDirStage}/{self._run_id}")

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
