from abc import ABC, abstractmethod
from pathlib import Path
from snowflakecli.nextflow.service_spec import VolumeConfig, parse_stage_mounts
from snowflake.cli.api.exceptions import CliError
from typing import Optional


class ProjectConfig:
    """Configuration for a Nextflow project."""

    def __init__(
        self,
        computePool: str = None,
        workDirStage: str = None,
        volumeConfig: VolumeConfig = None,
        driverImage: str = None,
        craneImage: str = None,
        eai: str = None,
        registryMappings: str = None,
    ):
        self.computePool = computePool
        self.workDirStage = workDirStage
        self.volumeConfig = volumeConfig
        self.driverImage = driverImage
        self.craneImage = craneImage
        self.eai = eai
        self.registryMappings = registryMappings

        self._validate_required_fields()

    def _validate_required_fields(self) -> None:
        """
        Validate that all required configuration fields are present.

        Raises:
            CliError: If any required field is missing (None)
        """
        # Define required fields - add new required fields here in the future
        required_fields = [
            ("computePool", "computePool"),
            ("workDirStage", "workDirStage"),
            ("driverImage", "driverImage"),
        ]

        for field_name, config_key in required_fields:
            field_value = getattr(self, field_name, None)
            if field_value is None:
                raise CliError(f"{config_key} is required but not found in nextflow.config")


class Project(ABC):
    """Abstract base class for providing project configuration."""

    @abstractmethod
    def get_project_config(self, profile: Optional[str] = None) -> ProjectConfig:
        """
        Get the project configuration.

        Args:
            profile: Optional profile name(s) to extract config from

        Returns:
            Tuple of (ProjectConfig object, list of plugins)
        """
        pass

    @abstractmethod
    def get_project_dir(self) -> Path:
        """
        Get the project directory path.

        Returns:
            Path to the project directory
        """
        pass


class FilesystemProject(Project):
    """Project implementation that reads from a project directory."""

    def __init__(self, project_dir: str):
        """
        Initialize the file system project.

        Args:
            project_dir: Path to the project directory containing nextflow.config
        """
        self._project_dir = Path(project_dir)

        if not self._project_dir.exists() or not self._project_dir.is_dir():
            raise CliError(f"Invalid project directory '{project_dir}'")

    def get_project_config(self, profile: Optional[str] = None) -> ProjectConfig:
        """
        Parse the nextflow.config file and return a ProjectConfig object.

        Args:
            profile: Optional profile name(s) to extract config from

        Returns:
            Tuple of (ProjectConfig object, list of plugins)
        """
        from snowflakecli.nextflow.config.parser import NextflowConfigParser

        parser = NextflowConfigParser()
        selected = parser.parse(str(self._project_dir), profile)

        config = ProjectConfig(
            computePool=selected.get("computePool", None),
            workDirStage=selected.get("workDirStage", None),
            driverImage=selected.get("driverImage", None),
            craneImage=None,
            eai=selected.get("externalAccessIntegrations", ""),
            volumeConfig=parse_stage_mounts(selected.get("stageMounts", None)),
            registryMappings=selected.get("registryMappings", None),
        )

        return config, selected.get("plugins", [])

    def get_project_dir(self) -> Path:
        """
        Get the project directory path.

        Returns:
            Path to the project directory
        """
        return self._project_dir


class InMemoryProject(Project):
    """Project implementation that reads from a configuration string."""

    def __init__(self, config_text: str, project_dir: Optional[str] = None):
        """
        Initialize the in-memory project.

        Args:
            config_text: Configuration text to parse
            project_dir: Optional project directory path (required for operations that need file access)
        """
        self._config_text = config_text
        if project_dir:
            self._project_dir = Path(project_dir)
        else:
            # Create a temporary directory for testing
            import tempfile

            self._temp_dir = tempfile.mkdtemp()
            self._project_dir = Path(self._temp_dir)

    def get_project_config(self, profile: Optional[str] = None) -> ProjectConfig:
        """
        Parse the configuration string and return a ProjectConfig object.

        Args:
            profile: Optional profile name(s) to extract config from

        Returns:
            Tuple of (ProjectConfig object, list of plugins)
        """
        from snowflakecli.nextflow.config.parser import NextflowConfigParser

        parser = NextflowConfigParser()
        selected = parser.parse_str(self._config_text, profile)

        config = ProjectConfig(
            computePool=selected.get("computePool", None),
            workDirStage=selected.get("workDirStage", None),
            driverImage=selected.get("driverImage", None),
            craneImage=None,
            eai=selected.get("externalAccessIntegrations", ""),
            volumeConfig=parse_stage_mounts(selected.get("stageMounts", None)),
            registryMappings=selected.get("registryMappings", None),
        )

        return config, selected.get("plugins", [])

    def get_project_dir(self) -> Path:
        """
        Get the project directory path.

        Returns:
            Path to the project directory
        """
        return self._project_dir
