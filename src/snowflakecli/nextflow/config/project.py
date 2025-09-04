from snowflakecli.nextflow.service_spec import VolumeConfig
from snowflake.cli.api.exceptions import CliError

class ProjectConfig:
    """Configuration for a Nextflow project."""

    def __init__(
        self,
        computePool: str = None,
        workDirStage: str = None,
        volumeConfig: VolumeConfig = None,
        driverImage: str = None,
        eai: str = None,
    ):
        self.computePool = computePool
        self.workDirStage = workDirStage
        self.volumeConfig = volumeConfig
        self.driverImage = driverImage
        self.eai = eai

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

