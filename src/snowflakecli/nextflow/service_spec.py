from dataclasses import dataclass, asdict
from snowflake.cli.api.exceptions import CliError
import yaml


@dataclass
class VolumeMount:
    name: str
    mountPath: str


@dataclass
class Container:
    name: str
    image: str
    command: list[str]
    volumeMounts: list[VolumeMount]


@dataclass
class StageConfig:
    name: str
    enableSymlink: bool


@dataclass
class Volume:
    name: str
    source: str
    stageConfig: StageConfig = None


@dataclass
class Endpoint:
    name: str
    port: int
    public: bool


@dataclass
class Spec:
    containers: list[Container]
    volumes: list[Volume]
    endpoints: list[Endpoint]


@dataclass
class Specification:
    spec: Spec

    def to_yaml(self) -> str:
        """
        Convert ServiceSpec to YAML string.

        Returns:
            YAML string representation of the ServiceSpec
        """
        spec_dict = asdict(self)
        # Remove None values recursively
        cleaned_dict = self._remove_none_values(spec_dict)
        return yaml.dump(cleaned_dict, default_flow_style=False, indent=2)

    def _remove_none_values(self, obj):
        """Recursively remove None values from dictionaries and lists."""
        if isinstance(obj, dict):
            return {k: self._remove_none_values(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [self._remove_none_values(item) for item in obj if item is not None]
        else:
            return obj


@dataclass
class VolumeConfig:
    volumes: list[Volume]
    volumeMounts: list[VolumeMount]


def parse_stage_mounts(stage_mounts: str, enable_stage_mount_v2: bool) -> VolumeConfig:
    if stage_mounts is None or stage_mounts == "":
        return VolumeConfig(volumes=[], volumeMounts=[])

    volume_mounts = []
    volumes = []

    stage_mounts = stage_mounts.split(",")
    for index in range(len(stage_mounts)):
        mount = stage_mounts[index].split(":")
        if len(mount) != 2:
            raise CliError("Invalid stage mount expression: " + stage_mounts[index])

        volume_name = "vol-" + str(index + 1)
        volume_mounts.append(VolumeMount(name=volume_name, mountPath=mount[1]))

        volume = (
            Volume(name=volume_name, source="stage", stageConfig=StageConfig(name="@" + mount[0], enableSymlink=True))
            if enable_stage_mount_v2
            else Volume(name=volume_name, source="@" + mount[0])
        )
        volumes.append(volume)

    return VolumeConfig(volumes=volumes, volumeMounts=volume_mounts)
