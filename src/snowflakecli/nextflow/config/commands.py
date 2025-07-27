from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
import typer
from snowflake.cli.api.config import set_config_value, PLUGINS_SECTION_PATH
from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider
from snowflake.cli.api.exceptions import CliError

NF_SNOWFLAKE_IMAGE_KEY = "nf_snowflake_image"
VALID_KEYS = [NF_SNOWFLAKE_IMAGE_KEY]

app = SnowTyperFactory(
    name="config",
    help="Manage Nextflow plugin configuration",
)


@app.command()
def set(
    key: str = typer.Option(
        ...,
        "-key",
        help="The key to get",
        show_default=False,
    ),
    value: str = typer.Option(
        ...,
        "-value",
        help="The value to set",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Get a configuration value for the Nextflow plugin.
    """
    if key not in VALID_KEYS:
        raise CliError(f"Invalid config key: {key}, valid keys are {VALID_KEYS}")

    set_config_value(path=PLUGINS_SECTION_PATH + ["nextflow", "config", key], value=value)

    return MessageResult(f"Successfully set config for {key} to {value}")


@app.command()
def get(
    key: str = typer.Option(
        ...,
        "-key",
        help="The key to get",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    if key not in VALID_KEYS:
        raise CliError(f"Invalid config key: {key}, valid keys are {VALID_KEYS}")

    plugin_config = PluginConfigProvider.get_config("nextflow")
    if key not in plugin_config.internal_config:
        raise CliError(f"Key {key} not found in plugin config")

    return MessageResult(f"Getting config for {key} to {plugin_config.internal_config[key]}")
