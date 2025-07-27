import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.exceptions import CliError
from snowflakecli.nextflow.image.manager import ImageManager
from snowflake.cli.api.config import set_config_value, PLUGINS_SECTION_PATH

app = SnowTyperFactory(
    name="image",
    help="Manage Nextflow plugin image operations",
)


@app.command("push", requires_connection=True)
def push_image(
    source=typer.Option(
        ...,
        "--source",
        help="Source image to pull (e.g., 'ghcr.io/owner/repo:tag')",
        show_default=False,
    ),
    target=typer.Option(
        ...,
        "--target",
        help="Target repository path in Snowflake (e.g., 'db.schema.repo')",
        show_default=False,
    ),
    update_config: bool = typer.Option(
        False,
        "--update-config",
        help="Update the config file with the new image",
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Pull an image from source, retag it, and push to Snowflake SPCS image repository.

    This command will:
    1. Pull the image from the source registry
    2. Get authentication token for Snowflake image registry
    3. Retag the image for the target Snowflake repository
    4. Push the image to Snowflake SPCS image registry

    Example:
        snow nextflow image push --source ghcr.io/owner/repo:latest --target /db/schema/repo
    """

    try:
        manager = ImageManager()
        image_name = manager.push_image(source, target)

    except Exception as e:
        raise CliError("Failed to push image: {}".format(e))

    if update_config:
        set_config_value(
            path=PLUGINS_SECTION_PATH + ["nextflow", "config", "nf_snowflake_image"], value=f"{image_name}"
        )

    return MessageResult("Successfully pushed image to Snowflake registry")
