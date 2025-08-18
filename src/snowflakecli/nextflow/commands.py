import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.exceptions import CliError
from snowflakecli.nextflow.manager import NextflowManager
from snowflakecli.nextflow.image.commands import app as image_app
from typing import Optional

app = SnowTyperFactory(
    name="nextflow",
    help="Run Nextflow workflows in Snowpark Container Service",
)

app.add_typer(image_app)


@app.command("run", requires_connection=True)
def run_workflow(
    project_dir: str = typer.Argument(help="Name of the workflow to run"),
    profile: str = typer.Option(
        None,
        "--profile",
        help="Nextflow profile to use for the workflow execution",
    ),
    async_run: Optional[bool] = typer.Option(
        False,
        "--async",
        help="Run workflow asynchronously without waiting for completion",
    ),
    **options,
) -> CommandResult:
    """
    Run a Nextflow workflow in Snowpark Container Service.
    """

    manager = NextflowManager(project_dir, profile)

    if async_run is not None and async_run:
        result = manager.run_async()
        # For async runs, result should contain service information
        return MessageResult(f"Nextflow workflow submitted successfully. Query ID: {result}")
    else:
        result = manager.run()
        # For sync runs, result should be exit code
        if result is not None:
            if result == 0:
                return MessageResult(f"Nextflow workflow completed successfully (exit code: {result})")
            else:
                raise CliError(f"Nextflow workflow completed with exit code: {result}")
        else:
            raise CliError("Nextflow workflow execution interrupted or failed to complete")
