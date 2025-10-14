import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from snowflake.cli.api.exceptions import CliError
from snowflakecli.nextflow.manager import NextflowManager
from snowflakecli.nextflow.image.commands import app as image_app
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.connector.cursor import DictCursor

app = SnowTyperFactory(
    name="nextflow",
    help="Run Nextflow workflows in Snowpark Container Service",
)

app.add_typer(image_app)


@app.command("log", requires_connection=True)
def show_history(
    limit: int = typer.Option(
        5,
        "--limit",
        "-l",
        help="Number of recent runs to display (default: 5)",
    ),
    **options,
) -> CommandResult:
    """
    Show execution history for Nextflow workflows.
    """
    try:
        # Use SqlManager to execute the query directly
        sql_manager = SqlManager()

        # Query the nxf_execution_history table for recent executions
        # Ordered by startTimestamp in descending order (most recent first)
        query = f"""
        SELECT * exclude (run_id, submitted_by)
        FROM nxf_execution_history
        WHERE submitted_by = CURRENT_USER()
        ORDER BY run_start_time DESC
        LIMIT {limit}
        """

        cursor = sql_manager.execute_query(query, cursor_class=DictCursor)

        # Return QueryResult which will handle the cursor results directly
        return QueryResult(cursor)

    except Exception as e:
        raise CliError(f"Failed to retrieve execution history: {str(e)}")


@app.command("run", requires_connection=True)
def run_workflow(
    project_dir: str = typer.Argument(help="Name of the workflow to run"),
    profile: str = typer.Option(
        None,
        "--profile",
        help="Nextflow profile to use for the workflow execution",
    ),
    resume: str = typer.Option(
        None,
        "--resume",
        help="Resume a workflow from a specific session ID",
    ),
    async_run: bool = typer.Option(
        False,
        "--async",
        help="Run workflow asynchronously without waiting for completion",
    ),
    params: list[str] = typer.Option(
        [],
        "--param",
        help="Parameters to pass to the workflow",
    ),
    quiet: bool = typer.Option(
        False,
        "-q",
        help="Suppress all output except for error messages",
    ),
    **options,
) -> CommandResult:
    """
    Run a Nextflow workflow in Snowpark Container Service.
    """


    manager = NextflowManager(project_dir, profile)

    if async_run is not None and async_run:
        result = manager.run_async(params, quiet, resume)
        # For async runs, result should contain service information
        return MessageResult("Nextflow workflow submitted successfully. Check Snowsight for status.")
    else:
        result = manager.run(params, quiet, resume)
        # For sync runs, result should be exit code
        if result is not None:
            if result == 0:
                return MessageResult(f"Nextflow workflow completed successfully (exit code: {result})")
            else:
                raise CliError(f"Nextflow workflow completed with exit code: {result}")
        else:
            raise CliError("Nextflow workflow execution interrupted or failed to complete")
