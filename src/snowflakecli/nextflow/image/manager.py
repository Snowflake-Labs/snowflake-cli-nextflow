from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli._plugins.spcs.image_repository.manager import ImageRepositoryManager
from snowflake.cli._plugins.spcs.image_registry.manager import RegistryManager
import docker


class ImageManager(SqlExecutionMixin):
    """Manager for handling Docker image operations with Snowflake SPCS image registry"""

    def __init__(self):
        super().__init__()
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            raise CliError("Failed to connect to Docker daemon. Please ensure Docker is running: {}".format(e))

    def _get_auth_token(self):
        # type: () -> str
        """Get Snowflake session token for registry authentication"""
        try:
            self.execute_query("alter session set python_connector_query_result_format = 'json'")
            token_data = self._ctx.connection._rest._token_request("ISSUE")
            return token_data["data"]["sessionToken"]
        except Exception as e:
            raise CliError("Failed to get authentication token: {}".format(e))

    def _parse_source_image(self, source_image) -> tuple[str, str]:
        parts = source_image.split(":")
        if len(parts) != 2:
            raise CliError("Invalid image name: {}".format(source_image))

        image_name = parts[0].split("/")[-1]
        image_tag = parts[1]

        return image_name, image_tag

    def push_image(self, source_image, target_repo) -> str:
        # type: (str, str) -> None
        """
        Pull image from source, retag it, and push to Snowflake SPCS image repository

        Args:
            source_image: Source image name (e.g., 'ghcr.io/owner/repo:tag')
            target_repo: Target repository path in Snowflake (e.g., '/db/schema/repo')
        """
        image_name, image_tag = self._parse_source_image(source_image)

        image_repository_manager = ImageRepositoryManager()
        repo_url = image_repository_manager.get_repository_url(target_repo, with_scheme=False)

        image_registry_manager = RegistryManager()
        image_registry_manager.docker_registry_login()

        # Construct full target image URL
        target_image = "{}/{}:{}".format(repo_url, image_name, image_tag)

        try:
            # Step 1: Pull the source image
            cc.step("Pulling source image: {}".format(source_image))
            try:
                # Force AMD64 platform since nf-snowflake only supports x86_64
                image = self.docker_client.images.pull(source_image, platform="linux/amd64")
                cc.message("Successfully pulled image: {}".format(source_image))
            except docker.errors.ImageNotFound:
                raise CliError("Source image not found: {}".format(source_image))
            except docker.errors.APIError as e:
                raise CliError("Failed to pull source image: {}".format(e))

            # Step 2: Tag the image for Snowflake registry
            cc.step("Tagging image for Snowflake registry: {}".format(target_image))
            try:
                success = image.tag(target_image)
                if not success:
                    raise CliError("Failed to tag image")
                cc.message("Successfully tagged image: {}".format(target_image))
            except Exception as e:
                raise CliError("Failed to tag image: {}".format(e))

            # Step 3: Push the image to Snowflake registry
            cc.step("Pushing image to Snowflake registry: {}".format(target_image))
            try:
                # Simple push without streaming
                self.docker_client.images.push(target_image)
                cc.message("✅ Successfully pushed image to: {}".format(target_image))

            except docker.errors.APIError as e:
                raise CliError("Failed to push image to Snowflake registry: {}".format(e))

        except Exception as e:
            if isinstance(e, CliError):
                raise e
            else:
                raise CliError("Image push operation failed: {}".format(e))

        # return image name used in execute job service spec
        # e.g. /db/schema/repo/nf-snowflake:0.7.0
        path_parts = target_image.split("/")[1:]  # Get everything after hostname
        return "/" + "/".join(path_parts)
