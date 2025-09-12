import pytest
from util.mock_db import MockDB
from unittest import mock
from unittest.mock import PropertyMock


@pytest.fixture
def mock_db():
    db = MockDB()
    db.register_query("select current_user() as USER", [{"USER": "test_user"}])
    db.register_query("select current_warehouse() as WH", [{"WH": "test_warehouse"}])

    patcher = mock.patch(
        "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.connection",
        new_callable=PropertyMock,
        return_value=db.get_connection(),
    )

    with patcher:
        yield db
