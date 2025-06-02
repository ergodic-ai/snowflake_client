"""
Tests for the Snowflake client package.
"""

import pytest
from unittest.mock import Mock, patch
from snowflake_client import SnowflakeClient, create_snowflake_client_from_env


class TestSnowflakeClient:
    """Test cases for SnowflakeClient class."""

    def test_client_initialization(self):
        """Test client initialization with required parameters."""
        client = SnowflakeClient(
            account="test-account", user="test-user", password="test-password"
        )

        assert client.connection_params["account"] == "test-account"
        assert client.connection_params["user"] == "test-user"
        assert client.connection_params["password"] == "test-password"
        assert client.connection is None

    def test_client_initialization_with_optional_params(self):
        """Test client initialization with optional parameters."""
        client = SnowflakeClient(
            account="test-account",
            user="test-user",
            password="test-password",
            warehouse="test-warehouse",
            database="test-database",
            schema="test-schema",
            role="test-role",
        )

        assert client.connection_params["warehouse"] == "test-warehouse"
        assert client.connection_params["database"] == "test-database"
        assert client.connection_params["schema"] == "test-schema"
        assert client.connection_params["role"] == "test-role"

    @patch.dict(
        "os.environ",
        {
            "SNOWFLAKE_ACCOUNT": "test-account",
            "SNOWFLAKE_USER": "test-user",
            "SNOWFLAKE_PASSWORD": "test-password",
            "SNOWFLAKE_WAREHOUSE": "test-warehouse",
            "SNOWFLAKE_ROLE": "test-role",
        },
    )
    def test_create_client_from_env(self):
        """Test creating client from environment variables."""
        client = create_snowflake_client_from_env()

        assert client.connection_params["account"] == "test-account"
        assert client.connection_params["user"] == "test-user"
        assert client.connection_params["password"] == "test-password"
        assert client.connection_params["warehouse"] == "test-warehouse"
        assert client.connection_params["role"] == "test-role"

    @patch.dict("os.environ", {}, clear=True)
    def test_create_client_from_env_missing_required(self):
        """Test creating client from environment variables with missing required vars."""
        with pytest.raises(Exception, match="SNOWFLAKE_ACCOUNT is not set"):
            create_snowflake_client_from_env()

    @patch("snowflake.connector.connect")
    def test_connect(self, mock_connect):
        """Test connection establishment."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        client = SnowflakeClient(
            account="test-account", user="test-user", password="test-password"
        )

        client.connect()

        mock_connect.assert_called_once_with(
            account="test-account", user="test-user", password="test-password"
        )
        assert client.connection == mock_connection

    def test_disconnect(self):
        """Test connection disconnection."""
        mock_connection = Mock()

        client = SnowflakeClient(
            account="test-account", user="test-user", password="test-password"
        )
        client.connection = mock_connection

        client.disconnect()

        mock_connection.close.assert_called_once()
        assert client.connection is None

    @patch("snowflake.connector.connect")
    def test_context_manager(self, mock_connect):
        """Test context manager functionality."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection

        client = SnowflakeClient(
            account="test-account", user="test-user", password="test-password"
        )

        with client as ctx_client:
            assert ctx_client == client
            assert client.connection == mock_connection

        mock_connection.close.assert_called_once()
        assert client.connection is None
