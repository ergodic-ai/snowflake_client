import snowflake.connector
from snowflake.connector import DictCursor
import os
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SnowflakeClient:
    """Snowflake database client for executing queries and managing connections."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
    ):
        """
        Initialize Snowflake client.

        Args:
            account: Snowflake account identifier
            user: Username
            password: Password
            warehouse: Default warehouse (optional)
            database: Default database (optional)
            schema: Default schema (optional)
            role: Default role (optional)
        """
        self.connection_params = {
            "account": account,
            "user": user,
            "password": password,
        }

        if warehouse:
            self.connection_params["warehouse"] = warehouse
        if database:
            self.connection_params["database"] = database
        if schema:
            self.connection_params["schema"] = schema
        if role:
            self.connection_params["role"] = role

        self.connection = None

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(**self.connection_params)
            logger.info("Successfully connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def disconnect(self) -> None:
        """Close connection to Snowflake."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from Snowflake")

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        init: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string
            params: Optional parameters for parameterized queries
            init: Optional initialization context (database, schema)

        Returns:
            List of dictionaries representing query results
        """
        if not self.connection:
            self.connect()

        if init:
            self.initialize_context(
                init.get("database", None), init.get("schema", None)
            )

        try:
            if not self.connection:
                raise Exception("No connection to Snowflake")

            cursor = self.connection.cursor(DictCursor)

            if params:
                print(f"Executing query with params: {params}")
                print(f"Query: {query}")
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            results = cursor.fetchall()
            cursor.close()

            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results  # type: ignore

        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise

    def initialize_context(
        self, database: Optional[str] = None, schema: Optional[str] = None
    ) -> None:
        """
        Execute initialization queries to set database and schema context.

        Args:
            database: Database name to use
            schema: Schema name to use
        """
        try:
            if database:
                # Set database context
                use_db_query = f"USE DATABASE {database}"
                self.execute_query(use_db_query)
                logger.info(f"Set database context to: {database}")

            # Set schema context
            if schema:
                use_schema_query = f"USE SCHEMA {schema}"
                self.execute_query(use_schema_query)
                logger.info(f"Set schema context to: {schema}")

        except Exception as e:
            logger.error(f"Failed to initialize context: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


def create_snowflake_client_from_env() -> SnowflakeClient:
    """
    Create Snowflake client using environment variables.

    Expected environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_WAREHOUSE (optional)
    - SNOWFLAKE_DATABASE (optional)
    - SNOWFLAKE_SCHEMA (optional)
    - SNOWFLAKE_ROLE (optional)
    """
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    role = os.getenv("SNOWFLAKE_ROLE")

    if not account:
        raise Exception("SNOWFLAKE_ACCOUNT is not set")
    if not user:
        raise Exception("SNOWFLAKE_USER is not set")
    if not password:
        raise Exception("SNOWFLAKE_PASSWORD is not set")

    return SnowflakeClient(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        role=role,
    )


def create_database(client: SnowflakeClient, database_name: str) -> None:
    """Create a database in Snowflake."""
    client.execute_query(f"CREATE DATABASE {database_name}")
