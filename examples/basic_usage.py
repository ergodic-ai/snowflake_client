#!/usr/bin/env python3
"""
Basic usage example for the Snowflake Client package.

This example demonstrates how to use the SnowflakeClient to connect
to Snowflake and execute queries.
"""

import os
from snowflake_client import SnowflakeClient, create_snowflake_client_from_env


def example_with_env_variables():
    """Example using environment variables."""
    print("=== Example 1: Using Environment Variables ===")

    try:
        # Create client from environment variables
        with create_snowflake_client_from_env() as client:
            # Execute a simple query
            results = client.execute_query("SELECT CURRENT_TIMESTAMP() as current_time")
            print(f"Current timestamp: {results[0]['CURRENT_TIME']}")

            # Execute query with database/schema context
            init_context = {"database": "SNOWFLAKE_SAMPLE_DATA", "schema": "TPCH_SF1"}
            results = client.execute_query(
                "SELECT COUNT(*) as row_count FROM CUSTOMER LIMIT 1", init=init_context
            )
            print(f"Customer table row count: {results[0]['ROW_COUNT']}")

    except Exception as e:
        print(f"Error: {e}")
        print("Make sure your environment variables are set correctly.")


def example_with_manual_config():
    """Example using manual configuration."""
    print("\n=== Example 2: Manual Configuration ===")

    # Replace these with your actual Snowflake credentials
    client = SnowflakeClient(
        account="your-account",
        user="your-username",
        password="your-password",
        warehouse="your-warehouse",
        database="your-database",
        schema="your-schema",
    )

    try:
        client.connect()

        # Execute a query
        results = client.execute_query("SELECT CURRENT_USER() as current_user")
        print(f"Current user: {results[0]['CURRENT_USER']}")

    except Exception as e:
        print(f"Error: {e}")
        print("Update the credentials in this example to test manual configuration.")
    finally:
        client.disconnect()


def example_parameterized_query():
    """Example using parameterized queries."""
    print("\n=== Example 3: Parameterized Queries ===")

    try:
        with create_snowflake_client_from_env() as client:
            # Using parameterized queries for safety
            query = """
            SELECT 
                %(param1)s as parameter_1,
                %(param2)s as parameter_2,
                CURRENT_TIMESTAMP() as query_time
            """
            params = {"param1": "Hello", "param2": "World"}

            results = client.execute_query(query, params=params)
            result = results[0]
            print(f"Parameter 1: {result['PARAMETER_1']}")
            print(f"Parameter 2: {result['PARAMETER_2']}")
            print(f"Query time: {result['QUERY_TIME']}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    print("Snowflake Client Examples")
    print("=" * 50)

    # Check if environment variables are set
    required_env_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(f"Missing environment variables: {', '.join(missing_vars)}")
        print("Please set these variables before running the examples.")
        print("\nExample:")
        print("export SNOWFLAKE_ACCOUNT='your-account'")
        print("export SNOWFLAKE_USER='your-username'")
        print("export SNOWFLAKE_PASSWORD='your-password'")
    else:
        example_with_env_variables()
        example_parameterized_query()

    # Always show manual config example (even if it fails)
    example_with_manual_config()
