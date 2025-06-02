"""
Snowflake Client Package

A simple and efficient Snowflake database client for Python.
"""

from .client import SnowflakeClient, create_snowflake_client_from_env

__version__ = "0.1.0"
__all__ = ["SnowflakeClient", "create_snowflake_client_from_env"]
