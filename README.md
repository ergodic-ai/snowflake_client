# Snowflake Client

A simple and efficient Snowflake database client for Python.

## Features

- Easy-to-use Snowflake database client
- Support for parameterized queries
- Context management for automatic connection handling
- Environment variable configuration
- Type hints for better development experience

## Installation

### From GitHub (using uv)

```bash
uv add git+https://github.com/ergodic-ai/snowflake_client.git
```

### From GitHub (using pip)

```bash
pip install git+https://github.com/ergodic-ai/snowflake_client.git
```

## Quick Start

### Using Environment Variables

Set up your environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="your-warehouse"  # optional
export SNOWFLAKE_DATABASE="your-database"    # optional
export SNOWFLAKE_SCHEMA="your-schema"        # optional
export SNOWFLAKE_ROLE="your-role"            # optional
```

Then use the client:

```python
from snowflake_client import create_snowflake_client_from_env

# Create client from environment variables
client = create_snowflake_client_from_env()

# Execute a query
results = client.execute_query("SELECT * FROM your_table LIMIT 10")
print(results)

# Don't forget to disconnect
client.disconnect()
```

### Using Context Manager (Recommended)

```python
from snowflake_client import create_snowflake_client_from_env

# Using context manager for automatic connection handling
with create_snowflake_client_from_env() as client:
    results = client.execute_query("SELECT * FROM your_table LIMIT 10")
    print(results)
# Connection is automatically closed
```

### Manual Configuration

```python
from snowflake_client import SnowflakeClient

# Create client with manual configuration
client = SnowflakeClient(
    account="your-account",
    user="your-username",
    password="your-password",
    warehouse="your-warehouse",
    database="your-database",
    schema="your-schema",
    role="your-role"
)

# Connect and execute query
client.connect()
results = client.execute_query("SELECT CURRENT_TIMESTAMP()")
client.disconnect()
```

### Parameterized Queries

```python
from snowflake_client import create_snowflake_client_from_env

with create_snowflake_client_from_env() as client:
    # Using parameterized queries for safety
    query = "SELECT * FROM users WHERE age > %(min_age)s AND city = %(city)s"
    params = {"min_age": 25, "city": "New York"}

    results = client.execute_query(query, params=params)
    print(results)
```

### Setting Database and Schema Context

```python
from snowflake_client import create_snowflake_client_from_env

with create_snowflake_client_from_env() as client:
    # Set context during query execution
    init_context = {"database": "MY_DATABASE", "schema": "MY_SCHEMA"}
    results = client.execute_query(
        "SELECT * FROM my_table",
        init=init_context
    )
    print(results)
```

## API Reference

### SnowflakeClient

The main client class for interacting with Snowflake.

#### Constructor

```python
SnowflakeClient(
    account: str,
    user: str,
    password: str,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    role: Optional[str] = None,
)
```

#### Methods

- `connect()`: Establish connection to Snowflake
- `disconnect()`: Close connection to Snowflake
- `execute_query(query, params=None, init=None)`: Execute SQL query and return results
- `initialize_context(database=None, schema=None)`: Set database and schema context

### Functions

- `create_snowflake_client_from_env()`: Create client using environment variables

## Environment Variables

| Variable              | Required | Description                       |
| --------------------- | -------- | --------------------------------- |
| `SNOWFLAKE_ACCOUNT`   | Yes      | Your Snowflake account identifier |
| `SNOWFLAKE_USER`      | Yes      | Your Snowflake username           |
| `SNOWFLAKE_PASSWORD`  | Yes      | Your Snowflake password           |
| `SNOWFLAKE_WAREHOUSE` | No       | Default warehouse to use          |
| `SNOWFLAKE_DATABASE`  | No       | Default database to use           |
| `SNOWFLAKE_SCHEMA`    | No       | Default schema to use             |
| `SNOWFLAKE_ROLE`      | No       | Default role to use               |

## Development

### Setting up development environment

```bash
# Clone the repository
git clone https://github.com/yourusername/snowflake_client.git
cd snowflake_client

# Install with development dependencies using uv
uv sync --extra dev

# Or using pip
pip install -e ".[dev]"
```

### Running tests

```bash
pytest
```

### Code formatting

```bash
black src/
isort src/
```

### Type checking

```bash
mypy src/
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
