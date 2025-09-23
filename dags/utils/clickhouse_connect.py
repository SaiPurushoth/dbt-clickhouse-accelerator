import sys
import os
sys.path.insert(0, "/usr/local/airflow/")  # Import the modules from the path
import clickhouse_connect

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST", "host.docker.internal"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "username": os.getenv("CLICKHOUSE_USER", "dbt_user"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "dbt_password"),
    "database": os.getenv("CLICKHOUSE_DATABASE", "default"),
    "secure": os.getenv("CLICKHOUSE_SECURE", "False").lower() == "true",
}

def get_clickhouse_client():
    """Create ClickHouse connection"""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_CONFIG["host"],
        port=CLICKHOUSE_CONFIG["port"],
        username=CLICKHOUSE_CONFIG["username"],
        password=CLICKHOUSE_CONFIG["password"],
        database=CLICKHOUSE_CONFIG["database"],
    )