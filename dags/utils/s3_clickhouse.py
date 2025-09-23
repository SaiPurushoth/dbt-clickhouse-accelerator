from dags.utils.clickhouse_connect import get_clickhouse_client
import sys
import os

sys.path.insert(0, "/usr/local/airflow/")
from dags.config.tables_info import tables as TABLE_CONFIGS
import logging

S3_CONFIG = {
    "bucket": os.getenv("S3_BUCKET"),
    "prefix": os.getenv("S3_PREFIX"),
    "aws_access_key": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region": os.getenv("AWS_REGION", "us-east-1"),
    "endpoint_url": os.getenv("S3_ENDPOINT_URL"),
}


def create_raw_tables(**context):
    """Create raw tables in ClickHouse if they don't exist"""
    client = get_clickhouse_client()

    try:
        for table_config in TABLE_CONFIGS:
            logging.info(f"Creating table: {table_config['table_name']}")
            client.command(table_config["schema"])
            logging.info(f"Table {table_config['table_name']} created successfully")

    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        client.close()


def ingest_table_from_s3(table_config, **context):
    """
    Ingest data from S3 to ClickHouse using ClickHouse S3 function
    This is the most efficient method as it leverages ClickHouse's native S3 integration
    """
    client = get_clickhouse_client()
    execution_date = context["ds"]

    try:
        table_name = table_config["table_name"]
        s3_path = table_config["s3_path"]
        file_format = table_config["file_format"]

        # Construct S3 URL - adjust path pattern as needed
        s3_url = f"s3://{S3_CONFIG['bucket']}/{S3_CONFIG['prefix']}{s3_path}/*"

        truncate_query = f"TRUNCATE TABLE {table_name};"

        truncate_result = client.command(truncate_query)

        # Method 1: Direct INSERT FROM S3 (Most Efficient)
        insert_query = f"""
        INSERT INTO {table_name}
        SELECT *
        FROM s3(
            '{s3_url}',
            '{S3_CONFIG['aws_access_key']}',
            '{S3_CONFIG['aws_secret_key']}',
            '{file_format}'
        )
        """
        logging.info(f"Query: {insert_query}")
        logging.info(f"Executing S3 ingestion for {table_name}")
        logging.info(f"S3 Path: {s3_url}")

        insert_result = client.command(insert_query)

        # Get row count for verification
        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE DATE(_ingestion_timestamp) = '{execution_date}'"
        row_count = client.command(count_query)

        logging.info(f"Successfully ingested {row_count} rows into {table_name}")

        return {
            "table_name": table_name,
            "rows_ingested": row_count,
            "execution_date": execution_date,
        }

    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {str(e)}")
        raise
    finally:
        client.close()
