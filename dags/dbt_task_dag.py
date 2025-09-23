# dags/my_pipeline.py

import sys
sys.path.insert(0, '/usr/local/airflow/') # Import the modules from the path

from include.profiles import profile_config, venv_execution_config, project_config
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from cosmos import DbtTaskGroup
from airflow import DAG
import clickhouse_connect
import pandas as pd
import io

dag = DAG(dag_id='shopping_data_pipeline',
         schedule=None, 
         catchup=False)

# Set the configuration to DbtTaskGroup
dbt_task_group = DbtTaskGroup(
    group_id='dbt_task_dag',
    project_config=project_config,
    profile_config=profile_config,
    execution_config=venv_execution_config,
    dag=dag
)


def injest_data_to_clickhouse(**context):
    """
    Extract data from S3 bucket and load to ClickHouse
    """
    s3_hook = S3Hook(aws_conn_id='aws_connection')
    # S3 configuration
    bucket_name = 'agentic-dataprocessing'
    key = 'raw/retail/customers/customers.csv' 
    
    try:
        # Get the file object from S3
        s3_obj = s3_hook.get_key(key, bucket_name)
        
        # Read the content as string
        file_content = s3_obj.get()['Body'].read()
        
        # Convert bytes to string if necessary
        if isinstance(file_content, bytes):
            file_content = file_content.decode('utf-8')
        
        # Create StringIO object for pandas
        csv_buffer = io.StringIO(file_content)
        
        # Read CSV content into DataFrame with proper data types
        df = pd.read_csv(csv_buffer, dtype={
            'customer_id': 'int64',
            'first_name': 'string',
            'last_name': 'string', 
            'email': 'string',
            'phone': 'string',
            'country': 'string',
            'state': 'string', 
            'city': 'string',
            'zip': 'string',
            'gender': 'string'
        }, parse_dates=['signup_date'])
        
        # Data type conversions and cleaning
        df['customer_id'] = df['customer_id'].astype('int64')
        df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')
        
        # Handle null values appropriately for ClickHouse
        df['first_name'] = df['first_name'].fillna('')
        df['last_name'] = df['last_name'].fillna('')
        df['email'] = df['email'].fillna('')
        df['phone'] = df['phone'].fillna('')
        df['country'] = df['country'].fillna('')
        df['state'] = df['state'].fillna('')
        df['city'] = df['city'].fillna('')
        df['zip'] = df['zip'].fillna('')
        df['gender'] = df['gender'].fillna('')
        
        print(f"Successfully extracted {len(df)} records from S3")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Data types: {df.dtypes.to_dict()}")
        
        # ClickHouse configuration
        database = 'default'
        clickhouse_config = {
            'host': 'host.docker.internal',
            'port': 8123,  # HTTP port
            'username': 'dbt_user',
            'password': 'dbt_password',
            'database': database
        }
        
        # Connect to ClickHouse
        client = clickhouse_connect.get_client(**clickhouse_config)
        
        # Create table if not exists with proper data types
        client.command("""
        CREATE TABLE IF NOT EXISTS customer_seeds (
            customer_id UInt64,
            first_name   String,
            last_name    String,
            email        String,
            phone        String,              
            signup_date  DateTime,          
            country      LowCardinality(String),
            state        LowCardinality(String),
            city         LowCardinality(String),
            zip          String,               
            gender       LowCardinality(String)        
        ) ENGINE = MergeTree ORDER BY customer_id
        """)
        
        # Insert data
        client.insert_df(
            table='customer_seeds',
            df=df,
            database=database 
        )
         
        print(f"Successfully inserted {len(df)} records to ClickHouse customer table")
    
    except Exception as e:
        print(f"Error in data pipeline: {str(e)}")
        raise


load_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=injest_data_to_clickhouse,
    do_xcom_push=False,
    dag=dag
)

end_empty_task = EmptyOperator(task_id='end', dag=dag)

# Pipeline
load_task >> dbt_task_group >> end_empty_task